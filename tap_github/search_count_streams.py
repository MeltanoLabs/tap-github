"""GitHub search count streams for statistical aggregation via GraphQL."""

from __future__ import annotations

import calendar
import re
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any, ClassVar, Dict, List, Tuple
from functools import lru_cache
from itertools import product

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_github.authenticator import GitHubTokenAuthenticator
from tap_github.client import GitHubGraphqlStream
from tap_github.utils.repository_discovery import RepositoryDiscovery
from tap_github.utils.repository_discovery import GitHubInstance
from tap_github.utils.search_queries import SearchQueryGenerator
from tap_github.utils.search_queries import SearchQuery
from tap_github.utils.http_client import GitHubGraphQLClient
from tap_github.utils.date_utils import GitHubDateUtils
from tap_github.utils.validation import GitHubValidationMixin




# SearchQuery and GitHubInstance now imported from utilities


class BaseSearchCountStream(GitHubGraphqlStream, GitHubValidationMixin):
    """Base stream for GitHub search count queries via GraphQL."""

    primary_keys: ClassVar[list[str]] = ["search_name", "month", "source"]
    replication_key = None
    state_partitioning_keys: ClassVar[list[str]] = ["source", "search_name"]
    stream_type: ClassVar[str] = "issue"
    count_field: ClassVar[str] = "issue_count"
    
    # Performance configuration
    DEFAULT_MAX_PARTITIONS = 5000
    DEFAULT_WARNING_THRESHOLD = 2000
    DEFAULT_CACHE_TTL_MINUTES = 60
    

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._repo_cache: Dict[str, Tuple[List[str], datetime]] = {}
        self._authenticator = None
        
        # Initialize utility classes
        self._repo_discovery = RepositoryDiscovery(self)
        self._query_generator = SearchQueryGenerator()
        self._http_client = GitHubGraphQLClient(self.config, self.logger)

    # Query configurations by stream type
    QUERY_KINDS_BY_STREAM: ClassVar[dict[str, list[str]]] = {
        "issue": ["issue", "bug"],
        "pr": ["pr"],
    }

    def _query_kinds(self) -> list[str]:
        """Get query kinds for this stream type."""
        return self.QUERY_KINDS_BY_STREAM[self.stream_type]


    def _make_named_query(self, kind: str, scope: str, target: str, start: str, end: str, month_id: str) -> SearchQuery:
        """Create a single SearchQuery with standardized naming."""
        q = self._query_generator.build_search_query(
            query_type=kind, scope=scope, target=target, start_date=start, end_date=end
        )
        base = self.clean_identifier(target)
        suffix = "prs" if self.stream_type == "pr" else ("bugs" if kind == "bug" else "issues")
        return SearchQuery(
            name=f"{base}_open_{suffix}_{self.clean_month_id(month_id)}",
            query=q,
            type=self.stream_type,
            month=month_id,
        )

    def _queries_for(self, scope: str, target: str, start: str, end: str, month_id: str) -> list[SearchQuery]:
        """Generate all queries for a target using configured query kinds."""
        return [self._make_named_query(kind, scope, target, start, end, month_id) for kind in self._query_kinds()]

    @classmethod
    def _base_schema(cls, count_field: str) -> dict:
        """Generate unified schema for search count streams."""
        return th.PropertiesList(
            th.Property("search_name", th.StringType, required=True),
            th.Property("search_query", th.StringType, required=True),
            th.Property("source", th.StringType, required=True),
            th.Property("month", th.StringType),
            th.Property("org", th.StringType, description="GitHub organization name"),
            th.Property(
                "repo",
                th.StringType,
                description="Repository name (null for org-level queries)",
            ),
            th.Property(count_field, th.IntegerType, required=True),
            th.Property("updated_at", th.DateTimeType),
        ).to_dict()

    def _resolve_token(self, instance_name: str | None = None, explicit_token: str | None = None) -> str | None:
        """Centralized token resolution with fallback hierarchy."""
        import os
        
        if explicit_token:
            return explicit_token
            
        # Instance-specific token if instance_name provided
        if instance_name:
            env_key = f"GITHUB_TOKEN_{instance_name.upper().replace('.', '_').replace('-', '_')}"
            instance_token = os.environ.get(env_key)
            if instance_token:
                return instance_token
        
        # Default token hierarchy
        default_token = (
            self.config.get("auth_token")
            or self.config.get("access_token")
            or os.environ.get("GITHUB_TOKEN")
        )
        
        if default_token:
            return default_token
        
        # Fallback to any GITHUB_TOKEN* environment variable
        for key, value in os.environ.items():
            if key.startswith("GITHUB_TOKEN") and value:
                return value
        
        return None

    @property
    def query(self) -> str:
        """Return the GraphQL query for search counts."""
        return """
        query($searchQuery: String!) {
          search(query: $searchQuery, type: ISSUE, first: 1) {
            issueCount
          }
          rateLimit {
            cost
            remaining
          }
        }
        """


    def prepare_request_payload(
        self, context: Mapping[str, Any] | None, next_page_token: Any | None
    ) -> dict[str, Any]:
        """Return GraphQL request payload with query and variables."""
        variables = {}
        if context:
            variables["searchQuery"] = context.get("search_query")

        return {
            "query": self.query,
            "variables": variables,
        }

    query_jsonpath: str = "$.data.search"

    @property
    def partitions(self) -> list[dict] | None:
        """Return partition contexts for Singer SDK."""
        # Explicit queries (highest priority)
        if "search_count_queries" in self.config:
            return self._get_explicit_partition_contexts()
            
        # Monthly partitions from orgs + date range
        if "search_orgs" in self.config and "date_range" in self.config:
            return self._get_monthly_partition_contexts()
            
        # Scope-based partitions
        if "search_scope" in self.config:
            return self._get_scope_partition_contexts()
            
        return None

    def _get_explicit_partition_contexts(self) -> list[dict]:
        """Get simplified partition contexts from explicit search_count_queries config."""
        contexts = []
        instances = self._get_github_instances()

        for query_config in self.config["search_count_queries"]:
            if query_config.get("type", "issue") == self.stream_type:
                for instance in instances:
                    contexts.append({
                        "search_name": query_config["name"],
                        "search_query": query_config["query"], 
                        "source": instance.name,
                        "api_url_base": instance.api_url_base,
                        "month": query_config.get("month"),
                        "type": query_config.get("type", "issue")
                    })
        return contexts

    def _get_scope_partition_contexts(self) -> list[dict]:
        """Get simplified partition contexts from search_scope configuration."""
        contexts = []
        scope_config = self.config["search_scope"]
        
        if not scope_config.get("instances"):
            self.logger.warning("search_scope.instances is required")
            return []
            
        date_range = self.config.get("date_range", {})
        if not date_range:
            self.logger.warning("date_range is required when using search_scope")
            return []

        month_ranges = self._generate_month_ranges()
        
        for instance_config in scope_config["instances"]:
            for start_date, end_date, month_id in month_ranges:
                # Org-level contexts
                for org in instance_config.get("org_level", []):
                    for query_kind in self._query_kinds():
                        query = self._query_generator.build_search_query(
                            query_type=query_kind, scope="org", target=org, 
                            start_date=start_date, end_date=end_date
                        )
                        contexts.append({
                            "search_name": f"{self.clean_identifier(org)}_open_{'bugs' if query_kind == 'bug' else query_kind}s_{self.clean_month_id(month_id)}",
                            "search_query": query,
                            "source": instance_config["instance"],
                            "api_url_base": instance_config["api_url_base"], 
                            "month": month_id,
                            "type": self.stream_type
                        })
                
                # Repo-level contexts  
                for repo_config in instance_config.get("repo_level", []):
                    org = repo_config["org"]
                    repos = self._get_top_repos_for_instance(
                        instance_config, org, 
                        repo_config.get("limit", 20),
                        repo_config.get("sort_by", "issues"),
                        instance_config.get("repo_discovery_cache_ttl", 60)
                    )
                    for repo in repos:
                        for query_kind in self._query_kinds():
                            query = self._query_generator.build_search_query(
                                query_type=query_kind, scope="repo", target=repo,
                                start_date=start_date, end_date=end_date
                            )
                            contexts.append({
                                "search_name": f"{self.clean_identifier(repo)}_open_{'bugs' if query_kind == 'bug' else query_kind}s_{self.clean_month_id(month_id)}",
                                "search_query": query,
                                "source": instance_config["instance"],
                                "api_url_base": instance_config["api_url_base"],
                                "month": month_id, 
                                "type": self.stream_type
                            })
        return contexts

    def _get_monthly_partition_contexts(self) -> list[dict]:
        """Get simplified partition contexts from legacy search_orgs configuration."""
        contexts = []
        organizations = self.config["search_orgs"]
        month_ranges = self._generate_month_ranges()
        instances = self._get_github_instances()

        for org in organizations:
            for start_date, end_date, month_id in month_ranges:
                for instance in instances:
                    for query_kind in self._query_kinds():
                        query = self._query_generator.build_search_query(
                            query_type=query_kind, scope="org", target=org,
                            start_date=start_date, end_date=end_date
                        )
                        contexts.append({
                            "search_name": f"{self.clean_identifier(org)}_open_{'bugs' if query_kind == 'bug' else query_kind}s_{self.clean_month_id(month_id)}",
                            "search_query": query,
                            "source": instance.name,
                            "api_url_base": instance.api_url_base,
                            "month": month_id,
                            "type": self.stream_type
                        })
        return contexts

    # Legacy get_partitions method removed - replaced with Singer SDK partitions property

    # Legacy _get_explicit_partitions removed

    def _get_github_instances(self) -> list[GitHubInstance]:
        """Get GitHub instances from config with sensible defaults.

        Authentication priority (first non-empty value wins):
        1. Instance-specific auth_token in github_instances config
        2. Global auth_token in config
        3. Global access_token in config (legacy)
        4. GITHUB_TOKEN environment variable
        5. GITHUB_TOKEN* environment variables (for multiple tokens)

        Best practice: Use environment variables to avoid storing tokens in config files.
        """
        import os

        # Get default token using centralized resolver
        default_token = self._resolve_token()

        if not default_token:
            self.logger.warning(
                "No authentication token found. Please set auth_token in config "
                "or GITHUB_TOKEN environment variable."
            )

        instances_config = self.config.get(
            "github_instances",
            [
                {
                    "name": "github.com",
                    "api_url_base": "https://api.github.com",
                    "auth_token": default_token,
                }
            ],
        )

        instances = []
        for instance in instances_config:
            # Allow instance to override with environment variable
            # Use centralized token resolver for instance-specific tokens
            instance_token = self._resolve_token(
                instance_name=instance['name'],
                explicit_token=instance.get("auth_token")
            ) or default_token

            if not instance_token:
                self.logger.warning(
                    f"No auth token for instance {instance['name']}, skipping"
                )
                continue

            instances.append(
                GitHubInstance(
                    name=instance["name"],
                    api_url_base=instance["api_url_base"],
                    auth_token=instance_token,
                )
            )

        return instances

    def _validate_date_format(self, date_str: str, field_name: str) -> None:
        """Validate date format is YYYY-MM-DD."""
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            msg = (
                f"Invalid {field_name} format '{date_str}'. Expected YYYY-MM-DD format."
            )
            raise ValueError(msg)

        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError as e:
            msg = f"Invalid {field_name} '{date_str}': {e}"
            raise ValueError(msg) from e


    def _generate_month_ranges(self) -> list[tuple[str, str, str]]:
        """Generate monthly date ranges from config."""
        date_range = self.config["date_range"]
        raw_start_date = date_range["start"]
        raw_end_date = date_range.get("end")

        # Validate and potentially adjust the date range
        start_date, end_date = self._validate_and_adjust_date_range(raw_start_date, raw_end_date)

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        ranges = []
        current = start.replace(day=1)

        while current <= end:
            if current.year == end.year and current.month == end.month:
                month_end_date = end_date
            else:
                last_day = calendar.monthrange(current.year, current.month)[1]
                month_end_date = f"{current.year}-{current.month:02d}-{last_day:02d}"

            month_start_date = f"{current.year}-{current.month:02d}-01"
            month_id = f"{current.year}-{current.month:02d}"

            ranges.append((month_start_date, month_end_date, month_id))

            # Move to next month
            current = (
                current.replace(year=current.year + 1, month=1)
                if current.month == 12
                else current.replace(month=current.month + 1)
            )

        return ranges

    def _get_auto_end_date(self, start_date: str) -> str:
        """Get auto-detected end date based on start date and current date.
        
        Rules:
        1. End date is last day of the previous complete month
        2. Configurable lookback limit (default: 1 year)
        3. Can enforce or warn based on configuration
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            
        Returns:
            End date string in YYYY-MM-DD format
        """
        today = datetime.now()
        
        # Last complete month is the previous month
        if today.month == 1:
            last_complete_month = today.replace(year=today.year - 1, month=12, day=1)
        else:
            last_complete_month = today.replace(month=today.month - 1, day=1)
        
        # Get last day of that month
        last_day = calendar.monthrange(last_complete_month.year, last_complete_month.month)[1]
        auto_end_date = last_complete_month.replace(day=last_day)
        
        return auto_end_date.strftime("%Y-%m-%d")
    
    def _validate_and_adjust_date_range(self, start_date: str, end_date: str | None = None) -> tuple[str, str]:
        """Validate and potentially adjust date range based on configuration.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (optional)
            
        Returns:
            Tuple of (validated_start_date, validated_end_date)
            
        Raises:
            ValueError: If dates are invalid or exceed limits when enforcement is enabled
        """
        return GitHubDateUtils.validate_and_adjust_date_range(
            start_date=start_date,
            end_date=end_date,
            enforce_lookback_limit=self.config.get("enforce_lookback_limit", False),
            max_lookback_years=self.config.get("max_lookback_years", 1),
            logger=self.logger
        )

    def _enforce_partition_limits(self, total: int, max_allowed: int | None = None, warn_at: int | None = None, label: str = "Global", enforce: bool | None = None) -> None:
        """Unified partition limit checking for both global and instance-specific limits."""
        max_allowed = max_allowed or self.config.get("max_partitions", self.DEFAULT_MAX_PARTITIONS)
        warn_at = warn_at or self.config.get("partition_warning_threshold", self.DEFAULT_WARNING_THRESHOLD)
        enforce = enforce if enforce is not None else self.config.get("enforce_partition_limit", True)
        
        if total > max_allowed and enforce:
            raise ValueError(
                f"{label} partition count {total} exceeds maximum {max_allowed}. "
                f"Consider reducing date range, repository count, or disable enforcement "
                f"with enforce_partition_limit=false"
            )
        if total > warn_at:
            self.logger.warning(
                f"{label} high partition count: {total} (threshold: {warn_at}). "
                f"This may result in rate limiting or performance issues."
            )

    def _check_partition_limits(self, total_partitions: int) -> None:
        """Check global partition count limits."""
        self._enforce_partition_limits(
            total_partitions, 
            max_allowed=self.DEFAULT_MAX_PARTITIONS,
            warn_at=self.DEFAULT_WARNING_THRESHOLD,
            enforce=True,
            label="Global"
        )



    def _generate_monthly_partitions(self) -> list[dict]:
        """Generate partitions programmatically from org list and date range."""
        organizations = self.config["search_orgs"]
        self.validate_org_names(organizations)

        partitions = []
        instances = self._get_github_instances()
        month_ranges = self._generate_month_ranges()

        total_partitions = len(organizations) * len(month_ranges) * len(instances)
        if self.stream_type == "issue":
            total_partitions *= 2  # Both issue and bug queries

        # Check partition limits
        self._check_partition_limits(total_partitions)

        for start_date, end_date, month_id in month_ranges:
            for org in organizations:
                search_queries = self._queries_for("org", org, start_date, end_date, month_id)

                for query in search_queries:
                    for instance in instances:
                        partition = {
                            "search_name": query.name,
                            "search_query": query.query,
                            "source": instance.name,
                            "month": query.month,
                            "api_url_base": instance.api_url_base,
                        }
                        partitions.append(partition)

        return partitions

    def _generate_scope_partitions(self) -> list[dict]:
        """Generate partitions from search_scope configuration."""
        scope_config = self.config["search_scope"]
        date_range = self.config.get("date_range")

        if not date_range:
            msg = "date_range is required when using search_scope"
            raise ValueError(msg)

        if "instances" not in scope_config:
            msg = "search_scope.instances is required. Please use the new instance-based configuration format."
            raise ValueError(msg)

        month_ranges = self._generate_month_ranges()
        return [
            partition
            for instance_config in scope_config["instances"]
            for partition in self._generate_partitions_for_instance(instance_config, month_ranges)
        ]

    def _generate_partitions_for_instance(self, instance_config: dict, month_ranges: list[tuple[str, str, str]]) -> list[dict]:
        """Generate all partitions for a single instance."""
        instance_partitions = []
        instance_partitions.extend(self._generate_org_level_partitions(instance_config, month_ranges))
        instance_partitions.extend(self._generate_repo_level_partitions(instance_config, month_ranges))
        
        # Check instance-specific limits
        max_partitions = int(instance_config.get("max_partitions", self.config.get("max_partitions", self.DEFAULT_MAX_PARTITIONS)))
        warning_threshold = int(instance_config.get("partition_warning_threshold", self.config.get("partition_warning_threshold", self.DEFAULT_WARNING_THRESHOLD)))
        enforce_limit = bool(instance_config.get("enforce_partition_limit", self.config.get("enforce_partition_limit", True)))
        instance_name = instance_config["instance"]
        
        self._check_instance_partition_limits(
            len(instance_partitions), instance_name, max_partitions, warning_threshold, enforce_limit
        )
        
        return instance_partitions

    def _generate_org_level_partitions(self, instance_config: dict, month_ranges: list[tuple[str, str, str]]) -> list[dict]:
        """Generate org-level partitions for an instance."""
        org_level_orgs = instance_config.get("org_level", [])
        if not org_level_orgs:
            return []
            
        self.validate_org_names(org_level_orgs)
        return [
            self._create_partition(query, instance_config)
            for org, (start_date, end_date, month_id) in product(org_level_orgs, month_ranges)
            for query in self._queries_for("org", org, start_date, end_date, month_id)
        ]

    def _generate_repo_level_partitions(self, instance_config: dict, month_ranges: list[tuple[str, str, str]]) -> list[dict]:
        """Generate repo-level partitions for an instance."""
        repo_level_configs = instance_config.get("repo_level", [])
        if not repo_level_configs:
            return []
            
        partitions = []
        cache_ttl = instance_config.get("repo_discovery_cache_ttl", self.config.get("repo_discovery_cache_ttl", self.DEFAULT_CACHE_TTL_MINUTES))
        
        for repo_config in repo_level_configs:
            org = repo_config["org"]
            limit = repo_config.get("limit", 20)
            sort_by = repo_config.get("sort_by", "issues")
            
            top_repos = self._get_top_repos_for_instance(instance_config, org, limit, sort_by, int(cache_ttl or self.config.get("repo_discovery_cache_ttl", self.DEFAULT_CACHE_TTL_MINUTES)))
            
            partitions.extend([
                self._create_partition(query, instance_config)
                for repo, (start_date, end_date, month_id) in product(top_repos, month_ranges)
                for query in self._queries_for("repo", repo, start_date, end_date, month_id)
            ])
            
        return partitions

    def _create_partition(self, query: SearchQuery, instance_config: dict) -> dict:
        """Create a partition dict from query and instance config."""
        return {
            "search_name": query.name,
            "search_query": query.query,
            "source": instance_config["instance"],
            "month": query.month,
            "api_url_base": instance_config["api_url_base"],
            "type": query.type
        }



    def _get_top_repos_for_instance(self, instance_config: dict, org: str, limit: int, sort_by: str, cache_ttl: int) -> list[str]:
        """Get top N repositories for a specific instance."""
        instance_name = instance_config["instance"]
        cache_key = f"{instance_name}:{org}:{limit}:{sort_by}"
        
        # Check instance-specific cache
        if cache_key in self._repo_cache:
            repos, cached_at = self._repo_cache[cache_key]
            cache_age = datetime.now() - cached_at
            if cache_age.total_seconds() < (cache_ttl * 60):
                self.logger.debug(f"Using cached repositories for {cache_key}")
                return repos
            del self._repo_cache[cache_key]
        
        # Create temporary GitHubInstance object for this instance
        explicit_token = instance_config.get("auth_token")
        auth_token = self._resolve_token(
            instance_name=instance_name,
            explicit_token=str(explicit_token) if explicit_token else None
        )
        
        if not auth_token:
            self.logger.warning(f"No auth token available for instance {instance_name}")
            return []
        
        instance = GitHubInstance(
            name=instance_name,
            api_url_base=instance_config["api_url_base"],
            auth_token=auth_token
        )
        
        # Fetch repositories using instance-specific API
        repos = self._repo_discovery.get_top_repos(org, limit, sort_by, instance)
        
        # Cache the results with instance-specific TTL
        self._repo_cache[cache_key] = (repos, datetime.now())
        self.logger.debug(f"Cached {len(repos)} repositories for {cache_key}")
        
        return repos

    def _check_instance_partition_limits(self, total_partitions: int, instance_name: str, max_partitions: int, warning_threshold: int, enforce_limit: bool) -> None:
        """Check partition limits for a specific instance."""
        self._enforce_partition_limits(total_partitions, max_partitions, warning_threshold, f"Instance '{instance_name}'", enforce_limit)


    # ===== REPOSITORY DISCOVERY METHODS ELIMINATED =====
    # All 8 duplicate methods removed - now using RepositoryDiscovery utility directly



    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Singer SDK compatible get_records - processes single partition from context."""
        if not context:
            return
            
        partition_data = context.get("partition")
        if not partition_data:
            return
        
        # Extract partition information
        search_query = partition_data.get("search_query")
        search_name = partition_data.get("search_name")
        source = partition_data.get("source", "github.com")
        api_url_base = partition_data.get("api_url_base", "https://api.github.com")
        month = partition_data.get("month")
        
        if not search_query or not search_name:
            self.logger.warning(f"Incomplete partition data: {partition_data}")
            return
            
        # Make single GraphQL request for this partition
        query = f"""
        query SearchCount($query: String!) {{
            search(query: $query, type: {'ISSUE' if self.stream_type == 'issue' else 'PULLREQUEST'}, first: 1) {{
                {self.count_field}
            }}
        }}
        """
        
        variables = {"query": search_query}
        
        # Use existing HTTP client but for single request
        instances = self._get_github_instances()
        instance_obj = next((i for i in instances if i.name == source), None)
        
        if not instance_obj:
            self.logger.warning(f"Instance {source} not found")
            return
            
        response = self._http_client.make_request(query, variables, instance_obj)
        
        if not response or "data" not in response:
            self.logger.warning(f"Request failed for partition {search_name}")
            return
            
        search_data = response["data"]["search"]
        count_value = search_data.get(self.count_field, 0)
        
        # Build result record
        result = {
            "search_name": search_name,
            "search_query": search_query,
            "source": source,
            self.count_field: count_value,
            "updated_at": datetime.now().isoformat()
        }
        
        if month:
            result["month"] = month
            
        yield result


    @property
    def authenticator(self) -> GitHubTokenAuthenticator:
        """Simple authenticator - tokens handled by HTTP client per instance."""
        if self._authenticator is None:
            self._authenticator = GitHubTokenAuthenticator(stream=self)
        return self._authenticator







class IssueSearchCountStream(BaseSearchCountStream):
    """Stream for GitHub issue search count queries via GraphQL."""

    name = "issue_search_counts"
    stream_type = "issue"
    count_field = "issue_count"
    schema = BaseSearchCountStream._base_schema("issue_count")


class PRSearchCountStream(BaseSearchCountStream):
    """Stream for GitHub PR search count queries via GraphQL."""

    name = "pr_search_counts"
    stream_type = "pr"
    count_field = "pr_count"
    schema = BaseSearchCountStream._base_schema("pr_count")
