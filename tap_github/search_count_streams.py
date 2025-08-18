g"""GitHub search count streams for statistical aggregation via GraphQL."""

from __future__ import annotations

import calendar
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, ClassVar
from functools import lru_cache

from singer_sdk import typing as th
from singer_sdk.exceptions import RetriableAPIError
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


@dataclass(frozen=True)
class Partition:
    """Represents a search partition with all required information."""
    search_name: str
    search_query: str
    source: str
    month: str | None = None
    api_url_base: str = "https://api.github.com"
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format expected by Singer SDK."""
        return asdict(self)


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
    DEFAULT_BATCH_SIZE = 10
    
    @property
    def max_partitions(self) -> int:
        """Maximum allowed partitions."""
        return self.config.get("max_partitions", self.DEFAULT_MAX_PARTITIONS)
    
    @property
    def warning_threshold(self) -> int:
        """Partition count warning threshold."""
        return self.config.get("partition_warning_threshold", self.DEFAULT_WARNING_THRESHOLD)
    
    @property
    def cache_ttl_minutes(self) -> int:
        """Repository discovery cache TTL in minutes."""
        return self.config.get("repo_discovery_cache_ttl", self.DEFAULT_CACHE_TTL_MINUTES)
    
    @property
    def batch_size(self) -> int:
        """Batch query size."""
        return self.config.get("batch_query_size", self.DEFAULT_BATCH_SIZE)
    
    @property
    def enforce_partition_limit(self) -> bool:
        """Whether to enforce partition limits."""
        return self.config.get("enforce_partition_limit", True)
    
    @property
    def enforce_lookback_limit(self) -> bool:
        """Whether to enforce lookback limits."""
        return self.config.get("enforce_lookback_limit", False)
    
    @property
    def max_lookback_years(self) -> int:
        """Maximum lookback years."""
        return self.config.get("max_lookback_years", 1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._repo_cache: Dict[str, Tuple[List[str], datetime]] = {}
        self._cache_ttl_minutes = self.config.get("repo_discovery_cache_ttl", self.DEFAULT_CACHE_TTL_MINUTES)
        self._batch_size = self.config.get("batch_query_size", self.DEFAULT_BATCH_SIZE)
        
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

    def _slug(self, s: str) -> str:
        """Convert string to URL-safe slug format."""
        return self.clean_identifier(s)

    def _make_named_query(self, kind: str, scope: str, target: str, start: str, end: str, month_id: str) -> SearchQuery:
        """Create a single SearchQuery with standardized naming."""
        q = self._query_generator.build_search_query(
            query_type=kind, scope=scope, target=target, start_date=start, end_date=end
        )
        base = self._slug(target)
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

    def _build_batch_query(self, search_queries: list[str]) -> str:
        """Build a batched GraphQL query for multiple search queries."""
        variables = [f"$q{i}: String!" for i in range(len(search_queries))]
        searches = [
            f"search{i}: search(query: $q{i}, type: ISSUE, first: 1) {{\n            issueCount\n          }}"
            for i in range(len(search_queries))
        ]
        
        variables_str = ", ".join(variables)
        searches_str = "\n          ".join(searches)
        
        return f"""
        query({variables_str}) {{
          {searches_str}
          rateLimit {{
            cost
            remaining
          }}
        }}
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

    def get_partitions(self, context: Mapping[str, Any] | None = None) -> list[dict]:
        """Generate partitions for search count queries across instances."""
        partitions: list[dict] = []

        # Check for explicit queries first
        if "search_count_queries" in self.config:
            return self._get_explicit_partitions()

        # Generate queries from search_scope configuration
        if "search_scope" in self.config:
            return self._generate_scope_partitions()

        # Fallback: Generate queries programmatically from search_orgs and date range
        if "search_orgs" in self.config and "date_range" in self.config:
            return self._generate_monthly_partitions()

        return partitions

    def _get_explicit_partitions(self) -> list[dict]:
        """Get partitions from explicit search_count_queries config."""
        partitions = []
        instances = self._get_github_instances()

        for query_config in self.config["search_count_queries"]:
            if query_config.get("type", "issue") == self.stream_type:
                search_query = SearchQuery(
                    name=query_config["name"],
                    query=query_config["query"],
                    type=query_config.get("type", "issue"),
                    month=query_config.get("month"),
                )

                for instance in instances:
                    partition = {
                        "search_name": search_query.name,
                        "search_query": search_query.query,
                        "source": instance.name,
                        "month": search_query.month,
                        "api_url_base": instance.api_url_base,
                    }
                    partitions.append(partition)

        return partitions

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

    def _validate_org_names(self, org_names: list[str]) -> None:
        """Validate GitHub organization names."""
        self.validate_org_names(org_names)

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
            enforce_lookback_limit=self.enforce_lookback_limit,
            max_lookback_years=self.max_lookback_years,
            logger=self.logger
        )

    def _enforce_partition_limits(self, total: int, max_allowed: int | None = None, warn_at: int | None = None, label: str = "Global", enforce: bool | None = None) -> None:
        """Unified partition limit checking for both global and instance-specific limits."""
        max_allowed = max_allowed or self.max_partitions
        warn_at = warn_at or self.warning_threshold
        enforce = enforce if enforce is not None else self.enforce_partition_limit
        
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
        self._enforce_partition_limits(total_partitions, label="Global")


    def _create_search_queries_for_month(
        self, org: str, start_date: str, end_date: str, month_id: str
    ) -> list[SearchQuery]:
        """Create search queries for a specific organization and month."""
        return self._queries_for("org", org, start_date, end_date, month_id)

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
                search_queries = self._create_search_queries_for_month(
                    org, start_date, end_date, month_id
                )

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

        month_ranges = self._generate_month_ranges()

        # Instance-based configuration is required
        if "instances" not in scope_config:
            msg = "search_scope.instances is required. Please use the new instance-based configuration format."
            raise ValueError(msg)

        return self._generate_instance_scoped_partitions(scope_config, month_ranges)

    def _generate_instance_scoped_partitions(self, scope_config: dict, month_ranges: list[tuple[str, str, str]]) -> list[dict]:
        """Generate partitions from new instance-scoped configuration."""
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
        self._check_instance_partition_limits_from_config(instance_config, len(instance_partitions))
        
        return instance_partitions

    def _generate_org_level_partitions(self, instance_config: dict, month_ranges: list[tuple[str, str, str]]) -> list[dict]:
        """Generate org-level partitions for an instance."""
        org_level_orgs = instance_config.get("org_level", [])
        if not org_level_orgs:
            return []
            
        self.validate_org_names(org_level_orgs)
        return [
            self._create_partition(query, instance_config)
            for org in org_level_orgs
            for start_date, end_date, month_id in month_ranges
            for query in self._create_search_queries_for_month(org, start_date, end_date, month_id)
        ]

    def _generate_repo_level_partitions(self, instance_config: dict, month_ranges: list[tuple[str, str, str]]) -> list[dict]:
        """Generate repo-level partitions for an instance."""
        repo_level_configs = instance_config.get("repo_level", [])
        if not repo_level_configs:
            return []
            
        partitions = []
        cache_ttl = instance_config.get("repo_discovery_cache_ttl", self.cache_ttl_minutes)
        
        for repo_config in repo_level_configs:
            org = repo_config["org"]
            limit = repo_config.get("limit", 20)
            sort_by = repo_config.get("sort_by", "issues")
            
            top_repos = self._get_top_repos_for_instance(instance_config, org, limit, sort_by, int(cache_ttl or self.cache_ttl_minutes))
            
            partitions.extend([
                self._create_partition(query, instance_config)
                for repo in top_repos
                for start_date, end_date, month_id in month_ranges
                for query in self._generate_repo_queries(repo, start_date, end_date, month_id)
            ])
            
        return partitions

    def _create_partition(self, query: SearchQuery, instance_config: dict) -> dict:
        """Create a single partition dict from query and instance config."""
        partition = Partition(
            search_name=query.name,
            search_query=query.query,
            source=instance_config["instance"],
            month=query.month,
            api_url_base=instance_config["api_url_base"],
        )
        return partition.to_dict()

    def _check_instance_partition_limits_from_config(self, instance_config: dict, partition_count: int) -> None:
        """Check partition limits using instance configuration."""
        max_partitions = int(instance_config.get("max_partitions", self.max_partitions))
        warning_threshold = int(instance_config.get("partition_warning_threshold", self.warning_threshold))
        enforce_limit = bool(instance_config.get("enforce_partition_limit", self.enforce_partition_limit))
        instance_name = instance_config["instance"]
        
        self._check_instance_partition_limits(
            partition_count, instance_name, max_partitions, warning_threshold, enforce_limit
        )


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

    def _get_top_repos(self, org: str, limit: int, sort_by: str) -> list[str]:
        """Get top N repositories from an organization sorted by the specified criteria.
        
        Uses caching to avoid repeated API calls for the same org/criteria combination.
        """
        cache_key = f"{org}:{limit}:{sort_by}"
        
        # Check cache first
        if cache_key in self._repo_cache:
            repos, cached_at = self._repo_cache[cache_key]
            cache_age = datetime.now() - cached_at
            if cache_age.total_seconds() < (self._cache_ttl_minutes * 60):
                self.logger.debug(f"Using cached repositories for {cache_key}")
                return repos
            # Cache expired, remove it
            del self._repo_cache[cache_key]
        
        # Fetch fresh data
        repos = self._repo_discovery.get_top_repos(org, limit, sort_by)
        
        # Cache the results
        self._repo_cache[cache_key] = (repos, datetime.now())
        self.logger.debug(f"Cached {len(repos)} repositories for {cache_key}")
        
        return repos

    # ===== REPOSITORY DISCOVERY METHODS ELIMINATED =====
    # All 8 duplicate methods removed - now using RepositoryDiscovery utility directly

    def _make_graphql_request_for_instance(self, instance: GitHubInstance, query: str, variables: dict[str, Any]) -> dict[str, Any] | None:
        """Make a GraphQL request to a specific GitHub instance."""
        return self._http_client.make_request(query, variables, instance=instance)

    def _generate_repo_queries(
        self, repo: str, start_date: str, end_date: str, month_id: str
    ) -> list[SearchQuery]:
        """Generate search queries for a specific repository and month."""
        return self._queries_for("repo", repo, start_date, end_date, month_id)

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Override to handle custom partitioning with batch processing."""
        partitions = self.get_partitions(context)
        
        # Group partitions by instance for batch processing
        instance_partitions: dict[str, list[dict]] = {}
        for partition in partitions:
            instance = partition.get("source", "github.com")
            if instance not in instance_partitions:
                instance_partitions[instance] = []
            instance_partitions[instance].append(partition)
        
        # Process each instance's partitions in batches
        for instance, instance_partition_list in instance_partitions.items():
            yield from self._process_partitions_in_batches(instance_partition_list)

    def _process_partitions_in_batches(self, partitions: list[dict]) -> Iterable[dict[str, Any]]:
        """Process partitions in batches for better performance."""
        batch_size = self._batch_size
        
        for i in range(0, len(partitions), batch_size):
            batch_partitions = partitions[i:i + batch_size]
            yield from self._process_batch_request(batch_partitions)

    def _process_batch_request(self, batch_partitions: list[dict]) -> Iterable[dict[str, Any]]:
        """Process multiple partitions in a single batched GraphQL request."""
        search_queries = [p.get("search_query", "") for p in batch_partitions]
        
        # Build batch query and variables
        batch_query = self._build_batch_query(search_queries)
        variables = {f"q{i}": query for i, query in enumerate(search_queries)}
        
        # Set up API URL for this batch (all partitions in batch have same instance)
        first_partition = batch_partitions[0]
        self._current_partition = first_partition
        
        # Make the batched GraphQL request
        response = self._make_batch_graphql_request(batch_query, variables, first_partition)
        
        if not response:
            self.logger.warning(f"Batch request failed for {len(batch_partitions)} queries")
            return
            
        try:
            if not response["data"]:
                self.logger.warning(f"Batch request failed for {len(batch_partitions)} queries")
                return
        except (KeyError, TypeError):
            self.logger.warning(f"Batch request failed for {len(batch_partitions)} queries")
            return
        
        # Parse batch results and yield records using EAFP
        try:
            data = response["data"]
            for i, partition in enumerate(batch_partitions):
                try:
                    search_result = data[f"search{i}"]
                    # Store partition context for post_process
                    self._current_partition_context = partition
                    yield search_result
                except KeyError:
                    self.logger.warning(f"No result for search{i} in batch")
        except (KeyError, TypeError):
            self.logger.warning(f"Invalid response structure for batch request")

    def _make_batch_graphql_request(self, query: str, variables: dict, partition: dict) -> dict | None:
        """Make a batched GraphQL request using instance-specific configuration."""
        instances = self._get_github_instances()
        return self._http_client.make_batch_request(query, variables, partition, instances)

    @property
    def authenticator(self) -> GitHubTokenAuthenticator:
        """Use instance-specific authentication.
        
        Note: For multi-instance support, we override the config with 
        instance-specific tokens based on current partition context.
        """
        # Create a copy of config for this instance
        instance_config = dict(self.config)
        
        # Override with instance-specific token if we have partition context
        if hasattr(self, '_current_partition'):
            source = self._current_partition.get('source', 'github.com')
            instances = self._get_github_instances()
            
            # Find the matching instance and use its token
            for instance in instances:
                if instance.name == source:
                    instance_config['auth_token'] = instance.auth_token
                    break
        
        # For multi-instance support, we need instance-specific authenticators
        # Since the authenticator is mainly used for token management and 
        # we handle tokens explicitly in our requests, we can use the default authenticator
        # and override tokens as needed in _make_graphql_request
        return GitHubTokenAuthenticator(stream=self)

    def prepare_request(
        self, context: Mapping[str, Any] | None, next_page_token: Any | None
    ) -> Any:
        """Store current partition for authentication and set API URL."""
        if context:
            self._current_partition = dict(context)  # Convert to dict for consistency
            # Override the API URL base for this request
            if "api_url_base" in context:
                self._url_base = context["api_url_base"]

        return super().prepare_request(context, next_page_token)

    @property
    def url_base(self) -> str:
        """Return the API URL base, allowing per-request override."""
        if hasattr(self, "_url_base"):
            return f"{self._url_base}/graphql"
        return super().url_base

    def post_process(self, row: dict, context: Mapping[str, Any] | None = None) -> dict:
        """Transform GraphQL response to our schema."""
        partition_context = getattr(self, "_current_partition_context", {})

        # Extract org and repo from search query
        search_query = partition_context.get("search_query", "")
        org, repo = self._extract_org_repo_from_query(search_query)

        count_value = row.get("issueCount", 0) if row else 0

        return {
            "search_name": partition_context.get("search_name"),
            "search_query": search_query,
            "source": partition_context.get("source", "github.com"),
            "month": partition_context.get("month"),
            "org": org,
            "repo": repo,
            "updated_at": datetime.utcnow().isoformat(),
            self.count_field: count_value,
        }

    def _extract_org_repo_from_query(
        self, search_query: str
    ) -> tuple[str | None, str | None]:
        """Extract organization and repository from GitHub search query.

        Args:
            search_query: GitHub search query string

        Returns:
            Tuple of (org, repo) where repo is None for org-level queries

        Examples:
            "org:Automattic type:issue" → ("Automattic", None)
            "repo:Automattic/wp-calypso type:issue" → ("Automattic", "wp-calypso")
        """
        # Check for repo-level query first (repo:owner/name)
        repo_match = re.search(r"repo:([^/\s]+)/([^\s]+)", search_query)
        if repo_match:
            org = repo_match.group(1)
            repo = repo_match.group(2)
            return org, repo

        # Check for org-level query (org:name)
        org_match = re.search(r"org:([^\s]+)", search_query)
        if org_match:
            org = org_match.group(1)
            return org, None

        # Fallback if no match found
        return None, None

    def validate_response(self, response) -> None:
        """Validate HTTP response and handle GraphQL errors."""
        super().validate_response(response)

        # Use EAFP - try to access errors directly
        try:
            json_resp = response.json()
            for error in json_resp["errors"]:
                error_msg = error.get("message", str(error))
                if "rate limit" in error_msg.lower():
                    raise RetriableAPIError(f"GitHub API rate limit: {error_msg}")
                self.logger.warning(f"GraphQL query error: {error_msg}")
        except (KeyError, TypeError):
            # No errors or malformed response structure - continue
            pass


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
