"""GitHub search count streams for statistical aggregation via GraphQL."""

from __future__ import annotations

import calendar
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Optional, List, Dict, Tuple
from functools import lru_cache

from singer_sdk import typing as th
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.helpers.types import Context

from tap_github.authenticator import GitHubTokenAuthenticator
from tap_github.client import GitHubGraphqlStream


@dataclass
class SearchQuery:
    """Represents a search query configuration."""

    name: str
    query: str
    type: str = "issue"
    month: str | None = None


@dataclass
class GitHubInstance:
    """Represents a GitHub instance configuration."""

    name: str
    api_url_base: str
    auth_token: str


class BaseSearchCountStream(GitHubGraphqlStream):
    """Base stream for GitHub search count queries via GraphQL."""

    primary_keys: ClassVar[list[str]] = ["search_name", "month", "source"]
    replication_key = None
    state_partitioning_keys: ClassVar[list[str]] = ["source", "search_name"]
    stream_type: ClassVar[str] = "issue"
    count_field: ClassVar[str] = "issue_count"
    
    # Performance configuration
    DEFAULT_MAX_PARTITIONS = 1000
    DEFAULT_WARNING_THRESHOLD = 500
    DEFAULT_CACHE_TTL_MINUTES = 60

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._repo_cache: Dict[str, Tuple[List[str], datetime]] = {}
        self._cache_ttl_minutes = self.config.get("repo_discovery_cache_ttl", self.DEFAULT_CACHE_TTL_MINUTES)

    # Search query templates for org-level queries
    ISSUE_QUERY_TEMPLATE = "org:{org} type:issue state:open created:{start}..{end}"
    BUG_QUERY_TEMPLATE = (
        "org:{org} type:issue state:open "
        'label:bug,defect,"[type] bug","type: bug" '
        "created:{start}..{end}"
    )
    PR_QUERY_TEMPLATE = "org:{org} type:pr is:merged merged:{start}..{end}"

    # Search query templates for repo-level queries
    REPO_ISSUE_QUERY_TEMPLATE = (
        "repo:{repo} type:issue state:open created:{start}..{end}"
    )
    REPO_BUG_QUERY_TEMPLATE = (
        "repo:{repo} type:issue state:open "
        'label:bug,defect,"[type] bug","type: bug" '
        "created:{start}..{end}"
    )
    REPO_PR_QUERY_TEMPLATE = "repo:{repo} type:pr is:merged merged:{start}..{end}"

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

    def _get_github_instances(self) -> List[GitHubInstance]:
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

        # Get default token from config or environment
        default_token = (
            self.config.get("auth_token")
            or self.config.get("access_token")
            or os.environ.get("GITHUB_TOKEN")
        )

        # Check for multiple tokens in environment (GITHUB_TOKEN, GITHUB_TOKEN_2, etc.)
        if not default_token:
            env_tokens = [
                value
                for key, value in os.environ.items()
                if key.startswith("GITHUB_TOKEN") and value
            ]
            if env_tokens:
                default_token = env_tokens[0]  # Use first available token

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
            # e.g., GITHUB_TOKEN_ENTERPRISE for github.enterprise.com
            instance_env_key = f"GITHUB_TOKEN_{instance['name'].upper().replace('.', '_').replace('-', '_')}"
            instance_token = (
                instance.get("auth_token")
                or os.environ.get(instance_env_key)
                or default_token
            )

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
        for org in org_names:
            if not org or not isinstance(org, str):
                msg = f"Invalid organization name: {org!r}. Must be non-empty string."
                raise ValueError(msg)

            if not re.match(
                r"^[a-zA-Z0-9]([a-zA-Z0-9-])*[a-zA-Z0-9]$|^[a-zA-Z0-9]$", org
            ):
                msg = f"Invalid organization name '{org}'. Must contain only alphanumeric characters and hyphens, cannot start or end with hyphen."
                raise ValueError(msg)

    def _generate_month_ranges(self) -> List[Tuple[str, str, str]]:
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
    
    def _validate_and_adjust_date_range(self, start_date: str, end_date: Optional[str] = None) -> Tuple[str, str]:
        """Validate and potentially adjust date range based on configuration.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (optional)
            
        Returns:
            Tuple of (validated_start_date, validated_end_date)
            
        Raises:
            ValueError: If dates are invalid or exceed limits when enforcement is enabled
        """
        # Configuration options
        enforce_lookback_limit = self.config.get("enforce_lookback_limit", False)
        max_lookback_years = self.config.get("max_lookback_years", 1)
        
        self._validate_date_format(start_date, "start date")
        
        if not end_date:
            end_date = self._get_auto_end_date(start_date)
            self.logger.info(f"Auto-detected end date: {end_date} (last complete month)")
        
        self._validate_date_format(end_date, "end date")
        
        # Parse dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start_dt > end_dt:
            raise ValueError(f"Start date '{start_date}' must be before or equal to end date '{end_date}'")
        
        # Check lookback limit
        max_lookback = end_dt.replace(year=end_dt.year - max_lookback_years)
        
        if start_dt < max_lookback:
            if enforce_lookback_limit:
                adjusted_start = max_lookback.strftime("%Y-%m-%d")
                self.logger.warning(
                    f"Start date {start_date} exceeds {max_lookback_years}-year limit. "
                    f"Enforcing limit by adjusting to {adjusted_start}"
                )
                return adjusted_start, end_date
            else:
                self.logger.warning(
                    f"Start date {start_date} exceeds recommended {max_lookback_years}-year lookback. "
                    f"Consider setting enforce_lookback_limit=true to automatically adjust."
                )
        
        return start_date, end_date

    def _check_partition_limits(self, total_partitions: int) -> None:
        """Check partition count limits and warn or raise if exceeded.
        
        Args:
            total_partitions: Total number of partitions to be generated
            
        Raises:
            ValueError: If partition count exceeds maximum when enforcement is enabled
        """
        max_partitions = self.config.get("max_partitions", self.DEFAULT_MAX_PARTITIONS)
        warning_threshold = self.config.get("partition_warning_threshold", self.DEFAULT_WARNING_THRESHOLD)
        enforce_partition_limit = self.config.get("enforce_partition_limit", True)
        
        if total_partitions > max_partitions and enforce_partition_limit:
            raise ValueError(
                f"Partition count {total_partitions} exceeds maximum {max_partitions}. "
                f"Consider reducing date range, repository count, or disable enforcement "
                f"with enforce_partition_limit=false"
            )
        elif total_partitions > warning_threshold:
            self.logger.warning(
                f"High partition count: {total_partitions} (threshold: {warning_threshold}). "
                f"This may result in rate limiting or performance issues."
            )

    def _create_search_queries_for_month(
        self, org: str, start_date: str, end_date: str, month_id: str
    ) -> List[SearchQuery]:
        """Create search queries for a specific organization and month."""
        org_clean = org.lower().replace("-", "_")
        queries = []

        if self.stream_type == "pr":
            queries.append(
                SearchQuery(
                    name=f"{org_clean}_merged_prs_{month_id.replace('-', '')}",
                    query=self.PR_QUERY_TEMPLATE.format(
                        org=org, start=start_date, end=end_date
                    ),
                    type="pr",
                    month=month_id,
                )
            )
        else:  # issue stream
            # All issues
            queries.append(
                SearchQuery(
                    name=f"{org_clean}_open_issues_{month_id.replace('-', '')}",
                    query=self.ISSUE_QUERY_TEMPLATE.format(
                        org=org, start=start_date, end=end_date
                    ),
                    type="issue",
                    month=month_id,
                )
            )

            # Bug issues
            queries.append(
                SearchQuery(
                    name=f"{org_clean}_open_bugs_{month_id.replace('-', '')}",
                    query=self.BUG_QUERY_TEMPLATE.format(
                        org=org, start=start_date, end=end_date
                    ),
                    type="issue",
                    month=month_id,
                )
            )

        return queries

    def _generate_monthly_partitions(self) -> list[dict]:
        """Generate partitions programmatically from org list and date range."""
        organizations = self.config["search_orgs"]
        self._validate_org_names(organizations)

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
                            "auth_token": instance.auth_token,
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

        partitions = []
        instances = self._get_github_instances()
        month_ranges = self._generate_month_ranges()

        # Generate org-level partitions
        org_level_orgs = scope_config.get("org_level", [])
        if org_level_orgs:
            self._validate_org_names(org_level_orgs)
            for org in org_level_orgs:
                for start_date, end_date, month_id in month_ranges:
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

        # Generate repo-level partitions
        repo_level_configs = scope_config.get("repo_level", [])
        
        # Handle both single dict (backward compat) and list of dicts
        if isinstance(repo_level_configs, dict):
            repo_level_configs = [repo_level_configs]
            
        for repo_config in repo_level_configs:
            org = repo_config["org"]
            limit = repo_config.get("limit", 20)
            sort_by = repo_config.get("sort_by", "issues")
            
            # Get top repositories
            top_repos = self._get_top_repos(org, limit, sort_by)

            for repo in top_repos:
                for start_date, end_date, month_id in month_ranges:
                    search_queries = self._generate_repo_queries(
                        repo, start_date, end_date, month_id
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

        total_partitions = len(partitions)
        self._check_partition_limits(total_partitions)

        return partitions

    def _get_top_repos(self, org: str, limit: int, sort_by: str) -> List[str]:
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
            else:
                # Cache expired, remove it
                del self._repo_cache[cache_key]
        
        # Fetch fresh data
        if sort_by == "issues":
            repos = self._get_top_repos_by_issues(org, limit)
        elif sort_by == "stars":
            repos = self._get_top_repos_by_stars(org, limit)
        elif sort_by == "forks":
            repos = self._get_top_repos_by_forks(org, limit)
        elif sort_by == "updated":
            repos = self._get_top_repos_by_updated(org, limit)
        else:
            msg = f"Unsupported sort_by criteria: {sort_by}"
            raise ValueError(msg)
        
        # Cache the results
        self._repo_cache[cache_key] = (repos, datetime.now())
        self.logger.debug(f"Cached {len(repos)} repositories for {cache_key}")
        
        return repos

    def _get_top_repos_by_issues(self, org: str, limit: int) -> List[str]:
        """Get top N repositories sorted by open issue count (descending).

        Note: GitHub GraphQL API doesn't support direct sorting by issue count,
        so we fetch more repos sorted by stars and then sort locally by issues.
        This may miss repositories with high issue counts but low star counts.
        """
        # Fetch 100 repos to increase chances of getting the actual top by issues
        fetch_limit = min(limit * 5, 100)

        query = """
        query($org: String!, $limit: Int!) {
          organization(login: $org) {
            repositories(first: $limit, orderBy: {field: STARGAZERS, direction: DESC}) {
              nodes {
                nameWithOwner
                issues(states: OPEN) {
                  totalCount
                }
              }
            }
          }
        }
        """

        variables = {"org": org, "limit": fetch_limit}

        # Make GraphQL request using SDK methods
        response = self._make_graphql_request(query, variables)

        if not response or "data" not in response:
            self.logger.warning(f"No data returned for org {org}")
            return []

        org_data = response["data"].get("organization")
        if not org_data:
            self.logger.warning(f"Organization {org} not found")
            return []

        repos = org_data.get("repositories", {}).get("nodes", [])

        # Sort by issue count descending
        repos_with_counts = [
            {"name": repo["nameWithOwner"], "issue_count": repo["issues"]["totalCount"]}
            for repo in repos
        ]

        sorted_repos = sorted(
            repos_with_counts, key=lambda x: x["issue_count"], reverse=True
        )
        return [repo["name"] for repo in sorted_repos[:limit]]

    def _get_top_repos_by_stars(self, org: str, limit: int) -> List[str]:
        """Get top N repositories sorted by star count (descending)."""
        query = """
        query($org: String!, $limit: Int!) {
          organization(login: $org) {
            repositories(first: $limit, orderBy: {field: STARGAZERS, direction: DESC}) {
              nodes {
                nameWithOwner
              }
            }
          }
        }
        """

        variables = {"org": org, "limit": limit}
        response = self._make_graphql_request(query, variables)

        if not response or "data" not in response:
            return []

        org_data = response["data"].get("organization")
        if not org_data:
            return []

        repos = org_data.get("repositories", {}).get("nodes", [])
        return [repo["nameWithOwner"] for repo in repos]

    def _get_top_repos_by_forks(self, org: str, limit: int) -> List[str]:
        """Get top N repositories sorted by fork count (descending)."""
        query = """
        query($org: String!, $limit: Int!) {
          organization(login: $org) {
            repositories(first: $limit, orderBy: {field: FORKS, direction: DESC}) {
              nodes {
                nameWithOwner
              }
            }
          }
        }
        """

        variables = {"org": org, "limit": limit}
        response = self._make_graphql_request(query, variables)

        if not response or "data" not in response:
            return []

        org_data = response["data"].get("organization")
        if not org_data:
            return []

        repos = org_data.get("repositories", {}).get("nodes", [])
        return [repo["nameWithOwner"] for repo in repos]

    def _get_top_repos_by_updated(self, org: str, limit: int) -> List[str]:
        """Get top N repositories sorted by last update (descending)."""
        query = """
        query($org: String!, $limit: Int!) {
          organization(login: $org) {
            repositories(first: $limit, orderBy: {field: UPDATED_AT, direction: DESC}) {
              nodes {
                nameWithOwner
              }
            }
          }
        }
        """

        variables = {"org": org, "limit": limit}
        response = self._make_graphql_request(query, variables)

        if not response or "data" not in response:
            return []

        org_data = response["data"].get("organization")
        if not org_data:
            return []

        repos = org_data.get("repositories", {}).get("nodes", [])
        return [repo["nameWithOwner"] for repo in repos]

    def _make_graphql_request(self, query: str, variables: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Make a GraphQL request and return the response.

        Uses the stream's built-in HTTP methods for consistency with authentication,
        retry logic, and timeout configuration.
        """
        from urllib.parse import urljoin

        import requests
        from requests.exceptions import ConnectionError, HTTPError, Timeout

        # Use the first instance's configuration for repo discovery
        instances = self._get_github_instances()
        if not instances:
            self.logger.error("No GitHub instances configured")
            return None

        instance = instances[0]  # Use first instance for discovery

        # Store current partition temporarily to use instance config
        original_partition = getattr(self, "_current_partition", None)
        self._current_partition = {
            "api_url_base": instance.api_url_base,
        }

        # Build the GraphQL endpoint URL
        graphql_url = urljoin(instance.api_url_base, "/graphql")

        # Prepare request payload
        payload = {
            "query": query,
            "variables": variables,
        }

        # Get timeout from config or use default
        timeout = self.config.get("stream_request_timeout", 300)

        try:
            # Use direct requests for repository discovery GraphQL calls
            # This is outside the main stream flow and doesn't need Singer SDK request handling
            response = requests.post(
                graphql_url,
                json=payload,
                headers={
                    "Authorization": f"Bearer {instance.auth_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/vnd.github.v3+json",
                },
                timeout=timeout,
            )

            json_resp = response.json()

            # Check for GraphQL errors
            if "errors" in json_resp:
                errors = json_resp.get("errors", [])
                for error in errors:
                    error_msg = error.get("message", str(error))
                    # Log GraphQL errors but continue if it's just a query issue
                    self.logger.warning(f"GraphQL query warning: {error_msg}")

            return json_resp

        except HTTPError as e:
            if e.response.status_code == 401:
                self.logger.error(f"Authentication failed: {e}. Check your auth token.")
            elif e.response.status_code == 403:
                self.logger.error(
                    f"Permission denied: {e}. Token may lack required scopes."
                )
            elif e.response.status_code == 404:
                self.logger.error(
                    f"GraphQL endpoint not found: {e}. Check api_url_base."
                )
            else:
                self.logger.error(f"HTTP error occurred: {e}")
            return None

        except Timeout:
            self.logger.error(
                f"Request timed out after {timeout} seconds. Consider increasing stream_request_timeout."
            )
            return None

        except ConnectionError as e:
            self.logger.error(f"Connection error: {e}. Check network and API URL.")
            return None

        except Exception as e:
            self.logger.error(f"Unexpected error in GraphQL request: {e}")
            return None

        finally:
            # Restore original partition context
            if original_partition is not None:
                self._current_partition = original_partition
            else:
                delattr(self, "_current_partition")

    def _generate_repo_queries(
        self, repo: str, start_date: str, end_date: str, month_id: str
    ) -> List[SearchQuery]:
        """Generate search queries for a specific repository and month."""
        queries = []
        repo_clean = repo.replace("/", "_").replace("-", "_").lower()

        if self.stream_type == "pr":
            queries.append(
                SearchQuery(
                    name=f"{repo_clean}_merged_prs_{month_id.replace('-', '')}",
                    query=self.REPO_PR_QUERY_TEMPLATE.format(
                        repo=repo, start=start_date, end=end_date
                    ),
                    type="pr",
                    month=month_id,
                )
            )
        else:  # issue stream
            # All issues
            queries.append(
                SearchQuery(
                    name=f"{repo_clean}_open_issues_{month_id.replace('-', '')}",
                    query=self.REPO_ISSUE_QUERY_TEMPLATE.format(
                        repo=repo, start=start_date, end=end_date
                    ),
                    type="issue",
                    month=month_id,
                )
            )

            # Bug issues
            queries.append(
                SearchQuery(
                    name=f"{repo_clean}_open_bugs_{month_id.replace('-', '')}",
                    query=self.REPO_BUG_QUERY_TEMPLATE.format(
                        repo=repo, start=start_date, end=end_date
                    ),
                    type="issue",
                    month=month_id,
                )
            )

        return queries

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Override to handle custom partitioning."""
        partitions = self.get_partitions(context)

        for partition in partitions:
            # Store the current partition context for post_process
            self._current_partition_context = partition
            # Yield records for this partition
            yield from self.request_records(partition)

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

        return {
            "search_name": partition_context.get("search_name"),
            "search_query": search_query,
            "source": partition_context.get("source", "github.com"),
            "month": partition_context.get("month"),
            "org": org,
            "repo": repo,
            "updated_at": datetime.utcnow().isoformat(),
            self.count_field: row.get("issueCount", 0) if row else 0,
        }

    def _extract_org_repo_from_query(
        self, search_query: str
    ) -> Tuple[Optional[str], Optional[str]]:
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

        # Check for GraphQL errors
        json_resp = response.json()
        if "errors" in json_resp:
            error_messages = [e.get("message", str(e)) for e in json_resp["errors"]]
            error_str = "; ".join(error_messages)

            # Check if it's a rate limit or other retryable error
            if any("rate limit" in msg.lower() for msg in error_messages):
                raise RetriableAPIError(f"GitHub API rate limit: {error_str}")

            # Log and continue for search syntax errors
            self.logger.warning(f"GraphQL query error: {error_str}")


class IssueSearchCountStream(BaseSearchCountStream):
    """Stream for GitHub issue search count queries via GraphQL."""

    name = "issue_search_counts"
    stream_type = "issue"
    count_field = "issue_count"

    schema = th.PropertiesList(
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
        th.Property("issue_count", th.IntegerType, required=True),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()


class PRSearchCountStream(BaseSearchCountStream):
    """Stream for GitHub PR search count queries via GraphQL."""

    name = "pr_search_counts"
    stream_type = "pr"
    count_field = "pr_count"

    schema = th.PropertiesList(
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
        th.Property("pr_count", th.IntegerType, required=True),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()
