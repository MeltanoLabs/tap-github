"""Simplified GitHub search count streams using more Singer SDK features."""

from __future__ import annotations

import calendar
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any, ClassVar

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_github.authenticator import GitHubTokenAuthenticator
from tap_github.client import GitHubGraphqlStream
from tap_github.utils.http_client import GitHubGraphQLClient
from tap_github.utils.repository_discovery import GitHubInstance


class BaseSearchCountStream(GitHubGraphqlStream):
    """Simplified base stream for GitHub search count queries via GraphQL."""

    primary_keys: ClassVar[list[str]] = ["search_name", "month", "source"]
    replication_key = None
    state_partitioning_keys: ClassVar[list[str]] = ["source", "search_name"]
    stream_type: ClassVar[str] = "issue"
    count_field: ClassVar[str] = "issue_count"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._batch_size = self.config.get("batch_query_size", 20)
        self._http_client = GitHubGraphQLClient(self.config, self.logger)
        self._authenticator = None

    @property
    def partitions(self) -> list[dict] | None:
        """Generate partitions using Singer SDK approach."""
        if "search_count_queries" in self.config:
            return self._build_explicit_partitions()
        
        if "search_scope" in self.config:
            return self._build_scope_partitions()
        
        if "search_orgs" in self.config and "date_range" in self.config:
            return self._build_monthly_partitions()
        
        return None

    def _build_explicit_partitions(self) -> list[dict]:
        """Build partitions from explicit query config."""
        partitions = []
        instances = self._get_github_instances()
        
        for query_config in self.config["search_count_queries"]:
            if query_config.get("type", "issue") == self.stream_type:
                for instance in instances:
                    partitions.append({
                        "search_name": query_config["name"],
                        "search_query": query_config["query"],
                        "source": instance.name,
                        "api_url_base": instance.api_url_base,
                        "month": query_config.get("month"),
                        "type": query_config.get("type", "issue")
                    })
        return partitions

    def _build_scope_partitions(self) -> list[dict]:
        """Build partitions from search_scope configuration."""
        partitions = []
        scope_config = self.config["search_scope"]
        date_range = self.config.get("date_range")
        
        if not date_range:
            self.logger.warning("date_range is required when using search_scope")
            return []

        # Generate month ranges
        start_date = datetime.strptime(date_range["start"], "%Y-%m-%d")
        end_date = datetime.strptime(
            date_range.get("end", self._get_last_complete_month()), "%Y-%m-%d"
        )
        
        current = start_date.replace(day=1)
        month_ranges = []
        while current <= end_date:
            month_id = f"{current.year}-{current.month:02d}"
            last_day = calendar.monthrange(current.year, current.month)[1]
            month_start = f"{current.year}-{current.month:02d}-01"
            month_end = f"{current.year}-{current.month:02d}-{last_day:02d}"
            month_ranges.append((month_start, month_end, month_id))
            
            # Next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        # Generate partitions for each instance
        for instance_config in scope_config.get("instances", []):
            # Org-level partitions
            for org in instance_config.get("org_level", []):
                for start_date, end_date, month_id in month_ranges:
                    for query_type in self._get_query_types():
                        query = self._build_search_query(query_type, org, start_date, end_date)
                        search_name = f"{org}_{query_type}s_{month_id}".replace("-", "_").lower()
                        
                        partitions.append({
                            "search_name": search_name,
                            "search_query": query,
                            "source": instance_config["instance"],
                            "api_url_base": instance_config["api_url_base"],
                            "month": month_id,
                            "type": self.stream_type
                        })
            
            # Repo-level partitions - generate queries for specific repos
            repos = instance_config.get("repo_level", [])
            if repos:
                for start_date, end_date, month_id in month_ranges:
                    for query_type in self._get_query_types():
                        # Generate repo-specific query using listed repos
                        query = self._build_repo_search_query(query_type, None, start_date, end_date, repos)
                        search_name = f"repos_{query_type}s_{month_id}".replace("-", "_").lower()
                        
                        partitions.append({
                            "search_name": search_name,
                            "search_query": query,
                            "source": instance_config["instance"],
                            "api_url_base": instance_config["api_url_base"],
                            "month": month_id,
                            "type": self.stream_type
                        })
        
        return partitions

    def _build_monthly_partitions(self) -> list[dict]:
        """Build monthly partitions from org list and date range."""
        partitions = []
        instances = self._get_github_instances()
        date_range = self.config["date_range"]
        
        # Simple date range generation
        start_date = datetime.strptime(date_range["start"], "%Y-%m-%d")
        end_date = datetime.strptime(
            date_range.get("end", self._get_last_complete_month()), "%Y-%m-%d"
        )
        
        current = start_date.replace(day=1)
        while current <= end_date:
            month_id = f"{current.year}-{current.month:02d}"
            last_day = calendar.monthrange(current.year, current.month)[1]
            month_start = f"{current.year}-{current.month:02d}-01"
            month_end = f"{current.year}-{current.month:02d}-{last_day:02d}"
            
            for org in self.config["search_orgs"]:
                for instance in instances:
                    # Generate search queries for this org/month
                    for query_type in self._get_query_types():
                        query = self._build_search_query(query_type, org, month_start, month_end)
                        search_name = f"{org}_{query_type}s_{month_id}".replace("-", "_").lower()
                        
                        partitions.append({
                            "search_name": search_name,
                            "search_query": query,
                            "source": instance.name,
                            "api_url_base": instance.api_url_base,
                            "month": month_id,
                            "type": self.stream_type
                        })
            
            # Next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
                
        return partitions

    def _get_last_complete_month(self) -> str:
        """Get last complete month as end date."""
        today = datetime.now()
        if today.month == 1:
            last_month = today.replace(year=today.year - 1, month=12, day=1)
        else:
            last_month = today.replace(month=today.month - 1, day=1)
        
        last_day = calendar.monthrange(last_month.year, last_month.month)[1]
        return last_month.replace(day=last_day).strftime("%Y-%m-%d")

    def _get_query_types(self) -> list[str]:
        """Get query types for this stream."""
        return ["issue", "bug"] if self.stream_type == "issue" else ["pr"]

    def _build_search_query(self, query_type: str, org: str, start_date: str, end_date: str) -> str:
        """Build a GitHub search query."""
        base_query = f"org:{org}"
        
        if query_type == "issue":
            base_query += " type:issue is:open"
        elif query_type == "bug":
            base_query += " type:issue is:open label:bug"
        elif query_type == "pr":
            base_query += " type:pr is:open"
            
        base_query += f" created:{start_date}..{end_date}"
        return base_query
    
    def _build_repo_search_query(self, query_type: str, org: str, start_date: str, end_date: str, repos: list[str] = None) -> str:
        """Build a GitHub search query for specific repos."""
        if not repos:
            # Fallback to org-level query if no repos specified
            return self._build_search_query(query_type, org, start_date, end_date)
        
        repo_query = " ".join([f"repo:{repo}" for repo in repos[:10]])  # limit to 10 repos
        
        if query_type == "issue":
            base_query = f"{repo_query} type:issue is:open"
        elif query_type == "bug":
            base_query = f"{repo_query} type:issue is:open label:bug"
        elif query_type == "pr":
            base_query = f"{repo_query} type:pr is:open"
            
        base_query += f" created:{start_date}..{end_date}"
        return base_query
    
    def _get_repos_for_org(self, org: str) -> list[str]:
        """Get repository list from config for an org."""
        # Check if repositories are specified in config
        if "repositories" in self.config:
            # Filter repositories for this org
            return [repo for repo in self.config["repositories"] if repo.startswith(f"{org}/")]
        
        # Check if there's a repo_configs section
        if "repo_configs" in self.config:
            org_config = self.config["repo_configs"].get(org, {})
            return org_config.get("repositories", [])
            
        # No specific repos configured, return empty (will fallback to org-level)
        return []

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Process records with GraphQL batching optimization."""
        # Handle single partition (Singer SDK style)
        if context and context.get("partition"):
            yield from self._process_single_partition(context["partition"])
            return

        # Handle all partitions with batching
        partitions = self.partitions or []
        if not partitions:
            return

        # Group by instance for efficient batching
        instance_groups: dict[str, list[dict]] = {}
        for partition in partitions:
            instance = partition["source"]
            if instance not in instance_groups:
                instance_groups[instance] = []
            instance_groups[instance].append(partition)

        # Process each instance's partitions in batches
        for instance_partitions in instance_groups.values():
            yield from self._process_in_batches(instance_partitions)

    def _process_single_partition(self, partition: dict) -> Iterable[dict[str, Any]]:
        """Process a single partition."""
        yield from self._process_batch([partition])

    def _process_in_batches(self, partitions: list[dict]) -> Iterable[dict[str, Any]]:
        """Process partitions in batches for performance."""
        for i in range(0, len(partitions), self._batch_size):
            batch = partitions[i:i + self._batch_size]
            yield from self._process_batch(batch)

    def _process_batch(self, batch_partitions: list[dict]) -> Iterable[dict[str, Any]]:
        """Process a batch of partitions with a single GraphQL request."""
        if not batch_partitions:
            return

        # Build batched GraphQL query
        queries = [p["search_query"] for p in batch_partitions]
        batch_query = self._build_batch_query(queries)
        variables = {f"q{i}": query for i, query in enumerate(queries)}

        # Make request
        first_partition = batch_partitions[0]
        instances = self._get_github_instances()
        response = self._http_client.make_batch_request(batch_query, variables, first_partition, instances)

        if not response or not response.get("data"):
            self.logger.warning(f"Batch request failed for {len(batch_partitions)} queries")
            return

        # Parse results
        data = response["data"]
        graphql_field = "issueCount"  # GitHub API always uses issueCount for all search types
        
        for i, partition in enumerate(batch_partitions):
            try:
                search_result = data[f"search{i}"]
                count_value = search_result.get(graphql_field, 0)
                
                result = {
                    "search_name": partition["search_name"],
                    "search_query": partition["search_query"],
                    "source": partition["source"],
                    self.count_field: count_value,
                    "updated_at": datetime.now().isoformat()
                }
                
                if partition.get("month"):
                    result["month"] = partition["month"]
                    
                yield result
            except KeyError:
                self.logger.warning(f"No result for search{i} in batch")

    def _build_batch_query(self, search_queries: list[str]) -> str:
        """Build batched GraphQL query."""
        variables = [f"$q{i}: String!" for i in range(len(search_queries))]
        
        if self.stream_type == "issue":
            search_type = "ISSUE"
            graphql_field = "issueCount"
        else:
            search_type = "ISSUE"  # GitHub API uses ISSUE for both issues and PRs
            graphql_field = "issueCount"  # GitHub API always uses issueCount field
        
        searches = [
            f"search{i}: search(query: $q{i}, type: {search_type}, first: 1) {{\n            {graphql_field}\n          }}"
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

    def _get_github_instances(self) -> list[GitHubInstance]:
        """Get GitHub instances with simple token resolution."""
        import os
        
        # Simple token resolution
        default_token = (
            self.config.get("auth_token") 
            or self.config.get("access_token")
            or os.environ.get("GITHUB_TOKEN")
        )
        
        if not default_token:
            self.logger.warning("No GitHub token found")
            return []

        instances_config = self.config.get(
            "github_instances",
            [{
                "name": "github.com",
                "api_url_base": "https://api.github.com",
                "auth_token": default_token,
            }]
        )

        instances = []
        for instance_config in instances_config:
            token = instance_config.get("auth_token", default_token)
            if token:
                instances.append(GitHubInstance(
                    name=instance_config["name"],
                    api_url_base=instance_config["api_url_base"],
                    auth_token=token
                ))
        
        return instances

    @classmethod
    def get_schema(cls) -> dict:
        """Get stream schema."""
        return th.PropertiesList(
            th.Property("search_name", th.StringType, required=True),
            th.Property("search_query", th.StringType, required=True),
            th.Property("source", th.StringType, required=True),
            th.Property("month", th.StringType),
            th.Property(cls.count_field, th.IntegerType, required=True),
            th.Property("updated_at", th.DateTimeType),
        ).to_dict()

    @property
    def authenticator(self) -> GitHubTokenAuthenticator:
        """Simple authenticator."""
        if self._authenticator is None:
            self._authenticator = GitHubTokenAuthenticator(stream=self)
        return self._authenticator

    @property
    def query(self) -> str:
        """Single query GraphQL (for SDK compatibility)."""
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

    def prepare_request_payload(self, context: Mapping[str, Any] | None, next_page_token: Any | None) -> dict:
        """Prepare GraphQL request payload."""
        variables = {}
        if context:
            variables["searchQuery"] = context.get("search_query")
        
        return {
            "query": self.query,
            "variables": variables,
        }


class IssueSearchCountStream(BaseSearchCountStream):
    """Simplified stream for GitHub issue search counts."""

    name = "issue_search_counts"
    stream_type = "issue"
    count_field = "issue_count"
    
    @property
    def schema(self) -> dict:
        """Get schema for issue search counts."""
        return th.PropertiesList(
            th.Property("search_name", th.StringType, required=True),
            th.Property("search_query", th.StringType, required=True),
            th.Property("source", th.StringType, required=True),
            th.Property("month", th.StringType),
            th.Property("issue_count", th.IntegerType, required=True),
            th.Property("updated_at", th.DateTimeType),
        ).to_dict()


class PRSearchCountStream(BaseSearchCountStream):
    """Simplified stream for GitHub PR search counts."""

    name = "pr_search_counts"
    stream_type = "pr"
    count_field = "pr_count"
    
    @property
    def schema(self) -> dict:
        """Get schema for PR search counts."""
        return th.PropertiesList(
            th.Property("search_name", th.StringType, required=True),
            th.Property("search_query", th.StringType, required=True),
            th.Property("source", th.StringType, required=True),
            th.Property("month", th.StringType),
            th.Property("pr_count", th.IntegerType, required=True),
            th.Property("updated_at", th.DateTimeType),
        ).to_dict()