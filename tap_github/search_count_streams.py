"""Simplified GitHub search count streams using more Singer SDK features."""

from __future__ import annotations

import calendar
import time
from collections.abc import Iterable, Mapping
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, ClassVar
import requests

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
        self._batch_size = self.config.get("batch_query_size", 10)
        self._http_client = GitHubGraphQLClient(self.config, self.logger)
        self._authenticator = None
        self._repo_cache = {}  # Cache repositories by org

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

        # Get stream-specific or general instance configuration
        stream_key = f"{self.stream_type}_streams"
        if stream_key in scope_config:
            # Use stream-specific config (e.g., "issue_streams" or "pr_streams")
            instances_config = scope_config[stream_key].get("instances", [])
        else:
            # Fall back to general "instances" config
            instances_config = scope_config.get("instances", [])

        # Generate partitions for each instance
        for instance_config in instances_config:
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
    
    def _discover_org_repositories(self, org: str, api_url_base: str, auth_token: str) -> list[str]:
        """Discover all repositories for an organization."""
        if org in self._repo_cache:
            self.logger.info(f"Using cached repositories for {org}")
            return self._repo_cache[org]
        
        self.logger.info(f"Discovering repositories for organization: {org}")
        
        url = f"{api_url_base}/graphql"
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
        
        query = """
        query GetOrgRepositories($org: String!, $after: String) {
          organization(login: $org) {
            repositories(first: 100, after: $after, orderBy: {field: NAME, direction: ASC}) {
              pageInfo { hasNextPage endCursor }
              nodes {
                name
                isPrivate
                isArchived
                isDisabled
              }
            }
          }
        }
        """
        
        repositories = []
        cursor = None
        page = 0
        
        while True:
            page += 1
            try:
                variables = {"org": org, "after": cursor}
                response = requests.post(url, json={"query": query, "variables": variables}, headers=headers)
                
                if response.status_code != 200:
                    self.logger.error(f"HTTP {response.status_code} fetching repos for {org}")
                    break
                
                data = response.json()
                if "errors" in data:
                    self.logger.error(f"GraphQL errors for {org}: {data['errors']}")
                    break
                
                org_data = data.get("data", {}).get("organization")
                if not org_data:
                    self.logger.warning(f"No organization data found for {org}")
                    break
                
                repos_data = org_data.get("repositories", {})
                for repo in repos_data.get("nodes", []):
                    if repo and not repo.get("isDisabled", False):  # Skip disabled repos
                        repositories.append(repo["name"])
                
                page_info = repos_data.get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    break
                    
                cursor = page_info.get("endCursor")
                
                # Safety limit
                if page >= 50:
                    self.logger.warning(f"Reached page limit for {org}, stopping")
                    break
                    
            except Exception as e:
                self.logger.error(f"Error discovering repos for {org}: {e}")
                break
        
        self.logger.info(f"Discovered {len(repositories)} repositories for {org}")
        
        # Cache the results
        self._repo_cache[org] = repositories
        return repositories
    
    def _search_with_repo_breakdown(self, org: str, search_query: str, api_url_base: str, auth_token: str) -> dict[str, int]:
        """Search with repository-level breakdown using ChatGPT's approach."""
        url = f"{api_url_base}/graphql"
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
        
        query = """
        query SearchIssuesWithRepoBreakdown($query: String!, $after: String) {
          search(query: $query, type: ISSUE, first: 100, after: $after) {
            issueCount
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              ... on Issue {
                repository {
                  name
                }
              }
            }
          }
        }
        """
        
        repo_counts = defaultdict(int)
        cursor = None
        page = 0
        
        while True:
            page += 1
            try:
                variables = {"query": search_query, "after": cursor}
                response = requests.post(url, json={"query": query, "variables": variables}, headers=headers)
                
                if response.status_code != 200:
                    self.logger.error(f"HTTP {response.status_code} for search: {search_query}")
                    break
                
                data = response.json()
                if "errors" in data:
                    self.logger.error(f"GraphQL errors: {data['errors']}")
                    break
                
                search_data = data.get("data", {}).get("search", {})
                if not search_data:
                    break
                
                total_count = search_data.get("issueCount", 0)
                if page == 1:
                    self.logger.info(f"Total results for '{search_query}': {total_count}")
                    
                    # If > 1000, warn about potential pagination limits
                    if total_count > 1000:
                        self.logger.warning(f"Results > 1000, may need date slicing for: {search_query}")
                
                # Process results
                for node in search_data.get("nodes", []):
                    if node and node.get("repository"):
                        repo_name = node["repository"]["name"]
                        repo_counts[repo_name] += 1
                
                # Check for next page
                page_info = search_data.get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    break
                    
                cursor = page_info.get("endCursor")
                
                # GitHub search is limited to 1000 results
                if page >= 10:  # Safety break
                    break
                    
            except Exception as e:
                self.logger.error(f"Error in search breakdown: {e}")
                break
        
        return dict(repo_counts)
    
    def _slice_month_if_needed(self, org: str, query_type: str, month_start: str, month_end: str, api_url_base: str, auth_token: str) -> dict[str, int]:
        """Search for a month, slicing into weeks if results > 1000."""
        
        # Build full search query
        search_query = self._build_search_query(query_type, org, month_start, month_end)
        
        # Try full month first
        repo_counts = self._search_with_repo_breakdown(org, search_query, api_url_base, auth_token)
        
        # Check if we hit the 1000 limit and need to slice
        total_results = sum(repo_counts.values())
        
        if total_results >= 900:  # Close to 1000 limit
            self.logger.info(f"High volume ({total_results}), slicing into weeks for {org} {query_type}")
            
            weekly_counts = defaultdict(int)
            
            # Generate weekly date ranges
            start_datetime = datetime.strptime(month_start, "%Y-%m-%d")
            end_datetime = datetime.strptime(month_end, "%Y-%m-%d")
            
            current_date = start_datetime
            while current_date <= end_datetime:
                week_end = min(current_date + timedelta(days=6), end_datetime)
                
                week_start_str = current_date.strftime("%Y-%m-%d")
                week_end_str = week_end.strftime("%Y-%m-%d")
                
                week_query = self._build_search_query(query_type, org, week_start_str, week_end_str)
                week_counts = self._search_with_repo_breakdown(org, week_query, api_url_base, auth_token)
                
                # Merge weekly counts
                for repo, count in week_counts.items():
                    weekly_counts[repo] += count
                
                current_date = week_end + timedelta(days=1)
            
            return dict(weekly_counts)
        
        return repo_counts
    
    def _should_use_repo_breakdown(self) -> bool:
        """Check if repository breakdown is explicitly enabled for this stream type."""
        # Check for explicit repo_breakdown flag in config
        if self.config.get("repo_breakdown", False):
            return True
            
        # Check search_scope config for repo_breakdown option
        if "search_scope" in self.config:
            scope_config = self.config["search_scope"]
            stream_key = f"{self.stream_type}_streams"
            
            if stream_key in scope_config:
                instances = scope_config[stream_key].get("instances", [])
                for instance in instances:
                    if instance.get("repo_breakdown", False):
                        return True
        
        return False

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Process records with GraphQL batching optimization."""
        self.logger.info(f"Stream {self.name} is selected, beginning processing")
        
        # Handle single partition (Singer SDK style) - context IS the partition
        if context and context.get("search_name"):
            yield from self._process_single_partition(context)
            return

        # Handle all partitions with batching
        partitions = self.partitions or []
        if not partitions:
            self.logger.info("No partitions found, returning")
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

    def _extract_org_from_query(self, search_query: str) -> str:
        """Extract organization name from search query."""
        import re
        
        # Look for org: pattern (e.g., "org:Automattic" -> "Automattic")
        org_match = re.search(r'org:(\w+)', search_query)
        if org_match:
            return org_match.group(1)
        
        # Look for repo: pattern (e.g., "repo:WordPress/Gutenberg" -> "WordPress")
        repo_match = re.search(r'repo:([^/\s]+)/', search_query)
        if repo_match:
            return repo_match.group(1)
        
        # Fallback: extract from search_query or return "unknown"
        return "unknown"

    def _process_batch(self, batch_partitions: list[dict]) -> Iterable[dict[str, Any]]:
        """Process a batch of partitions with repository-level breakdown."""
        if not batch_partitions:
            return

        # Check if repository breakdown is enabled for this stream type
        if self._should_use_repo_breakdown():
            yield from self._process_batch_with_repo_breakdown(batch_partitions)
            return
        
        # Use original processing for org-level aggregates
        yield from self._process_batch_original(batch_partitions)
    
    def _process_batch_with_repo_breakdown(self, batch_partitions: list[dict]) -> Iterable[dict[str, Any]]:
        """Process batch with repository-level breakdown for org-only config."""
        # Group partitions by org/month to avoid duplicate processing
        org_month_groups = {}
        
        for partition in batch_partitions:
            org = self._extract_org_from_query(partition["search_query"])
            month = partition.get("month")
            key = f"{org}_{month}"
            
            if key not in org_month_groups:
                org_month_groups[key] = {
                    "org": org,
                    "month": month,
                    "partitions": [],
                    "source": partition["source"],
                    "api_url_base": partition.get("api_url_base", "https://api.github.com")
                }
            
            org_month_groups[key]["partitions"].append(partition)
        
        # Process each org/month combination once
        for group_key, group_data in org_month_groups.items():
            try:
                org = group_data["org"]
                month = group_data["month"]
                source = group_data["source"]
                api_url_base = group_data["api_url_base"]
                
                # Parse month to get date range
                if month:
                    year, month_num = month.split("-")
                    month_start = f"{year}-{month_num}-01"
                    last_day = calendar.monthrange(int(year), int(month_num))[1]
                    month_end = f"{year}-{month_num}-{last_day:02d}"
                else:
                    self.logger.warning(f"No month found in group: {group_key}")
                    continue
                
                # Get auth token
                auth_token = None
                if "search_scope" in self.config:
                    scope_config = self.config["search_scope"]
                    stream_key = f"{self.stream_type}_streams"
                    instances_config = scope_config.get(stream_key, {}).get("instances", [])
                    if not instances_config:
                        instances_config = scope_config.get("instances", [])
                    
                    for instance_config in instances_config:
                        if instance_config.get("instance") == source:
                            auth_token = instance_config.get("auth_token")
                            break
                
                if not auth_token:
                    auth_token = self.config.get("auth_token") or self.config.get("access_token")
                
                if not auth_token:
                    self.logger.error(f"No auth token found for {org}")
                    continue
                
                # Discover repositories for this org (cached)
                all_repos = self._discover_org_repositories(org, api_url_base, auth_token)
                
                # Process both query types for this org/month
                for partition in group_data["partitions"]:
                    # Detect query type
                    if "type:pr" in partition["search_query"]:
                        query_type = "pr"
                    elif "label:bug" in partition["search_query"]:
                        query_type = "bug"  
                    else:
                        query_type = "issue"
                    
                    # Get repo breakdown from search
                    repo_counts = self._slice_month_if_needed(org, query_type, month_start, month_end, api_url_base, auth_token)
                    
                    # Generate records for all repos (including 0 counts)
                    for repo_name in all_repos:
                        count_value = repo_counts.get(repo_name, 0)
                        
                        result = {
                            "search_name": f"{org}_{repo_name}_{query_type}s_{month}".replace("-", "_").lower(),
                            "search_query": partition["search_query"],
                            "source": source,
                            "org": org,
                            "repo": repo_name,
                            "month": month,
                            self.count_field: count_value,
                            "updated_at": datetime.now().isoformat()
                        }
                        
                        yield result
                
                # Small delay between org/month groups
                time.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"Error processing group {group_key}: {e}")
                continue
    
    def _process_batch_original(self, batch_partitions: list[dict]) -> Iterable[dict[str, Any]]:
        """Original batch processing method for backward compatibility."""
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

        # Small delay to avoid overwhelming GitHub API
        time.sleep(0.5)

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
                    "org": self._extract_org_from_query(partition["search_query"]),
                    "repo": "aggregate",  # Indicate this is org-level aggregate
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
        
        # GitHub GraphQL always uses ISSUE type and issueCount field
        # The actual filtering (issue vs PR) is done in the search query string
        search_type = "ISSUE"
        graphql_field = "issueCount"
        
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
        """Get GitHub instances from configuration."""
        # Handle simple org configuration (search_orgs + auth token)
        if "search_orgs" in self.config:
            auth_token = self.config.get("auth_token") or self.config.get("access_token")
            if auth_token:
                return [GitHubInstance(
                    name="github.com",
                    api_url_base="https://api.github.com",
                    auth_token=auth_token
                )]
        
        # Handle search_count_queries configuration (explicit queries)
        if "search_count_queries" in self.config:
            # Support multiple auth tokens/instances
            instances = []
            
            # Try top-level auth token (single instance - supports both auth_token and access_token)
            auth_token = self.config.get("auth_token") or self.config.get("access_token")
            if auth_token:
                instances.append(GitHubInstance(
                    name="github.com",
                    api_url_base="https://api.github.com",
                    auth_token=auth_token
                ))
            
            # Try instances list (multiple instances)
            if "instances" in self.config:
                for instance_config in self.config["instances"]:
                    instance_auth_token = instance_config.get("auth_token") or instance_config.get("access_token")
                    if instance_auth_token:
                        instances.append(GitHubInstance(
                            name=instance_config.get("instance", instance_config.get("name", "github.com")),
                            api_url_base=instance_config.get("api_url_base", "https://api.github.com"),
                            auth_token=instance_auth_token
                        ))
            
            return instances
        
        # Handle search_scope configuration
        if "search_scope" not in self.config:
            return []
            
        scope_config = self.config["search_scope"]
        instances = []
        
        # Get stream-specific or general instance configuration
        stream_key = f"{self.stream_type}_streams"
        if stream_key in scope_config:
            # Use stream-specific config (e.g., "issue_streams" or "pr_streams")
            instances_config = scope_config[stream_key].get("instances", [])
        else:
            # Fall back to general "instances" config
            instances_config = scope_config.get("instances", [])
        
        for instance_config in instances_config:
            auth_token = instance_config.get("auth_token")
            if auth_token:
                instances.append(GitHubInstance(
                    name=instance_config["instance"],
                    api_url_base=instance_config["api_url_base"],
                    auth_token=auth_token
                ))
        
        return instances

    @classmethod
    def get_schema(cls) -> dict:
        """Get stream schema."""
        return th.PropertiesList(
            th.Property("search_name", th.StringType, required=True),
            th.Property("search_query", th.StringType, required=True),
            th.Property("source", th.StringType, required=True),
            th.Property("org", th.StringType, required=True),
            th.Property("repo", th.StringType, required=True),
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
            th.Property("org", th.StringType, required=True),
            th.Property("repo", th.StringType, required=True),
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
            th.Property("org", th.StringType, required=True),
            th.Property("repo", th.StringType, required=True),
            th.Property("month", th.StringType),
            th.Property("pr_count", th.IntegerType, required=True),
            th.Property("updated_at", th.DateTimeType),
        ).to_dict()