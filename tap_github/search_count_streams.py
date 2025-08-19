"""GitHub search count streams - simplified Singer SDK implementation."""

from __future__ import annotations

import calendar
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, ClassVar, Iterable, Mapping

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_github.client import GitHubGraphqlStream


class SearchCountStreamBase(GitHubGraphqlStream):
    """Base class for GitHub search count streams using simplified configuration."""

    # Configure in subclasses
    stream_type: ClassVar[str] = "issue"  # "issue" | "pr" | "bug"
    count_field: ClassVar[str] = "issue_count"

    primary_keys: ClassVar[list[str]] = ["search_name", "month", "source", "org", "repo"]
    replication_method = "FULL_TABLE"
    state_partitioning_keys: ClassVar[list[str]] = ["source", "org", "month"]

    def __init__(self, tap, name=None, schema=None, path=None):
        """Initialize stream with schema."""
        super().__init__(
            tap=tap,
            name=name or self.name,
            schema=schema or self.get_schema(),
            path=path
        )

    @classmethod
    def get_schema(cls) -> dict:
        """Return schema for search count streams."""
        return th.PropertiesList(
            th.Property("search_name", th.StringType, required=True),
            th.Property("search_query", th.StringType, required=True),
            th.Property("source", th.StringType, required=True),
            th.Property("org", th.StringType, required=True),
            th.Property("repo", th.StringType, required=True),  # "aggregate" or repo name
            th.Property("month", th.StringType, required=True),  # YYYY-MM
            th.Property(cls.count_field, th.IntegerType, required=True),
            th.Property("updated_at", th.DateTimeType),
        ).to_dict()


    @property
    def query(self) -> str:
        """GraphQL query for search with repo breakdown support."""
        return """
        query SearchWithRepos($q: String!, $after: String) {
          search(query: $q, type: ISSUE, first: 100, after: $after) {
            issueCount
            pageInfo { hasNextPage endCursor }
            nodes { 
              ... on Issue { repository { name } }
              ... on PullRequest { repository { name } }
            }
          }
          rateLimit { cost remaining }
        }
        """

    def prepare_request_payload(self, context: Mapping[str, Any] | None, next_page_token: Any | None) -> dict:
        """Prepare GraphQL request - actual requests made in get_records()."""
        return {"query": self.query, "variables": {"q": "", "after": None}}

    @property
    def partitions(self) -> list[dict]:
        """Generate partitions using existing search_scope configuration."""
        cfg = self.config
        
        # Get months to process (backfill or sliding window)
        months = self._get_months_to_process()
        if not months:
            return []

        partitions = []
        
        # Get search_scope configuration
        search_scope = cfg.get("search_scope", {})
        
        # Determine which stream config to use based on stream type
        if self.stream_type == "issue":
            stream_config = search_scope.get("issue_streams", {})
        elif self.stream_type == "pr":
            stream_config = search_scope.get("pr_streams", {})
        else:
            # Fallback for other types
            stream_config = search_scope.get("issue_streams", {})
        
        # Get instances from stream config
        stream_instances = stream_config.get("instances", [])
        
        for month in months:
            year, month_num = map(int, month.split("-"))
            start_date = f"{year:04d}-{month_num:02d}-01"
            last_day = calendar.monthrange(year, month_num)[1]
            end_date = f"{year:04d}-{month_num:02d}-{last_day:02d}"

            for instance_config in stream_instances:
                instance_name = instance_config.get("instance", "github.com")
                api_url_base = instance_config.get("api_url_base", "https://api.github.com")
                repo_breakdown = instance_config.get("repo_breakdown", False)
                
                # Process org-level searches
                for org in instance_config.get("org_level", []):
                    query = self._build_search_query(org, start_date, end_date, self.stream_type)
                    partitions.append({
                        "search_name": f"{org}_{self.stream_type}_{month}",
                        "search_query": query,
                        "source": instance_name,
                        "api_url_base": api_url_base,
                        "org": org,
                        "month": month,
                        "kind": self.stream_type,
                        "repo_breakdown": repo_breakdown
                    })
                
                # Process repo-level searches
                for repo in instance_config.get("repo_level", []):
                    org = repo.split("/")[0]
                    query = self._build_repo_search_query(repo, start_date, end_date, self.stream_type)
                    partitions.append({
                        "search_name": f"{repo.replace('/', '_')}_{self.stream_type}_{month}",
                        "search_query": query,
                        "source": instance_name,
                        "api_url_base": api_url_base,
                        "org": org,
                        "month": month,
                        "kind": self.stream_type,
                        "repo_breakdown": False  # Individual repos don't need breakdown
                    })

        return partitions

    def _get_months_to_process(self) -> list[str]:
        """Get months to process based on backfill or sliding window config."""
        cfg = self.config
        
        # Check for backfill configuration
        if cfg.get("backfill_start_month"):
            start = cfg["backfill_start_month"]
            end = cfg.get("backfill_end_month") or self._get_last_complete_month()
            batch_size = int(cfg.get("backfill_batch_months", 6))
            
            # Get completed months from stream state
            stream_state = self.get_context_state({}) or {}
            completed_months = stream_state.get("completed_months", [])
            
            # Get all months in range, filter out completed ones
            all_months = self._get_month_range(start, end)
            remaining_months = [m for m in all_months if m not in completed_months]
            
            if not remaining_months:
                # Backfill complete, switch to sliding window
                self.logger.info("Backfill complete, switching to sliding window mode")
                stream_state["backfill_done"] = True
                self.set_context_state({}, stream_state)
                return self._get_sliding_window_months()
            
            # Return batch of remaining months
            batch = remaining_months[:batch_size]
            self.logger.info(f"Processing {len(batch)} months in backfill: {batch}")
            return batch
        
        # Default to sliding window
        return self._get_sliding_window_months()

    def _get_sliding_window_months(self) -> list[str]:
        """Get recent months for sliding window processing."""
        lookback = int(self.config.get("lookback_months", 2))
        months = []
        today = date.today().replace(day=1)
        
        for i in range(lookback + 1):
            if i == 0:
                month_date = today
            else:
                # Go back i months
                month_date = today
                for _ in range(i):
                    if month_date.month == 1:
                        month_date = month_date.replace(year=month_date.year - 1, month=12)
                    else:
                        month_date = month_date.replace(month=month_date.month - 1)
            
            months.append(f"{month_date.year}-{month_date.month:02d}")
        
        return months

    def _get_month_range(self, start_month: str, end_month: str) -> list[str]:
        """Generate list of months between start and end (inclusive)."""
        months = []
        start = datetime.strptime(f"{start_month}-01", "%Y-%m-%d")
        end = datetime.strptime(f"{end_month}-01", "%Y-%m-%d")
        
        current = start
        while current <= end:
            months.append(f"{current.year}-{current.month:02d}")
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        return months

    def _get_last_complete_month(self) -> str:
        """Get the last complete month as YYYY-MM."""
        today = date.today()
        if today.day == 1:
            last_month = today - timedelta(days=1)
        else:
            last_month = today.replace(day=1) - timedelta(days=1)
        return f"{last_month.year}-{last_month.month:02d}"

    def _build_search_query(self, org: str, start_date: str, end_date: str, kind: str) -> str:
        """Build GitHub search query for an organization."""
        base_query = f"org:{org}"
        
        if kind == "bug":
            base_query += ' is:issue is:open label:bug,"[type] bug","type: bug"'
        elif kind == "pr":
            base_query += " type:pr is:merged"
        else:  # issue
            base_query += " type:issue is:open"
        
        base_query += f" created:{start_date}..{end_date}"
        return base_query

    def _build_repo_search_query(self, repo: str, start_date: str, end_date: str, kind: str) -> str:
        """Build GitHub search query for a specific repository."""
        base_query = f"repo:{repo}"
        
        if kind == "bug":
            base_query += ' is:issue is:open label:bug,"[type] bug","type: bug"'
        elif kind == "pr":
            base_query += " type:pr is:merged"
        else:  # issue
            base_query += " type:issue is:open"
        
        base_query += f" created:{start_date}..{end_date}"
        return base_query

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Get search count records for each partition."""
        now = datetime.utcnow().isoformat() + "Z"
        
        partitions_to_process = [context] if context else self.partitions
        
        for partition in partitions_to_process:
            org = partition["org"]
            month = partition["month"]
            query = partition["search_query"]
            api_url_base = partition["api_url_base"]
            repo_breakdown = partition.get("repo_breakdown", False)
            
            # Get search results
            if repo_breakdown:
                # Get per-repo counts
                repo_counts = self._search_with_repo_breakdown(query, api_url_base)
                for repo, count in repo_counts.items():
                    yield {
                        "search_name": f"{org}_{repo}_{partition['kind']}_{month}",
                        "search_query": query,
                        "source": partition["source"],
                        "org": org,
                        "repo": repo,
                        "month": month,
                        self.count_field: count,
                        "updated_at": now,
                    }
            else:
                # Get aggregate count
                total_count = self._search_aggregate_count(query, api_url_base)
                yield {
                    "search_name": partition["search_name"],
                    "search_query": query,
                    "source": partition["source"],
                    "org": org,
                    "repo": "aggregate",
                    "month": month,
                    self.count_field: total_count,
                    "updated_at": now,
                }

    def _search_with_repo_breakdown(self, query: str, api_url_base: str) -> dict[str, int]:
        """Search and return counts broken down by repository."""
        # Check if we need to slice the month due to large result sets
        total_count = self._search_aggregate_count(query, api_url_base)
        
        if total_count <= 1000:
            # Small enough to get all results at once
            return self._get_repo_counts_from_nodes(query, api_url_base)
        else:
            # Auto-slice into smaller date ranges
            return self._search_with_auto_slicing(query, api_url_base)

    def _search_aggregate_count(self, query: str, api_url_base: str) -> int:
        """Get total search count without pagination."""
        payload = {"query": self.query, "variables": {"q": query, "after": None}}
        prepared_request = self.build_prepared_request(
            method="POST",
            url=f"{api_url_base}/graphql", 
            json=payload
        )
        response = self._request(prepared_request, None)
        response_json = response.json()
        return response_json["data"]["search"]["issueCount"]

    def _get_repo_counts_from_nodes(self, query: str, api_url_base: str) -> dict[str, int]:
        """Get repository counts by paginating through search nodes."""
        repo_counts = defaultdict(int)
        after = None
        
        while True:
            payload = {"query": self.query, "variables": {"q": query, "after": after}}
            prepared_request = self.build_prepared_request(
                method="POST",
                url=f"{api_url_base}/graphql", 
                json=payload
            )
            response = self._request(prepared_request, None)
            response_json = response.json()
            search = response_json["data"]["search"]
            
            # Count by repository
            for node in search["nodes"]:
                repo_name = node["repository"]["name"]
                repo_counts[repo_name] += 1
            
            if not search["pageInfo"]["hasNextPage"]:
                break
            after = search["pageInfo"]["endCursor"]
        
        return dict(repo_counts)

    def _search_with_auto_slicing(self, query: str, api_url_base: str) -> dict[str, int]:
        """Auto-slice large queries into weekly chunks to stay under 1000 result limit."""
        # Extract date range from query
        date_match = re.search(r"created:(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})", query)
        if not date_match:
            # Fallback to regular pagination if can't parse dates
            return self._get_repo_counts_from_nodes(query, api_url_base)
        
        start_date = datetime.strptime(date_match.group(1), "%Y-%m-%d")
        end_date = datetime.strptime(date_match.group(2), "%Y-%m-%d")
        
        repo_counts = defaultdict(int)
        current = start_date
        
        # Process in 7-day slices
        while current <= end_date:
            slice_end = min(current + timedelta(days=6), end_date)
            slice_query = re.sub(
                r"created:\d{4}-\d{2}-\d{2}\.\.\d{4}-\d{2}-\d{2}",
                f"created:{current.strftime('%Y-%m-%d')}..{slice_end.strftime('%Y-%m-%d')}",
                query
            )
            
            slice_counts = self._get_repo_counts_from_nodes(slice_query, api_url_base)
            for repo, count in slice_counts.items():
                repo_counts[repo] += count
            
            current = slice_end + timedelta(days=1)
        
        return dict(repo_counts)


class IssueSearchCountStream(SearchCountStreamBase):
    """Stream for GitHub issue search counts."""
    
    name = "issue_search_counts"
    stream_type = "issue"
    count_field = "issue_count"


class PRSearchCountStream(SearchCountStreamBase):
    """Stream for GitHub PR search counts."""
    
    name = "pr_search_counts"
    stream_type = "pr"
    count_field = "pr_count"


class BugSearchCountStream(SearchCountStreamBase):
    """Stream for GitHub bug search counts."""
    
    name = "bug_search_counts"
    stream_type = "bug"
    count_field = "bug_count"

