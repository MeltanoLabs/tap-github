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
    replication_method = "INCREMENTAL"
    replication_key = "month"
    state_partitioning_keys: ClassVar[list[str]] = ["source", "org"]  # Removed month for proper incremental

    def __init__(self, tap, name=None, schema=None, path=None):
        """Initialize stream with schema."""
        # Store tap reference for state access
        self.tap = tap
        
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
        """Generate filtered partitions using incremental state and last complete month logic."""
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
        elif self.stream_type == "bug":
            stream_config = search_scope.get("issue_streams", {})
        elif self.stream_type == "pr":
            stream_config = search_scope.get("pr_streams", {})
        else:
            # Fallback for other types
            stream_config = search_scope.get("issue_streams", {})
        
        # Get instances from stream config
        stream_instances = stream_config.get("instances", [])
        
        # Collect unique org contexts for state lookup
        org_contexts = set()
        for instance_config in stream_instances:
            instance_name = instance_config.get("instance", "github.com")
            for org in instance_config.get("org", []):
                org_contexts.add((instance_name, org))
            for repo in instance_config.get("repo_level", []):
                org = repo.split("/")[0]
                org_contexts.add((instance_name, org))
        
        # Pre-calculate bookmark dates per org context
        org_bookmarks = {}
        for instance_name, org in org_contexts:
            context = {"source": instance_name, "org": org}
            bookmark_str = self._get_bookmark_for_context(context)
            
            if bookmark_str:
                bookmark_date = self._month_to_date(bookmark_str)
                org_bookmarks[instance_name, org] = bookmark_date
                self.logger.debug(f"Org {org} bookmark={bookmark_str}")
            else:
                org_bookmarks[instance_name, org] = None
                self.logger.debug(f"Org {org} has no bookmark, will process all months")
        
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
                for org in instance_config.get("org", []):
                    # Check if this month should be included
                    bookmark_date = org_bookmarks.get((instance_name, org))
                    if self._should_include_month(month, bookmark_date):
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
                    else:
                        self.logger.debug(f"Filtered out partition org={org} month={month}")
                
                # Process repo-level searches
                for repo in instance_config.get("repo_level", []):
                    org = repo.split("/")[0]
                    
                    # Check if this month should be included  
                    bookmark_date = org_bookmarks.get((instance_name, org))
                    if self._should_include_month(month, bookmark_date):
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
                    else:
                        self.logger.debug(f"Filtered out partition repo={repo} month={month}")

        self.logger.info(f"Generated {len(partitions)} partitions after incremental filtering")
        return partitions

    def _get_months_to_process(self) -> list[str]:
        """Get months to process based on backfill configuration."""
        cfg = self.config
        
        # Check for backfill configuration
        if cfg.get("backfill_start_month"):
            start = cfg["backfill_start_month"]
            end = cfg.get("backfill_end_month") or self._get_last_complete_month()
            
            self.logger.info(f"Backfill config - start: {start}, end: {end}")
            
            # Get all months in range - incremental filtering handled at partition level
            all_months = self._get_month_range(start, end)
            
            self.logger.info(f"Processing {len(all_months)} months in backfill: {all_months}")
            return all_months
        
        # No backfill config, return empty list
        self.logger.info("No backfill configuration found, skipping processing")
        return []



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
        """Get the last complete month as YYYY-MM string (always exclude current month)."""
        today = date.today()
        if today.month == 1:
            return f"{today.year - 1}-12"
        else:
            return f"{today.year}-{today.month - 1:02d}"
    
    def _get_last_complete_month_date(self) -> date:
        """Get the last complete month as a date object (always exclude current month)."""
        today = date.today()
        if today.month == 1:
            return date(today.year - 1, 12, 1)
        else:
            return date(today.year, today.month - 1, 1)
    
    def _month_to_date(self, month_str: str) -> date:
        """Convert YYYY-MM string to date object (first day of month)."""
        year, month = map(int, month_str.split("-"))
        return date(year, month, 1)
    
    def _get_bookmark_for_context(self, context: dict) -> str | None:
        """Get bookmark for a specific context from tap state."""
        try:
            stream_state = self.tap.state.get("bookmarks", {}).get(self.name, {})
            partitions_state = stream_state.get("partitions", [])
            
            for partition_state in partitions_state:
                state_context = partition_state.get("context", {})
                if (state_context.get("source") == context["source"] and 
                    state_context.get("org") == context["org"]):
                    return partition_state.get("replication_key_value")
            
            return None
        except Exception:
            return None
    
    def _should_include_month(self, month_str: str, bookmark_date: date | None) -> bool:
        """Simple logic: include months after bookmark up to last complete month."""
        month_date = self._month_to_date(month_str)
        last_complete = self._get_last_complete_month_date()
        
        # Never process future or current months
        if month_date > last_complete:
            return False
        
        # If no bookmark, include everything up to last complete
        if bookmark_date is None:
            return True
        
        # Only include months after bookmark
        return month_date > bookmark_date
    
    

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
            
            self.logger.info(f"Processing partition: {org}/{month}")
            
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


class ConfigurableSearchCountStream(SearchCountStreamBase):
    
    def __init__(self, stream_config: dict, tap):
        self.stream_config = stream_config
        self.query_template = stream_config["query_template"]
        self.count_field_name = stream_config["count_field"]
        self.stream_description = stream_config.get("description", f"Search stream: {stream_config['name']}")
        
        self.name = f"{stream_config['name']}_search_counts"
        self.stream_type = stream_config.get("stream_type", "custom")
        self.count_field = self.count_field_name
        
        # Store tap reference for state access
        self.tap = tap
        
        super().__init__(
            tap=tap,
            name=self.name,
            schema=self.get_configurable_schema(),
        )
    
    def get_configurable_schema(self) -> dict:
        return th.PropertiesList(
            th.Property("search_name", th.StringType, required=True),
            th.Property("search_query", th.StringType, required=True),
            th.Property("source", th.StringType, required=True),
            th.Property("org", th.StringType, required=True),
            th.Property("repo", th.StringType, required=True),
            th.Property("month", th.StringType, required=True),
            th.Property(self.count_field_name, th.IntegerType, required=True),
            th.Property("updated_at", th.DateTimeType),
        ).to_dict()
    
    def _build_search_query(self, org: str, start_date: str, end_date: str, stream_type: str) -> str:
        return self.query_template.format(
            org=org,
            start=start_date,
            end=end_date
        )

    @property
    def partitions(self) -> list[Context]:
        search_scope = self.config.get("search_scope", {})
        instances = search_scope.get("instances", [])
        
        if not instances:
            self.logger.warning(f"No instances found in search_scope for {self.name}")
            return []

        partitions = []
        months = self._get_months_to_process()
        stream_name = self.stream_config['name']
        
        # Collect unique org contexts for state lookup
        org_contexts = set()
        for instance_config in instances:
            instance_name = instance_config.get("instance", "github.com")
            for org in instance_config.get("org", []):
                org_contexts.add((instance_name, org))
        
        # Pre-calculate bookmark dates per org context
        org_bookmarks = {}
        for instance_name, org in org_contexts:
            context = {"source": instance_name, "org": org}
            bookmark_str = self._get_bookmark_for_context(context)
            
            if bookmark_str:
                bookmark_date = self._month_to_date(bookmark_str)
                org_bookmarks[instance_name, org] = bookmark_date
                self.logger.debug(f"Org {org} bookmark={bookmark_str}")
            else:
                org_bookmarks[instance_name, org] = None
                self.logger.debug(f"Org {org} has no bookmark, will process all months")
        
        for month in months:
            year, month_num = map(int, month.split("-"))
            start_date = f"{year:04d}-{month_num:02d}-01"
            last_day = calendar.monthrange(year, month_num)[1]
            end_date = f"{year:04d}-{month_num:02d}-{last_day:02d}"

            for instance_config in instances:
                supported_streams = instance_config.get("streams", [])
                if supported_streams and stream_name not in supported_streams:
                    continue
                    
                instance_name = instance_config.get("instance", "github.com")
                api_url_base = instance_config.get("api_url_base", "https://api.github.com")
                repo_breakdown = instance_config.get("repo_breakdown", False)
                
                for org in instance_config.get("org", []):
                    # Check if this month should be included
                    bookmark_date = org_bookmarks.get((instance_name, org))
                    if self._should_include_month(month, bookmark_date):
                        query = self._build_search_query(org, start_date, end_date, self.stream_type)
                        partitions.append({
                            "search_name": f"{org}_{stream_name}_{month}",
                            "search_query": query,
                            "source": instance_name,
                            "api_url_base": api_url_base,
                            "org": org,
                            "month": month,
                            "kind": self.stream_type,
                            "repo_breakdown": repo_breakdown
                        })
                    else:
                        self.logger.debug(f"Filtered out partition org={org} month={month}")

        self.logger.info(f"Generated {len(partitions)} partitions after incremental filtering")
        return partitions


def validate_stream_config(stream_config: dict) -> list[str]:
    errors = []
    
    required_fields = ["name", "query_template", "count_field"]
    for field in required_fields:
        if not stream_config.get(field):
            errors.append(f"Missing required field: {field}")
    
    name = stream_config.get("name", "")
    if name and not name.replace("_", "").replace("-", "").isalnum():
        errors.append(f"Stream name '{name}' must contain only alphanumeric characters, underscores, and hyphens")
    
    query_template = stream_config.get("query_template", "")
    required_placeholders = ["{org}", "{start}", "{end}"]
    for placeholder in required_placeholders:
        if placeholder not in query_template:
            errors.append(f"Query template must contain {placeholder} placeholder")
    
    count_field = stream_config.get("count_field", "")
    if count_field and not count_field.replace("_", "").isalnum():
        errors.append(f"Count field '{count_field}' must contain only alphanumeric characters and underscores")
    
    return errors


def create_configurable_streams(tap) -> list:
    streams = []
    
    # Handle search_streams configuration (new format)
    stream_definitions = tap.config.get("search_streams", [])
    
    if stream_definitions:
        for stream_config in stream_definitions:
            errors = validate_stream_config(stream_config)
            if errors:
                tap.logger.warning(f"Invalid stream config '{stream_config.get('name', 'unknown')}': {'; '.join(errors)}")
                continue
            
            try:
                stream = ConfigurableSearchCountStream(stream_config, tap)
                streams.append(stream)
                tap.logger.info(f"Created search stream: {stream.name}")
            except Exception as e:
                tap.logger.warning(f"Failed to create stream '{stream_config.get('name', 'unknown')}': {e}")
    
    if not streams:
        tap.logger.warning("No search streams created from configuration")
    
    return streams

