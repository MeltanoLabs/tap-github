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
                for org in instance_config.get("org", []):
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
        """Get months to process based on backfill configuration."""
        cfg = self.config
        
        # Check for backfill configuration
        if cfg.get("backfill_start_month"):
            start = cfg["backfill_start_month"]
            end = cfg.get("backfill_end_month") or self._get_last_complete_month()
            
            self.logger.info(f"Backfill config - start: {start}, end: {end}")
            
            # Get all months in range
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


class ConfigurableSearchCountStream(SearchCountStreamBase):
    
    # Singer SDK state partitioning - ensures proper bookmark tracking per partition
    state_partitioning_keys = ["source", "org", "month"]
    
    def __init__(self, stream_config: dict, tap):
        self.stream_config = stream_config
        self.query_template = stream_config["query_template"]
        self.count_field_name = stream_config["count_field"]
        self.stream_description = stream_config.get("description", f"Search stream: {stream_config['name']}")
        
        self.name = f"{stream_config['name']}_search_counts"
        self.stream_type = stream_config.get("stream_type", "custom")
        self.count_field = self.count_field_name
        
        # Batching infrastructure
        self._ordered_partitions = None
        self._partition_to_index = {}
        self._batch_cache = {}
        
        super().__init__(
            tap=tap,
            name=self.name,
            schema=self.get_configurable_schema(),
        )
    
    @property
    def batch_size(self) -> int:
        """Get batch size from config, default to 20."""
        return self.config.get("batch_query_size", 20)
    
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

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Batch-aware record retrieval for efficient GraphQL processing."""
        # If no context provided, fallback to parent implementation
        if not context:
            yield from super().get_records(context)
            return
        
        # Check if batching is enabled
        if self.batch_size <= 1:
            yield from self._get_individual_records(context)
            return
        
        # Batch-aware processing for both aggregate and repo breakdown
        batch_id = self._get_batch_id(context)
        
        # If this batch hasn't been fetched, do batched GraphQL query
        if batch_id not in self._batch_cache:
            self._fetch_batch(batch_id)
        
        # Yield only records for this specific partition
        partition_key = self._get_partition_key(context)
        yield from self._batch_cache[batch_id].get(partition_key, [])
        
        # Memory cleanup: remove cache when we've processed the last partition in batch
        if self._is_last_partition_in_batch(context, batch_id):
            del self._batch_cache[batch_id]
    
    def _ensure_partition_index(self):
        """Build index of all partitions for batching logic."""
        if self._ordered_partitions is not None:
            return
        
        # Get all partitions and build lookup index
        all_partitions = list(self.partitions or [])
        self._ordered_partitions = all_partitions
        
        # Create stable partition key to index mapping
        for i, partition in enumerate(all_partitions):
            partition_key = self._get_partition_key(partition)
            self._partition_to_index[partition_key] = i
    
    def _get_batch_id(self, context: dict) -> int:
        """Determine which batch this partition belongs to."""
        self._ensure_partition_index()
        partition_key = self._get_partition_key(context)
        partition_index = self._partition_to_index.get(partition_key, 0)
        return partition_index // self.batch_size
    
    def _get_partition_key(self, context: dict) -> str:
        """Create stable key for partition identification."""
        # Use state partitioning keys for consistency
        key_parts = []
        for key in self.state_partitioning_keys:
            if key in context:
                key_parts.append(f"{key}={context[key]}")
        return "|".join(key_parts)
    
    def _is_last_partition_in_batch(self, context: dict, batch_id: int) -> bool:
        """Check if this is the last partition in the batch for cache cleanup."""
        self._ensure_partition_index()
        partition_key = self._get_partition_key(context)
        partition_index = self._partition_to_index.get(partition_key, 0)
        
        batch_start = batch_id * self.batch_size
        batch_end = min(batch_start + self.batch_size, len(self._ordered_partitions))
        
        return partition_index == (batch_end - 1)

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

        return partitions

    def _fetch_batch(self, batch_id: int):
        """Execute batched GraphQL queries for entire batch of partitions."""
        self._ensure_partition_index()
        
        # Get all partitions in this batch
        batch_start = batch_id * self.batch_size
        batch_end = min(batch_start + self.batch_size, len(self._ordered_partitions))
        batch_partitions = self._ordered_partitions[batch_start:batch_end]
        
        # Group by API URL and breakdown type for proper batching
        api_groups = {}
        for partition in batch_partitions:
            api_url = partition["api_url_base"]
            breakdown_type = "repo" if partition.get("repo_breakdown", False) else "aggregate"
            key = (api_url, breakdown_type)
            
            if key not in api_groups:
                api_groups[key] = []
            api_groups[key].append(partition)
        
        # Initialize batch cache for this batch
        self._batch_cache[batch_id] = {}
        
        # Process each API group
        for (api_url_base, breakdown_type), group_partitions in api_groups.items():
            self.logger.info(f"Processing {breakdown_type} batch {batch_id} with {len(group_partitions)} partitions for {api_url_base}")
            
            try:
                if breakdown_type == "repo":
                    # Use repo breakdown batching
                    self._fetch_repo_breakdown_batch(group_partitions, api_url_base, batch_id)
                else:
                    # Use aggregate count batching
                    self._fetch_aggregate_batch(group_partitions, api_url_base, batch_id)
                    
            except Exception as e:
                self.logger.warning(f"Batch request failed: {e}, falling back to individual processing")
                # Fallback to individual processing for this group
                for partition in group_partitions:
                    partition_key = self._get_partition_key(partition)
                    try:
                        records = list(self._get_individual_records(partition))
                        self._batch_cache[batch_id][partition_key] = records
                    except Exception as fallback_error:
                        self.logger.error(f"Individual fallback failed for partition {partition_key}: {fallback_error}")
                        self._batch_cache[batch_id][partition_key] = []
    
    def _fetch_aggregate_batch(self, partitions: list[dict], api_url_base: str, batch_id: int):
        """Fetch aggregate counts for multiple partitions in a single GraphQL query."""
        # Build batched GraphQL query with aliases
        query, variables = self._build_aggregate_batch_query(partitions)
        
        # Execute single API call
        response = self._execute_graphql_query(query, variables, api_url_base)
        
        # Parse and cache results per partition
        self._cache_aggregate_batch_results(partitions, response, batch_id)
    
    def _build_aggregate_batch_query(self, partitions: list[dict]) -> tuple[str, dict]:
        """Build GraphQL query with aliases for batch processing."""
        variables = {}
        field_fragments = []
        
        for i, partition in enumerate(partitions):
            var_name = f"q{i}"
            variables[var_name] = partition["search_query"]
            
            # Create aliased GraphQL field
            field_fragments.append(f"""
                {var_name}: search(query: ${var_name}, type: ISSUE, first: 1) {{
                    issueCount
                }}
            """.strip())
        
        # Build complete query with variable declarations
        var_declarations = ", ".join(f"${var}: String!" for var in variables.keys())
        query = f"""
            query({var_declarations}) {{
                {chr(10).join(field_fragments)}
                rateLimit {{ cost remaining }}
            }}
        """
        
        return query, variables
    
    def _execute_graphql_query(self, query: str, variables: dict, api_url_base: str) -> dict:
        """Execute GraphQL query using existing stream infrastructure."""
        payload = {"query": query, "variables": variables}
        
        # Use existing request building infrastructure
        prepared_request = self.build_prepared_request(
            method="POST",
            url=f"{api_url_base}/graphql",
            json=payload
        )
        
        # Execute request using existing HTTP client
        response = self._request(prepared_request, None)
        return response.json()
    
    def _cache_aggregate_batch_results(self, partitions: list[dict], response: dict, batch_id: int):
        """Parse GraphQL response and cache results per partition."""
        data = response.get("data", {})
        now = datetime.utcnow().isoformat() + "Z"
        
        for i, partition in enumerate(partitions):
            alias = f"q{i}"
            partition_key = self._get_partition_key(partition)
            
            if alias in data and "issueCount" in data[alias]:
                count = data[alias]["issueCount"]
                
                # Create record in same format as individual processing
                record = {
                    "search_name": partition["search_name"],
                    "search_query": partition["search_query"],
                    "source": partition["source"],
                    "org": partition["org"],
                    "repo": "aggregate",
                    "month": partition["month"],
                    self.count_field: count,
                    "updated_at": now,
                }
                
                self._batch_cache[batch_id][partition_key] = [record]
            else:
                # Handle missing data gracefully
                self.logger.warning(f"No data found for partition {partition_key} in batch response")
                self._batch_cache[batch_id][partition_key] = []
    
    def _fetch_repo_breakdown_batch(self, partitions: list[dict], api_url_base: str, batch_id: int):
        """Fetch repo breakdown counts for multiple partitions using two-phase batching."""
        
        # Phase 1: Batch count checks to determine processing strategy
        count_query, count_variables = self._build_count_batch_query(partitions)
        count_response = self._execute_graphql_query(count_query, count_variables, api_url_base)
        
        # Separate partitions by processing needs
        light_partitions = []  # Can fetch in single batch (≤1000 results)
        heavy_partitions = []  # Need individual processing (>1000 results)
        
        count_data = count_response.get("data", {})
        for i, partition in enumerate(partitions):
            alias = f"count{i}"
            if alias in count_data and "issueCount" in count_data[alias]:
                count = count_data[alias]["issueCount"]
                if count <= 1000:
                    light_partitions.append(partition)
                else:
                    heavy_partitions.append(partition)
            else:
                # If count check failed, process individually
                heavy_partitions.append(partition)
        
        # Phase 2: Batch fetch for light partitions
        if light_partitions:
            self.logger.info(f"Batch processing {len(light_partitions)} light repo breakdown partitions")
            self._fetch_light_repo_breakdown_batch(light_partitions, api_url_base, batch_id)
        
        # Phase 3: Individual processing for heavy partitions  
        if heavy_partitions:
            self.logger.info(f"Individual processing {len(heavy_partitions)} heavy repo breakdown partitions")
            for partition in heavy_partitions:
                partition_key = self._get_partition_key(partition)
                try:
                    records = list(self._get_individual_records(partition))
                    self._batch_cache[batch_id][partition_key] = records
                except Exception as e:
                    self.logger.error(f"Individual processing failed for {partition_key}: {e}")
                    self._batch_cache[batch_id][partition_key] = []
    
    def _build_count_batch_query(self, partitions: list[dict]) -> tuple[str, dict]:
        """Build GraphQL query to check counts for multiple partitions."""
        variables = {}
        field_fragments = []
        
        for i, partition in enumerate(partitions):
            var_name = f"count{i}"
            variables[var_name] = partition["search_query"]
            
            field_fragments.append(f"""
                {var_name}: search(query: ${var_name}, type: ISSUE, first: 1) {{
                    issueCount
                }}
            """.strip())
        
        var_declarations = ", ".join(f"${var}: String!" for var in variables.keys())
        query = f"""
            query({var_declarations}) {{
                {chr(10).join(field_fragments)}
                rateLimit {{ cost remaining }}
            }}
        """
        
        return query, variables
    
    def _fetch_light_repo_breakdown_batch(self, partitions: list[dict], api_url_base: str, batch_id: int):
        """Fetch repo breakdown for partitions with manageable result counts."""
        # Build batched repo breakdown query
        repo_query, repo_variables = self._build_repo_breakdown_batch_query(partitions)
        repo_response = self._execute_graphql_query(repo_query, repo_variables, api_url_base)
        
        # Parse and cache repo breakdown results
        self._cache_repo_breakdown_batch_results(partitions, repo_response, batch_id)
    
    def _build_repo_breakdown_batch_query(self, partitions: list[dict]) -> tuple[str, dict]:
        """Build GraphQL query to fetch repo breakdown for multiple partitions."""
        variables = {}
        field_fragments = []
        
        for i, partition in enumerate(partitions):
            var_name = f"repo{i}"
            variables[var_name] = partition["search_query"]
            
            field_fragments.append(f"""
                {var_name}: search(query: ${var_name}, type: ISSUE, first: 100) {{
                    issueCount
                    pageInfo {{ hasNextPage endCursor }}
                    nodes {{
                        ... on Issue {{ repository {{ name }} }}
                        ... on PullRequest {{ repository {{ name }} }}
                    }}
                }}
            """.strip())
        
        var_declarations = ", ".join(f"${var}: String!" for var in variables.keys())
        query = f"""
            query({var_declarations}) {{
                {chr(10).join(field_fragments)}
                rateLimit {{ cost remaining }}
            }}
        """
        
        return query, variables
    
    def _cache_repo_breakdown_batch_results(self, partitions: list[dict], response: dict, batch_id: int):
        """Parse batched repo breakdown response and cache results per partition."""
        data = response.get("data", {})
        now = datetime.utcnow().isoformat() + "Z"
        
        for i, partition in enumerate(partitions):
            alias = f"repo{i}"
            partition_key = self._get_partition_key(partition)
            
            if alias not in data:
                self.logger.warning(f"No data found for partition {partition_key} in batch response")
                self._batch_cache[batch_id][partition_key] = []
                continue
                
            search_data = data[alias]
            nodes = search_data.get("nodes", [])
            
            # Count by repository
            repo_counts = defaultdict(int)
            for node in nodes:
                if "repository" in node and "name" in node["repository"]:
                    repo_name = node["repository"]["name"]
                    repo_counts[repo_name] += 1
            
            # Create records for each repository
            records = []
            for repo, count in repo_counts.items():
                record = {
                    "search_name": f"{partition['org']}_{repo}_{partition.get('kind', 'search')}_{partition['month']}",
                    "search_query": partition["search_query"],
                    "source": partition["source"],
                    "org": partition["org"],
                    "repo": repo,
                    "month": partition["month"],
                    self.count_field: count,
                    "updated_at": now,
                }
                records.append(record)
            
            # Handle pagination if needed
            page_info = search_data.get("pageInfo", {})
            if page_info.get("hasNextPage", False):
                self.logger.info(f"Partition {partition_key} needs pagination, processing individually")
                # Fall back to individual processing for this partition
                try:
                    individual_records = list(self._get_individual_records(partition))
                    self._batch_cache[batch_id][partition_key] = individual_records
                except Exception as e:
                    self.logger.error(f"Individual processing failed for {partition_key}: {e}")
                    self._batch_cache[batch_id][partition_key] = records  # Use partial results
            else:
                self._batch_cache[batch_id][partition_key] = records
    
    def _get_individual_records(self, context: dict) -> Iterable[dict[str, Any]]:
        """Fallback to individual partition processing."""
        # Use parent class implementation for individual processing
        yield from super().get_records(context)


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

