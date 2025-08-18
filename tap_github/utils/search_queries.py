"""Search query generation utilities for GitHub search API."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple


@dataclass
class SearchQuery:
    """Represents a search query configuration."""
    name: str
    query: str
    type: str = "issue"
    month: str | None = None


class SearchQueryGenerator:
    """Generates GitHub search queries for different scopes and time periods."""
    
    # Unified query templates - single source of truth
    QUERY_TEMPLATES = {
        "issue": "{scope}:{target} type:issue state:open created:{start}..{end}",
        "bug": "{scope}:{target} type:issue state:open label:bug,defect,\"[type] bug\",\"type: bug\" created:{start}..{end}",
        "pr": "{scope}:{target} type:pr is:merged merged:{start}..{end}"
    }
    
    def build_search_query(
        self,
        query_type: str,
        scope: str,  # "org" or "repo"
        target: str,  # organization name or "owner/repo"
        start_date: str,
        end_date: str
    ) -> str:
        """Build a GitHub search query string.
        
        Args:
            query_type: Type of query - "issue", "bug", or "pr"
            scope: Search scope - "org" or "repo"  
            target: Target organization or repository
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Formatted GitHub search query string
        """
        if query_type not in self.QUERY_TEMPLATES:
            raise ValueError(f"Invalid query_type: {query_type}. Must be one of {list(self.QUERY_TEMPLATES.keys())}")
        
        if scope not in ["org", "repo"]:
            raise ValueError(f"Invalid scope: {scope}. Must be 'org' or 'repo'")
        
        template = self.QUERY_TEMPLATES[query_type]
        return template.format(
            scope=scope,
            target=target,
            start=start_date,
            end=end_date
        )
    
    def generate_monthly_queries(
        self,
        organizations: List[str],
        start_date: str,
        end_date: str,
        query_types: List[str] = None
    ) -> List[SearchQuery]:
        """Generate monthly search queries for organizations.
        
        Args:
            organizations: List of organization names
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format  
            query_types: List of query types to generate (default: ["issue", "bug", "pr"])
            
        Returns:
            List of SearchQuery objects
        """
        if query_types is None:
            query_types = ["issue", "bug", "pr"]
        
        queries = []
        date_ranges = self._generate_monthly_date_ranges(start_date, end_date)
        
        for org in organizations:
            for start, end in date_ranges:
                month_str = start.strftime("%Y-%m")
                
                for query_type in query_types:
                    query_string = self.build_search_query(
                        query_type=query_type,
                        scope="org",
                        target=org,
                        start_date=start.strftime("%Y-%m-%d"),
                        end_date=end.strftime("%Y-%m-%d")
                    )
                    
                    query_name = f"{org}_{query_type}_{month_str}"
                    
                    queries.append(SearchQuery(
                        name=query_name,
                        query=query_string,
                        type=query_type,
                        month=month_str
                    ))
        
        return queries
    
    def generate_repo_queries(
        self,
        repositories: List[str],
        start_date: str,
        end_date: str,
        query_types: List[str] = None
    ) -> List[SearchQuery]:
        """Generate search queries for specific repositories.
        
        Args:
            repositories: List of repository names in "owner/repo" format
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            query_types: List of query types to generate (default: ["issue", "bug", "pr"])
            
        Returns:
            List of SearchQuery objects
        """
        if query_types is None:
            query_types = ["issue", "bug", "pr"]
        
        queries = []
        date_ranges = self._generate_monthly_date_ranges(start_date, end_date)
        
        for repo in repositories:
            for start, end in date_ranges:
                month_str = start.strftime("%Y-%m")
                
                for query_type in query_types:
                    query_string = self.build_search_query(
                        query_type=query_type,
                        scope="repo",
                        target=repo,
                        start_date=start.strftime("%Y-%m-%d"),
                        end_date=end.strftime("%Y-%m-%d")
                    )
                    
                    # Clean repo name for query name
                    repo_clean = repo.replace("/", "_")
                    query_name = f"{repo_clean}_{query_type}_{month_str}"
                    
                    queries.append(SearchQuery(
                        name=query_name,
                        query=query_string,
                        type=query_type,
                        month=month_str
                    ))
        
        return queries
    
    def _generate_monthly_date_ranges(self, start_date: str, end_date: str) -> List[Tuple[datetime, datetime]]:
        """Generate monthly date ranges between start and end dates.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of (start_datetime, end_datetime) tuples for each month
        """
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        ranges = []
        current = start_dt.replace(day=1)  # Start from first day of start month
        
        while current <= end_dt:
            # Get last day of current month
            last_day = calendar.monthrange(current.year, current.month)[1]
            month_end = current.replace(day=last_day)
            
            # Don't go beyond the end date
            if month_end > end_dt:
                month_end = end_dt
            
            ranges.append((current, month_end))
            
            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        return ranges