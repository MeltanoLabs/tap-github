"""Repository discovery utilities for GitHub organizations."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from singer_sdk.exceptions import RetriableAPIError


@dataclass
class GitHubInstance:
    """Represents a GitHub instance configuration."""
    name: str
    api_url_base: str
    auth_token: str


class RepositoryDiscovery:
    """Utility for discovering top repositories from GitHub organizations."""
    
    # GraphQL sort field mappings
    SORT_FIELDS = {
        "issues": "STARGAZERS",  # We'll sort by stars then filter by issue count
        "stars": "STARGAZERS", 
        "forks": "FORKS",
        "updated": "UPDATED_AT"
    }
    
    def __init__(self, graphql_requester):
        """Initialize with a GraphQL requester (stream instance)."""
        self.requester = graphql_requester
        self.logger = logging.getLogger(__name__)
    
    def get_top_repos(
        self,
        org: str,
        limit: int,
        sort_by: str = "issues",
        instance: Optional[GitHubInstance] = None
    ) -> List[str]:
        """Get top N repositories for an organization.
        
        Args:
            org: Organization name
            limit: Number of repositories to return
            sort_by: Sort criteria - "issues", "stars", "forks", or "updated"
            instance: Optional GitHub instance for GitHub Enterprise
            
        Returns:
            List of repository names in "org/repo" format
        """
        if sort_by not in self.SORT_FIELDS:
            raise ValueError(f"Invalid sort_by: {sort_by}. Must be one of {list(self.SORT_FIELDS.keys())}")
        
        sort_field = self.SORT_FIELDS[sort_by]
        
        # Build GraphQL query based on sort type
        if sort_by == "issues":
            query = self._build_issues_query(org, limit)
        else:
            query = self._build_standard_query(org, limit, sort_field)
        
        variables = {"org": org, "limit": limit}
        
        try:
            # Use the appropriate GraphQL request method based on instance
            if instance:
                response = self.requester._make_graphql_request_for_instance(
                    instance, query, variables
                )
            else:
                # For github.com, use the batch request method (it works for single queries too)
                response = self.requester._make_batch_graphql_request(
                    query, variables, {"source": "github.com", "api_url_base": "https://api.github.com"}
                )
            
            if not response or "data" not in response:
                self.logger.warning(f"No data returned for org {org}")
                return []
            
            org_data = response["data"].get("organization")
            if not org_data:
                self.logger.warning(f"Organization {org} not found")
                return []
            
            repos = org_data.get("repositories", {}).get("nodes", [])
            
            if sort_by == "issues":
                # Sort by issue count for issues-based queries
                repos_with_counts = [
                    {"name": repo["nameWithOwner"], "issue_count": repo["issues"]["totalCount"]}
                    for repo in repos
                ]
                repos_with_counts.sort(key=lambda x: x["issue_count"], reverse=True)
                return [repo["name"] for repo in repos_with_counts[:limit]]
            else:
                # Return repository names directly
                return [repo["nameWithOwner"] for repo in repos]
                
        except Exception as e:
            self.logger.error(f"Failed to get top repos for {org}: {e}")
            return []
    
    def _build_issues_query(self, org: str, limit: int) -> str:
        """Build GraphQL query optimized for issue count sorting."""
        return f"""
        query($org: String!, $limit: Int!) {{
          organization(login: $org) {{
            repositories(first: $limit, orderBy: {{field: STARGAZERS, direction: DESC}}) {{
              nodes {{
                nameWithOwner
                issues {{
                  totalCount
                }}
              }}
            }}
          }}
        }}
        """
    
    def _build_standard_query(self, org: str, limit: int, sort_field: str) -> str:
        """Build GraphQL query for standard sorting (stars, forks, updated)."""
        return f"""
        query($org: String!, $limit: Int!) {{
          organization(login: $org) {{
            repositories(first: $limit, orderBy: {{field: {sort_field}, direction: DESC}}) {{
              nodes {{
                nameWithOwner
              }}
            }}
          }}
        }}
        """