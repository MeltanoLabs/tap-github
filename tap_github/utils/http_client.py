"""HTTP client utilities for GitHub GraphQL requests."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests
from requests.exceptions import ConnectionError, HTTPError, Timeout

from .repository_discovery import GitHubInstance


class GitHubGraphQLClient:
    """Unified GraphQL client for GitHub instances."""
    
    def __init__(self, config: dict, logger: Optional[logging.Logger] = None):
        """Initialize the GraphQL client."""
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
    
    def make_request(
        self,
        query: str,
        variables: Dict[str, Any],
        instance: Optional[GitHubInstance] = None,
        api_url_base: Optional[str] = None,
        auth_token: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Make a GraphQL request to GitHub instance."""
        
        if instance:
            api_url = instance.api_url_base.rstrip('/') + '/graphql'
            token = instance.auth_token
            instance_name = instance.name
        else:
            api_url = (api_url_base or "https://api.github.com").rstrip('/') + '/graphql'
            token = auth_token or self.config.get("auth_token")
            instance_name = "github.com"
        
        if not token:
            self.logger.error(f"No auth token found for instance {instance_name}")
            return None
        
        payload = {"query": query, "variables": variables}
        timeout = self.config.get("stream_request_timeout", 300)
        
        try:
            response = requests.post(
                api_url,
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "Accept": "application/vnd.github.v3+json",
                },
                timeout=timeout,
            )
            response.raise_for_status()
            
            json_resp = response.json()
            
            if "errors" in json_resp:
                self._handle_graphql_errors(json_resp["errors"], instance_name)
            
            return json_resp
            
        except HTTPError as e:
            self._handle_http_error(e, instance_name)
            return None
        except Timeout:
            self.logger.error(f"Request timed out for {instance_name} after {timeout} seconds")
            return None
        except ConnectionError as e:
            self.logger.error(f"Connection error for {instance_name}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error for {instance_name}: {e}")
            return None
    
    def make_batch_request(
        self,
        query: str,
        variables: Dict[str, Any],
        partition: Dict[str, Any],
        instances: List[GitHubInstance]
    ) -> Optional[Dict[str, Any]]:
        """Make a batched GraphQL request using partition configuration."""
        
        api_url_base = partition.get("api_url_base", "https://api.github.com")
        source = partition.get("source", "github.com")
        
        auth_token = None
        for instance in instances:
            if instance.name == source:
                auth_token = instance.auth_token
                break
        
        response = self.make_request(
            query=query,
            variables=variables,
            api_url_base=api_url_base,
            auth_token=auth_token
        )
        
        if response:
            self._log_batch_metrics(response, variables, source)
        
        return response
    
    def _handle_graphql_errors(self, errors: List[Dict[str, Any]], instance_name: str) -> None:
        """Handle GraphQL errors from response."""
        for error in errors:
            self.logger.warning(f"GraphQL error for {instance_name}: {error.get('message', error)}")
    
    def _handle_http_error(self, error: HTTPError, instance_name: str) -> None:
        """Handle HTTP errors with appropriate logging."""
        self.logger.error(f"HTTP error for {instance_name}: {error}")
    
    def _log_batch_metrics(
        self,
        response: Dict[str, Any],
        variables: Dict[str, Any],
        source: str
    ) -> None:
        """Log batch request performance metrics."""
        rate_limit = response.get("data", {}).get("rateLimit", {})
        cost = rate_limit.get("cost", len(variables))
        remaining = rate_limit.get("remaining", "unknown")
        self.logger.info(f"Batch request: {len(variables)} queries, cost: {cost}, remaining: {remaining}")