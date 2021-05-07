"""REST client handling, including GitHubStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.streams import RESTStream


class GitHubStream(RESTStream):
    """GitHub stream class."""

    url_base = "https://api.github.com"
    MAX_PER_PAGE = 10000

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {"Accept": "application/vnd.github.v3+json"}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        if "auth_token" in self.config:
            headers["auth_token"] = self.config["auth_token"]
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any] = None
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        return None
        # next_page_token = response.headers.get("X-Next-Page", None)
        # if next_page_token:
        #     self.logger.info(f"Next page token retrieved: {next_page_token}")
        # return next_page_token

    def get_url_params(
        self, partition: Optional[dict], next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {"per_page": self.MAX_PER_PAGE, "query": self.query}
        if next_page_token:
            params["page"] = next_page_token
        # if self.replication_key:
        #     params["sort"] = "asc"
        #     params["order_by"] = self.replication_key
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        for row in resp_json.get("items"):
            yield row
