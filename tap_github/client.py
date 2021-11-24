"""REST client handling, including GitHubStream base class."""

from datetime import datetime
from os import environ
from typing import Any, Callable, Dict, Iterable, List, Optional, cast

import requests
from ratelimit import RateLimitException, limits
from ratelimit.decorators import sleep_and_retry
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.streams import RESTStream

limiter = limits(calls=1000, period=3600)


class GitHubStream(RESTStream):
    """GitHub stream class."""

    MAX_PER_PAGE = 1000
    MAX_RESULTS_LIMIT: Optional[int] = None
    DEFAULT_API_BASE_URL = "https://api.github.com"
    LOG_REQUEST_METRIC_URLS = True

    @property
    def url_base(self) -> str:
        return self.config.get("api_url_base", self.DEFAULT_API_BASE_URL)

    primary_keys = ["id"]
    replication_key: Optional[str] = None
    tolerated_http_errors: List[int] = []

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {"Accept": "application/vnd.github.v3+json"}
        if "user_agent" in self.config:
            headers["User-Agent"] = cast(str, self.config.get("user_agent"))

        if "auth_token" in self.config:
            headers["Authorization"] = f"token {self.config['auth_token']}"
        elif "GITHUB_TOKEN" in environ:
            self.logger.info(
                "Found 'GITHUB_TOKEN' environment variable for authentication."
            )
            headers["Authorization"] = f"token {environ['GITHUB_TOKEN']}"
        else:
            self.logger.info(
                "No auth token detected. "
                "For higher rate limits, please specify `auth_token` in config."
            )

        return headers

    def request_decorator(self, func: Callable) -> Callable:
        """Decorate the request method of the stream.
        Args:
            func: The RESTStream._request method.
        Returns:
            Decorated method.
        """
        return sleep_and_retry(limiter(func))

    def validate_response(self, response: requests.Response) -> None:
        """Validate the HTTP response."""
        remaining = int(response.headers["X-RateLimit-Remaining"])
        reset = int(response.headers["X-RateLimit-Reset"])
        self.logger.info("Remaining requests %d", remaining)

        # raise FatalAPIError("woops")

        if remaining == 0:
            backoff = reset - datetime.utcnow().timestamp()
            self.logger.info("Backing off for %s seconds", backoff)
            raise RateLimitException("Backoff triggered", backoff)

        if response.status_code not in self.tolerated_http_errors:
            super().validate_response(response)

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if (
            previous_token
            and self.MAX_RESULTS_LIMIT
            and (
                cast(int, previous_token) * self.MAX_PER_PAGE >= self.MAX_RESULTS_LIMIT
            )
        ):
            return None

        resp_json = response.json()
        if isinstance(resp_json, list):
            results = resp_json
        else:
            results = resp_json.get("items")

        if results:
            # Paginate as long as the response has items
            return (previous_token or 1) + 1

        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {"per_page": self.MAX_PER_PAGE}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "updated"
            params["direction"] = "asc"
            since = self.get_starting_timestamp(context)
            if since:
                params["since"] = since
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        # TODO - Split into handle_reponse and parse_response.
        if response.status_code in self.tolerated_http_errors:
            return []

        resp_json = response.json()

        if isinstance(resp_json, list):
            results = resp_json
        elif resp_json.get("items") is not None:
            results = resp_json.get("items")
        else:
            results = [resp_json]

        for row in results:
            yield row
