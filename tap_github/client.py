"""REST client handling, including GitHubStream base class."""

import requests
from typing import Any, Dict, List, Optional, Iterable, cast

from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from tap_github.authenticator import GitHubTokenAuthenticator


class GitHubStream(RESTStream):
    """GitHub stream class."""

    MAX_PER_PAGE = 100  # GitHub's limit is 100.
    MAX_RESULTS_LIMIT: Optional[int] = None
    DEFAULT_API_BASE_URL = "https://api.github.com"
    LOG_REQUEST_METRIC_URLS = True

    _authenticator: Optional[GitHubTokenAuthenticator] = None

    @property
    def authenticator(self) -> GitHubTokenAuthenticator:
        if self._authenticator is None:
            self._authenticator = GitHubTokenAuthenticator(stream=self)
        return self._authenticator

    @property
    def url_base(self) -> str:
        return self.config.get("api_url_base", self.DEFAULT_API_BASE_URL)

    primary_keys = ["id"]
    replication_key: Optional[str] = None
    tolerated_http_errors: List[int] = []

    @property
    def http_headers(self) -> Dict[str, str]:
        """Return the http headers needed."""
        headers = {"Accept": "application/vnd.github.v3+json"}
        if "user_agent" in self.config:
            headers["User-Agent"] = cast(str, self.config.get("user_agent"))

        return headers

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

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        In case an error is tolerated, continue without raising it.

        In case an error is deemed transient and can be safely retried, then this
        method should raise an :class:`singer_sdk.exceptions.RetriableAPIError`.

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        if response.status_code in self.tolerated_http_errors:
            self.logger.info(
                "Request returned a tolerated error for {}".format(response.url)
            )
            self.logger.info(
                f"Reason: {response.status_code} - {str(response.content)}"
            )
            return

        if 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
            )
            # Retry on rate limiting
            if (
                response.status_code == 403
                and "rate limit exceeded" in str(response.content).lower()
            ):
                # Update token
                self.authenticator.get_next_auth_token()
                # Raise an error to force a retry with the new token (this function has a retry decorator).
                raise RetriableAPIError(msg)
            raise FatalAPIError(msg)

        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        # TODO - Split into handle_reponse and parse_response.
        if response.status_code in self.tolerated_http_errors:
            return []

        # Update token rate limit info and loop through tokens if needed.
        self.authenticator.update_rate_limit(response.headers)

        resp_json = response.json()

        if isinstance(resp_json, list):
            results = resp_json
        elif resp_json.get("items") is not None:
            results = resp_json.get("items")
        else:
            results = [resp_json]

        for row in results:
            yield row
