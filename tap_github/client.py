"""REST client handling, including GitHubStream base class."""

import requests
from typing import Any, Dict, List, Optional, Iterable, cast

from singer_sdk.streams import RESTStream
from tap_github.authenticator import GitHubTokenAuthenticator


class GitHubStream(RESTStream):
    """GitHub stream class."""

    MAX_PER_PAGE = 1000
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
    def http_headers(self) -> dict[str, str]:
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

    def _request_with_backoff(
        self, prepared_request, context: Optional[dict]
    ) -> requests.Response:
        """Override private method _request_with_backoff to account for expected 404 Not Found erros."""
        # TODO - Adapt Singer
        response = self.requests_session.send(prepared_request)
        if self._LOG_REQUEST_METRICS:
            extra_tags = {}
            if self._LOG_REQUEST_METRIC_URLS:
                extra_tags["url"] = cast(str, prepared_request.path_url)
            self._write_request_duration_log(
                endpoint=self.path,
                response=response,
                context=context,
                extra_tags=extra_tags,
            )
        if response.status_code in self.tolerated_http_errors:
            self.logger.info(
                "Request returned a tolerated error for {}".format(prepared_request.url)
            )
            self.logger.info(
                f"Reason: {response.status_code} - {str(response.content)}"
            )
            return response

        # Rate limiting
        if response.status_code == 403 and "Rate Limit Exceeded" in str(
            response.content
        ):
            # Update token
            self.authenticator.get_next_auth_token()
            # Raise an error to force a retry with the new token (this function has a retry decorator).
            raise RuntimeError(
                "GitHub rate limit exceeded. Updated active token and retrying."
            )

        if response.status_code in [401, 403]:
            self.logger.info("Failed request for {}".format(prepared_request.url))
            self.logger.info(
                f"Reason: {response.status_code} - {str(response.content)}"
            )
            raise RuntimeError(
                "Requested resource was unauthorized, forbidden, or not found."
            )
        elif response.status_code >= 400:
            raise RuntimeError(
                f"Error making request to API: {prepared_request.url} "
                f"[{response.status_code} - {str(response.content)}]".replace(
                    "\\n", "\n"
                )
            )
        self.logger.debug("Response received successfully.")
        return response

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
