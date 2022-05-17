"""REST client handling, including GitHubStream base class."""

from typing import Any, Dict, Iterable, List, Optional, cast

import requests
import simplejson

from dateutil.parser import parse
from urllib.parse import parse_qs, urlparse

from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import GraphQLStream, RESTStream

from tap_github.authenticator import GitHubTokenAuthenticator


class GitHubRestStream(RESTStream):
    """GitHub Rest stream class."""

    MAX_PER_PAGE = 100  # GitHub's limit is 100.
    MAX_RESULTS_LIMIT: Optional[int] = None
    DEFAULT_API_BASE_URL = "https://api.github.com"
    LOG_REQUEST_METRIC_URLS = True

    # GitHub is missing the "since" parameter on a few endpoints
    # set this parameter to True if your stream needs to navigate data in descending order
    # and try to exit early on its own.
    # This only has effect on streams whose `replication_key` is `updated_at`.
    missing_since_parameter = False

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

        # Leverage header links returned by the GitHub API.
        if "next" not in response.links.keys():
            return None

        resp_json = response.json()
        if isinstance(resp_json, list):
            results = resp_json
        else:
            results = resp_json.get("items")

        # Exit early if the response has no items. ? Maybe duplicative of the "next" link check.
        if not results:
            return None

        # Unfortunately endpoints such as /starred, /events and /pulls do not support
        # the "since" parameter out of the box. So we use a workaround here to exit early.
        # For such streams, we sort by descending dates (most recent first), and paginate
        # "back in time" until we reach records before our "since" parameter.
        request_parameters = parse_qs(str(urlparse(response.request.url).query))
        # parse_qs interprets "+" as a space, revert this to keep an aware datetime
        try:
            since = (
                request_parameters["since"][0].replace(" ", "+")
                if "since" in request_parameters
                else ""
            )
        except IndexError:
            since = ""
        direction = (
            request_parameters["direction"][0]
            if "direction" in request_parameters
            else None
        )
        if (
            # commit_timestamp is a constructed key which does not exist in the raw response
            self.replication_key != "commit_timestamp"
            and since
            and direction == "desc"
            and (parse(results[-1][self.replication_key]) < parse(since))
        ):
            return None

        # Use header links returned by the GitHub API.
        parsed_url = urlparse(response.links["next"]["url"])
        captured_page_value_list = parse_qs(parsed_url.query).get("page")
        next_page_string = (
            captured_page_value_list[0] if captured_page_value_list else None
        )
        if next_page_string and next_page_string.isdigit():
            return int(next_page_string)

        return (previous_token or 1) + 1

    def get_url_params(
        self, context: Optional[Dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {"per_page": self.MAX_PER_PAGE}
        if next_page_token:
            params["page"] = next_page_token

        if self.replication_key == "updated_at":
            params["sort"] = "updated"
            params["direction"] = "desc" if self.missing_since_parameter else "asc"

        # Unfortunately the /starred (starred_at) and /events (created_at) endpoints do not support
        # the "since" parameter out of the box. But we use a workaround in 'get_next_page_token'.
        elif self.replication_key in ["starred_at", "created_at"]:
            params["sort"] = "created"
            params["direction"] = "desc"

        # Warning: /commits endpoint accept "since" but results are ordered by descending commit_timestamp
        elif self.replication_key == "commit_timestamp":
            params["direction"] = "desc"

        elif self.replication_key:
            self.logger.warning(
                f"The replication key '{self.replication_key}' is not fully supported by this client yet."
            )

        since = self.get_starting_timestamp(context)
        if self.replication_key and since:
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
        full_path = urlparse(response.url).path
        if response.status_code in self.tolerated_http_errors:
            msg = (
                f"{response.status_code} Tolerated Status Code "
                f"(Reason: {response.reason}) for path: {full_path}"
            )
            self.logger.info(msg)
            return

        if 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{str(response.content)} (Reason: {response.reason}) for path: {full_path}"
            )
            # Retry on rate limiting
            if (
                response.status_code == 403
                and "rate limit exceeded" in str(response.content).lower()
            ):
                # Update token
                self.authenticator.get_next_auth_token()
                # Raise an error to force a retry with the new token.
                raise RetriableAPIError(msg)

            # The GitHub API randomly returns 401 Unauthorized errors, so we try again.
            if (
                response.status_code == 401
                # if the token is invalid, we are also told about it
                and not "bad credentials" in str(response.content).lower()
            ):
                raise RetriableAPIError(msg)

            # all other errors are fatal
            # Note: The API returns a 404 "Not Found" if trying to read a repo
            # for which the token is not allowed access.
            raise FatalAPIError(msg)

        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{str(response.content)} (Reason: {response.reason}) for path: {full_path}"
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

        yield from results

    def post_process(self, row: dict, context: Optional[Dict[str, str]] = None) -> dict:
        """Add `repo_id` by default to all streams."""
        if context is not None and "repo_id" in context:
            row["repo_id"] = context["repo_id"]
        return row


class GitHubGraphqlStream(GraphQLStream, GitHubRestStream):
    """GitHub Graphql stream class."""

    @property
    def url_base(self) -> str:
        return f'{self.config.get("api_url_base", self.DEFAULT_API_BASE_URL)}/graphql'

    # the jsonpath under which to fetch the list of records from the graphql response
    query_jsonpath: str = "$.data.[*]"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.

        Args:
            response: A raw `requests.Response`_ object.

        Yields:
            One item for every item found in the response.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        resp_json = response.json()
        yield from extract_jsonpath(self.query_jsonpath, input=resp_json)

    def get_url_params(
        self, context: Optional[Dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = context or dict()
        params["per_page"] = self.MAX_PER_PAGE
        return params
