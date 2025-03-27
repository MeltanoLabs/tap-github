"""REST client handling, including GitHubStream base class."""

from __future__ import annotations

import email.utils
import inspect
import random
import time
from types import FrameType
from typing import TYPE_CHECKING, Any, ClassVar, cast
from urllib.parse import parse_qs, urlparse

from dateutil.parser import parse
from nested_lookup import nested_lookup
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import GraphQLStream, RESTStream

from tap_github.authenticator import GitHubTokenAuthenticator

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from backoff.types import Details

EMPTY_REPO_ERROR_STATUS = 409


class GitHubRestStream(RESTStream):
    """GitHub Rest stream class."""

    MAX_PER_PAGE = 100  # GitHub's limit is 100.
    MAX_RESULTS_LIMIT: int | None = None
    DEFAULT_API_BASE_URL = "https://api.github.com"
    LOG_REQUEST_METRIC_URLS = True

    # GitHub is missing the "since" parameter on a few endpoints
    # set this parameter to True if your stream needs to navigate data in descending order  # noqa: E501
    # and try to exit early on its own.
    # This only has effect on streams whose `replication_key` is `updated_at`.
    use_fake_since_parameter = False

    _authenticator: GitHubTokenAuthenticator | None = None

    @property
    def authenticator(self) -> GitHubTokenAuthenticator:
        if self._authenticator is None:
            self._authenticator = GitHubTokenAuthenticator(stream=self)
        return self._authenticator

    @property
    def url_base(self) -> str:
        return self.config.get("api_url_base", self.DEFAULT_API_BASE_URL)

    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key: str | None = None
    tolerated_http_errors: ClassVar[list[int]] = []

    @property
    def http_headers(self) -> dict[str, str]:
        """Return the http headers needed."""
        headers = {"Accept": "application/vnd.github.v3+json"}
        headers["User-Agent"] = cast(str, self.config.get("user_agent", "tap-github"))
        return headers

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,  # noqa: ANN401
    ) -> Any | None:  # noqa: ANN401
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
        if "next" not in response.links:
            return None

        resp_json = response.json()
        results = resp_json if isinstance(resp_json, list) else resp_json.get("items")

        # Exit early if the response has no items. ? Maybe duplicative the "next" link check.  # noqa: E501
        if not results:
            return None

        # Unfortunately endpoints such as /starred, /stargazers, /events and /pulls do not support  # noqa: E501
        # the "since" parameter out of the box. So we use a workaround here to exit early.  # noqa: E501
        # For such streams, we sort by descending dates (most recent first), and paginate  # noqa: E501
        # "back in time" until we reach records before our "fake_since" parameter.
        if self.replication_key and self.use_fake_since_parameter:
            request_parameters = parse_qs(str(urlparse(response.request.url).query))
            # parse_qs interprets "+" as a space, revert this to keep an aware datetime
            try:
                since = (
                    request_parameters["fake_since"][0].replace(" ", "+")
                    if "fake_since" in request_parameters
                    else ""
                )
            except IndexError:
                return None

            direction = (
                request_parameters["direction"][0]
                if "direction" in request_parameters
                else None
            )

            # commit_timestamp is a constructed key which does not exist in the raw response  # noqa: E501
            replication_date = (
                results[-1][self.replication_key]
                if self.replication_key != "commit_timestamp"
                else results[-1]["commit"]["committer"]["date"]
            )
            # exit early if the replication_date is before our since parameter
            if (
                since
                and direction == "desc"
                and (parse(replication_date) < parse(since))
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
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {"per_page": self.MAX_PER_PAGE}
        if next_page_token:
            params["page"] = next_page_token

        if self.replication_key == "updated_at":
            params["sort"] = "updated"
            params["direction"] = "desc" if self.use_fake_since_parameter else "asc"

        # Unfortunately the /starred, /stargazers (starred_at) and /events (created_at) endpoints do not support  # noqa: E501
        # the "since" parameter out of the box. But we use a workaround in 'get_next_page_token'.  # noqa: E501
        elif self.replication_key in ["starred_at", "created_at"]:
            params["sort"] = "created"
            params["direction"] = "desc"

        # Warning: /commits endpoint accept "since" but results are ordered by descending commit_timestamp  # noqa: E501
        elif self.replication_key == "commit_timestamp":
            params["direction"] = "desc"

        elif self.replication_key:
            self.logger.warning(
                f"The replication key '{self.replication_key}' is not fully supported by this client yet."  # noqa: E501
            )

        since = self.get_starting_timestamp(context)
        since_key = "since" if not self.use_fake_since_parameter else "fake_since"
        if self.replication_key and since:
            params[since_key] = since.isoformat(sep="T")
            # Leverage conditional requests to save API quotas
            # https://github.community/t/how-does-if-modified-since-work/139627
            self._http_headers["If-modified-since"] = email.utils.format_datetime(since)
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
        if response.status_code in (
            [*self.tolerated_http_errors, EMPTY_REPO_ERROR_STATUS]
        ):
            msg = (
                f"{response.status_code} Tolerated Status Code "
                f"(Reason: {response.reason}) for path: {full_path}"
            )
            self.logger.info(msg)
            return

        if 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.content!s} (Reason: {response.reason}) for path: {full_path}"  # noqa: E501
            )
            # Retry on rate limiting
            if (
                response.status_code == 403
                and "rate limit exceeded" in str(response.content).lower()
            ):
                # Update token
                self.authenticator.get_next_auth_token()
                # Raise an error to force a retry with the new token.
                raise RetriableAPIError(msg, response)

            # Retry on secondary rate limit
            if (
                response.status_code == 403
                and "secondary rate limit" in str(response.content).lower()
            ):
                # Wait about a minute and retry
                time.sleep(60 + 30 * random.random())
                raise RetriableAPIError(msg, response)

            # The GitHub API randomly returns 401 Unauthorized errors, so we try again.
            if (
                response.status_code == 401
                # if the token is invalid, we are also told about it
                and "bad credentials" not in str(response.content).lower()
            ):
                raise RetriableAPIError(msg, response)

            # all other errors are fatal
            # Note: The API returns a 404 "Not Found" if trying to read a repo
            # for which the token is not allowed access.
            raise FatalAPIError(msg)

        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.content!s} (Reason: {response.reason}) for path: {full_path}"  # noqa: E501
            )
            raise RetriableAPIError(msg, response)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        # TODO - Split into handle_reponse and parse_response.
        if response.status_code in (
            [*self.tolerated_http_errors, EMPTY_REPO_ERROR_STATUS]
        ):
            return

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

    def post_process(self, row: dict, context: dict[str, str] | None = None) -> dict:
        """Add `repo_id` by default to all streams."""
        if context is not None and "repo_id" in context:
            row["repo_id"] = context["repo_id"]
        return row

    def backoff_handler(self, details: Details) -> None:
        """Handle retriable error by swapping auth token."""
        self.logger.info("Retrying request with different token")
        # use python introspection to obtain the error object
        # FIXME: replace this once https://github.com/litl/backoff/issues/158
        # is fixed
        exc = cast(
            FrameType,
            cast(FrameType, cast(FrameType, inspect.currentframe()).f_back).f_back,
        ).f_locals["e"]
        if (
            exc.response is not None
            and exc.response.status_code == 403
            and "rate limit exceeded" in str(exc.response.content)
        ):
            # we hit a rate limit, rotate token
            prepared_request = details["args"][0]
            self.authenticator.get_next_auth_token()
            prepared_request.headers.update(self.authenticator.auth_headers or {})

    def calculate_sync_cost(
        self,
        request: requests.PreparedRequest,
        response: requests.Response,
        context: dict | None,
    ) -> dict[str, int]:
        """Return the cost of the last REST API call."""
        return {"rest": 1, "graphql": 0, "search": 0}


class GitHubDiffStream(GitHubRestStream):
    """Base class for GitHub diff streams."""

    # Known Github API errors for diff requests
    tolerated_http_errors: ClassVar[list[int]] = [404, 406, 422, 502]

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed for diff requests."""
        headers = super().http_headers
        headers["Accept"] = "application/vnd.github.v3.diff"
        return headers

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response to yield the diff text instead of an object
        and prevent buffer overflow."""
        if response.status_code != 200:
            contents = response.json()
            self.logger.info(
                "Skipping %s due to %d error: %s",
                self.name.replace("_", " "),
                response.status_code,
                contents["message"],
            )
            yield {
                "success": False,
                "error_message": contents["message"],
            }
            return

        if content_length_str := response.headers.get("Content-Length"):
            content_length = int(content_length_str)
            max_size = 41_943_040  # 40 MiB
            if content_length > max_size:
                self.logger.info(
                    "Skipping %s. The diff size (%.2f MiB) exceeded the maximum"
                    " size limit of 40 MiB.",
                    self.name.replace("_", " "),
                    content_length / 1024 / 1024,
                )
                yield {
                    "success": False,
                    "error_message": "Diff exceeded the maximum size limit of 40 MiB.",
                }
                return

        yield {"diff": response.text, "success": True}


class GitHubGraphqlStream(GraphQLStream, GitHubRestStream):
    """GitHub Graphql stream class."""

    @property
    def url_base(self) -> str:
        return f"{self.config.get('api_url_base', self.DEFAULT_API_BASE_URL)}/graphql"

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

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,  # noqa: ANN401
    ) -> Any | None:  # noqa: ANN401
        """
        Return a dict of cursors for identifying next page or None if no more pages.

        Note - pagination requires the Graphql query to have nextPageCursor_X parameters
        with the assosciated hasNextPage_X, startCursor_X and endCursor_X.

        X should be an integer between 0 and 9, increasing with query depth.

        Warning - we recommend to avoid using deep (nested) pagination.
        """

        resp_json = response.json()

        # Find if results contains "hasNextPage_X" flags and if any are True.
        # If so, set nextPageCursor_X to endCursor_X for X max.

        next_page_results = nested_lookup(
            key="hasNextPage_",
            document=resp_json,
            wild=True,
            with_keys=True,
        )

        has_next_page_indices: list[int] = []
        # Iterate over all the items and filter items with hasNextPage = True.
        for key, value in next_page_results.items():
            # Check if key is even then add pair to new dictionary
            if any(value):
                pagination_index = int(str(key).split("_")[1])
                has_next_page_indices.append(pagination_index)

        # Check if any "hasNextPage" is True. Otherwise, exit early.
        if not len(has_next_page_indices) > 0:
            return None

        # Get deepest pagination item
        max_pagination_index = max(has_next_page_indices)

        # We leverage previous_token to remember the pagination cursors
        # for indices below max_pagination_index.
        next_page_cursors: dict[str, str] = {}
        for key, value in (previous_token or {}).items():
            # Only keep pagination info for indices below max_pagination_index.
            pagination_index = int(str(key).split("_")[1])
            if pagination_index < max_pagination_index:
                next_page_cursors[key] = value

        # Get the pagination cursor to update and increment it.
        next_page_end_cursor_results = nested_lookup(
            key=f"endCursor_{max_pagination_index}",
            document=resp_json,
        )

        next_page_key = f"nextPageCursor_{max_pagination_index}"
        next_page_cursor = next(
            cursor for cursor in next_page_end_cursor_results if cursor is not None
        )
        next_page_cursors[next_page_key] = next_page_cursor

        return next_page_cursors

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = context.copy() if context else {}
        params["per_page"] = self.MAX_PER_PAGE
        if next_page_token:
            params.update(next_page_token)

        since = self.get_starting_timestamp(context)
        if self.replication_key and since:
            params["since"] = since.isoformat(sep="T")

        return params

    def calculate_sync_cost(
        self,
        request: requests.PreparedRequest,
        response: requests.Response,
        context: dict | None,
    ) -> dict[str, int]:
        """Return the cost of the last graphql API call."""
        costgen = extract_jsonpath("$.data.rateLimit.cost", input=response.json())
        # calculate_sync_cost is called before the main response parsing.
        # In some cases, the tap crashes here before we have been able to
        # properly analyze where the error comes from, so we ignore these
        # costs to allow figuring out what happened downstream, by setting
        # them to 0.
        cost = next(costgen, 0)
        return {"rest": 0, "graphql": int(cost), "search": 0}

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        The graphql spec is a bit confusing around response codes
        (https://github.com/graphql/graphql-over-http/blob/main/spec/GraphQLOverHTTP.md#response)
        Github's API is a bit of a free adaptation of standards, so we
        choose fail immediately on error here, so that something is logged
        at the very minimum.

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.
        """
        super().validate_response(response)
        rj = response.json()
        if "errors" in rj:
            msg = rj["errors"]
            raise FatalAPIError(f"Graphql error: {msg}", response)
