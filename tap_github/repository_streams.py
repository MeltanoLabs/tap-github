"""Repository Stream types classes for tap-github."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import parse_qs, urlparse

from dateutil.parser import parse
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_github.client import GitHubDiffStream, GitHubGraphqlStream, GitHubRestStream
from tap_github.schema_objects import (
    files_object,
    label_object,
    milestone_object,
    reactions_object,
    user_object,
)
from tap_github.scraping import scrape_dependents, scrape_metrics

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests


class RepositoryStream(GitHubRestStream):
    """Defines 'Repository' stream."""

    name = "repositories"
    # updated_at will be updated any time the repository object is updated,
    # e.g. when the description or the primary language of the repository is updated.
    replication_key = "updated_at"

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        assert context is not None, f"Context cannot be empty for '{self.name}' stream."
        params = super().get_url_params(context, next_page_token)
        if "search_query" in context:
            # we're in search mode
            params["q"] = context["search_query"]

        return params

    @property
    def path(self) -> str:  # type: ignore
        """Return the API endpoint path. Path options are mutually exclusive."""

        if "searches" in self.config:
            # Search API max: 1,000 total.
            self.MAX_RESULTS_LIMIT = 1000
            return "/search/repositories"
        if "repositories" in self.config:
            # the `repo` and `org` args will be parsed from the partition's `context`
            return "/repos/{org}/{repo}"
        if "organizations" in self.config:
            return "/orgs/{org}/repos"

    @property
    def records_jsonpath(self) -> str:  # type: ignore
        if "searches" in self.config:
            return "$.items[*]"
        else:
            return "$[*]"

    def get_repo_ids(self, repo_list: list[tuple[str]]) -> list[dict[str, str]]:
        """Enrich the list of repos with their numeric ID from github.

        This helps maintain a stable id for context and bookmarks.
        It uses the github graphql api to fetch the databaseId.
        It also removes non-existant repos and corrects casing to ensure
        data is correct downstream.
        """

        # use a temp handmade stream to reuse all the graphql setup of the tap
        class TempStream(GitHubGraphqlStream):
            name = "tempStream"
            schema = th.PropertiesList(
                th.Property("id", th.StringType),
                th.Property("databaseId", th.IntegerType),
            ).to_dict()

            def __init__(self, tap, repo_list) -> None:  # noqa: ANN001
                super().__init__(tap)
                self.repo_list = repo_list

            @property
            def query(self) -> str:
                chunks = []
                for i, repo in enumerate(self.repo_list):
                    chunks.append(
                        f'repo{i}: repository(name: "{repo[1]}", owner: "{repo[0]}") '
                        "{ nameWithOwner databaseId }"
                    )
                return "query {" + " ".join(chunks) + " rateLimit { cost } }"

            def validate_response(self, response: requests.Response) -> None:
                """Allow some specific errors.
                Do not raise exceptions if the error is "type": "NOT_FOUND"
                as we actually expect these in this stream when we send an invalid
                repo name.
                """
                try:
                    super().validate_response(response)
                except FatalAPIError as e:
                    if "NOT_FOUND" in str(e):
                        return
                    raise

        if len(repo_list) < 1:
            return []

        repos_with_ids: list = []
        temp_stream = TempStream(self._tap, list(repo_list))
        # replace manually provided org/repo values by the ones obtained
        # from github api. This guarantees that case is correct in the output data.
        # See https://github.com/MeltanoLabs/tap-github/issues/110
        # Also remove repos which do not exist to avoid crashing further down
        # the line.
        for record in temp_stream.request_records({}):
            for item in record:
                if item == "rateLimit":
                    continue
                try:
                    repo_full_name = "/".join(repo_list[int(item[4:])])
                    name_with_owner = record[item]["nameWithOwner"]
                    org, repo = name_with_owner.split("/")
                except TypeError:
                    # one of the repos returned `None`, which means it does
                    # not exist, log some details, and move on to the next one
                    repo_full_name = "/".join(repo_list[int(item[4:])])
                    self.logger.info(
                        f"Repository not found: {repo_full_name} \t"
                        "Removing it from list"
                    )
                    continue
                # check if repo has moved or been renamed
                if repo_full_name.lower() != name_with_owner.lower():
                    # the repo name has changed, log some details, and move on.
                    self.logger.info(
                        f"Repository name changed: {repo_full_name} \t"
                        f"New name: {name_with_owner}"
                    )

                repos_with_ids.append(
                    {"org": org, "repo": repo, "repo_id": record[item]["databaseId"]}
                )
        self.logger.info(f"Running the tap on {len(repos_with_ids)} repositories")
        return repos_with_ids

    @property
    def partitions(self) -> list[dict[str, str]] | None:
        """Return a list of partitions.

        This is called before syncing records, we use it to fetch some additional
        context
        """
        if "searches" in self.config:
            return [
                {"search_name": s["name"], "search_query": s["query"]}
                for s in self.config["searches"]
            ]

        if "repositories" in self.config:
            split_repo_names = [s.split("/") for s in self.config["repositories"]]
            augmented_repo_list = []
            # chunk requests to the graphql endpoint to avoid timeouts and other
            # obscure errors that the api doesn't say much about. The actual limit
            # seems closer to 1000, use half that to stay safe.
            chunk_size = 500
            list_length = len(split_repo_names)
            self.logger.info(f"Filtering repository list of {list_length} repositories")
            for ndx in range(0, list_length, chunk_size):
                augmented_repo_list += self.get_repo_ids(
                    split_repo_names[ndx : ndx + chunk_size]
                )
            self.logger.info(
                f"Running the tap on {len(augmented_repo_list)} repositories"
            )
            return augmented_repo_list

        if "organizations" in self.config:
            return [{"org": org} for org in self.config["organizations"]]
        return None

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.
        Developers may override this behavior to send specific information to child
        streams for context.
        """
        return {
            "org": record["owner"]["login"],
            "repo": record["name"],
            "repo_id": record["id"],
        }

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """
        Override the parent method to allow skipping API calls
        if the stream is deselected and skip_parent_streams is True in config.
        This allows running the tap with fewer API calls and preserving
        quota when only syncing a child stream. Without this,
        the API call is sent but data is discarded.
        """
        if (
            not self.selected
            and "skip_parent_streams" in self.config
            and self.config["skip_parent_streams"]
            and context is not None
        ):
            # build a minimal mock record so that self._sync_records
            # can proceed with child streams
            # the id is fetched in `get_repo_ids` above
            yield {
                "owner": {
                    "login": context["org"],
                },
                "name": context["repo"],
                "id": context["repo_id"],
            }
        else:
            yield from super().get_records(context)

    schema = th.PropertiesList(
        th.Property("search_name", th.StringType),
        th.Property("search_query", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("name", th.StringType),
        th.Property("full_name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("owner", user_object),
        th.Property(
            "license",
            th.ObjectType(
                th.Property("key", th.StringType),
                th.Property("name", th.StringType),
                th.Property("url", th.StringType),
                th.Property("spdx_id", th.StringType),
            ),
        ),
        th.Property("master_branch", th.StringType),
        th.Property("default_branch", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("pushed_at", th.DateTimeType),
        th.Property("git_url", th.StringType),
        th.Property("ssh_url", th.StringType),
        th.Property("clone_url", th.StringType),
        th.Property("homepage", th.StringType),
        th.Property("private", th.BooleanType),
        th.Property("archived", th.BooleanType),
        th.Property("disabled", th.BooleanType),
        th.Property("size", th.IntegerType),
        th.Property("stargazers_count", th.IntegerType),
        th.Property("fork", th.BooleanType),
        th.Property(
            "topics",
            th.ArrayType(th.StringType),
        ),
        th.Property("visibility", th.StringType),
        th.Property("language", th.StringType),
        # These `_count` metrics appear to be duplicates but have valid data
        # and are documented: https://docs.github.com/en/rest/reference/search
        th.Property("forks", th.IntegerType),
        th.Property("forks_count", th.IntegerType),
        th.Property("watchers", th.IntegerType),
        th.Property("watchers_count", th.IntegerType),
        th.Property("open_issues", th.IntegerType),
        th.Property("network_count", th.IntegerType),
        th.Property("subscribers_count", th.IntegerType),
        th.Property("open_issues_count", th.IntegerType),
        th.Property("allow_squash_merge", th.BooleanType),
        th.Property("allow_merge_commit", th.BooleanType),
        th.Property("allow_rebase_merge", th.BooleanType),
        th.Property("allow_auto_merge", th.BooleanType),
        th.Property("delete_branch_on_merge", th.BooleanType),
        th.Property("organization", user_object),
    ).to_dict()


class ReadmeStream(GitHubRestStream):
    """
    A stream dedicated to fetching the object version of a README.md.

    Including its content, base64 encoded of the readme in GitHub flavored Markdown.
    For html, see ReadmeHtmlStream.
    """

    name = "readme"
    path = "/repos/{org}/{repo}/readme"
    primary_keys: ClassVar[list[str]] = ["repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    tolerated_http_errors: ClassVar[list[int]] = [404]

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # README Keys
        th.Property("type", th.StringType),
        th.Property("encoding", th.StringType),
        th.Property("size", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("path", th.StringType),
        th.Property("content", th.StringType),
        th.Property("sha", th.StringType),
        th.Property("url", th.StringType),
        th.Property("git_url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("download_url", th.StringType),
        th.Property(
            "_links",
            th.ObjectType(
                th.Property("git", th.StringType),
                th.Property("self", th.StringType),
                th.Property("html", th.StringType),
            ),
        ),
    ).to_dict()


class ReadmeHtmlStream(GitHubRestStream):
    """
    A stream dedicated to fetching the HTML version of README.md.

    For the object details, such as path and size, see ReadmeStream.
    """

    name = "readme_html"
    path = "/repos/{org}/{repo}/readme"
    primary_keys: ClassVar[list[str]] = ["repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    tolerated_http_errors: ClassVar[list[int]] = [404]

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Overridden to get the raw HTML version of the readme.
        """
        headers = super().http_headers
        headers["Accept"] = "application/vnd.github.v3.html"
        return headers

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the README to yield the html response instead of an object."""
        if response.status_code in self.tolerated_http_errors:
            return

        yield {"raw_html": response.text}

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Readme HTML
        th.Property("raw_html", th.StringType),
    ).to_dict()


class CommunityProfileStream(GitHubRestStream):
    """Defines 'CommunityProfile' stream."""

    name = "community_profile"
    path = "/repos/{org}/{repo}/community/profile"
    primary_keys: ClassVar[list[str]] = ["repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    tolerated_http_errors: ClassVar[list[int]] = [404]

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Community Profile
        th.Property("health_percentage", th.IntegerType),
        th.Property("description", th.StringType),
        th.Property("documentation", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("content_reports_enabled", th.BooleanType),
        th.Property(
            "files",
            th.ObjectType(
                th.Property(
                    "code_of_conduct",
                    th.ObjectType(
                        th.Property("key", th.StringType),
                        th.Property("name", th.StringType),
                        th.Property("html_url", th.StringType),
                        th.Property("url", th.StringType),
                    ),
                ),
                th.Property(
                    "code_of_conduct_file",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
                th.Property(
                    "contributing",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
                th.Property(
                    "issue_template",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
                th.Property(
                    "pull_request_template",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
                th.Property(
                    "license",
                    th.ObjectType(
                        th.Property("key", th.StringType),
                        th.Property("name", th.StringType),
                        th.Property("spdx_id", th.StringType),
                        th.Property("node_id", th.StringType),
                        th.Property("html_url", th.StringType),
                        th.Property("url", th.StringType),
                    ),
                ),
                th.Property(
                    "readme",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
            ),
        ),
    ).to_dict()


class EventsStream(GitHubRestStream):
    """
    Defines 'Events' stream.
    Issue events are fetched from the repository level (as opposed to per issue)
    to optimize for API quota usage.
    """

    name = "events"
    path = "/repos/{org}/{repo}/events"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "created_at"
    parent_stream_type = RepositoryStream
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    ignore_parent_replication_key = True
    # GitHub is missing the "since" parameter on this endpoint.
    use_fake_since_parameter = True

    def get_records(self, context: dict | None = None) -> Iterable[dict[str, Any]]:
        """Return a generator of row-type dictionary objects.
        Each row emitted should be a dictionary of property names to their values.
        """
        if context and context.get("events", None) == 0:
            self.logger.debug(f"No events detected. Skipping '{self.name}' sync.")
            return []

        return super().get_records(context)

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row = super().post_process(row, context)
        # TODO - We should think about the best approach to handle this. An alternative would be to  # noqa: E501
        # do a 'dumb' tap that just keeps the same schemas as GitHub without renaming these  # noqa: E501
        # objects to "target_". They are worth keeping, however, as they can be different from  # noqa: E501
        # the parent stream, e.g. for fork/parent PR events.
        row["target_repo"] = row.pop("repo", None)
        row["target_org"] = row.pop("org", None)
        return row

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        th.Property("public", th.BooleanType),
        th.Property("_sdc_repository", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("distinct_size", th.IntegerType),
        th.Property("head", th.StringType),
        th.Property("push_id", th.IntegerType),
        th.Property("ref", th.StringType),
        th.Property("size", th.IntegerType),
        th.Property(
            "target_repo",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "target_org",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("login", th.StringType),
            ),
        ),
        th.Property(
            "actor",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("login", th.StringType),
                th.Property("display_login", th.StringType),
                th.Property("avatar_url", th.StringType),
                th.Property("gravatar_id", th.StringType),
                th.Property("url", th.StringType),
            ),
        ),
        th.Property(
            "payload",
            th.ObjectType(
                th.Property("before", th.StringType),
                th.Property("action", th.StringType),
                th.Property(
                    "comment",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("id", th.IntegerType),
                        th.Property("node_id", th.StringType),
                        th.Property("body", th.StringType),
                        th.Property("created_at", th.DateTimeType),
                        th.Property("updated_at", th.DateTimeType),
                    ),
                ),
                th.Property(
                    "comments",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("url", th.StringType),
                            th.Property("id", th.IntegerType),
                            th.Property("node_id", th.StringType),
                            th.Property("body", th.StringType),
                            th.Property("created_at", th.DateTimeType),
                            th.Property("updated_at", th.DateTimeType),
                        ),
                    ),
                ),
                th.Property(
                    "issue",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("id", th.IntegerType),
                        th.Property("node_id", th.StringType),
                        th.Property("number", th.IntegerType),
                        th.Property("title", th.StringType),
                        th.Property("body", th.StringType),
                        th.Property("created_at", th.DateTimeType),
                        th.Property("updated_at", th.DateTimeType),
                    ),
                ),
                th.Property(
                    "pull_request",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("id", th.IntegerType),
                        th.Property("node_id", th.StringType),
                        th.Property("number", th.IntegerType),
                        th.Property("title", th.StringType),
                        th.Property("body", th.StringType),
                        th.Property("created_at", th.DateTimeType),
                        th.Property("updated_at", th.DateTimeType),
                    ),
                ),
                th.Property(
                    "review",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("id", th.IntegerType),
                        th.Property("node_id", th.StringType),
                        th.Property("body", th.StringType),
                        th.Property("submitted_at", th.DateTimeType),
                    ),
                ),
                th.Property("description", th.StringType),
                th.Property("master_branch", th.StringType),
                th.Property("pusher_type", th.StringType),
                th.Property("ref", th.StringType),
                th.Property("ref_type", th.StringType),
                th.Property(
                    "commits",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property(
                                "author",
                                th.ObjectType(
                                    th.Property("email", th.StringType),
                                    th.Property("name", th.StringType),
                                ),
                            ),
                            th.Property("distinct", th.BooleanType),
                            th.Property("message", th.StringType),
                            th.Property("sha", th.StringType),
                            th.Property("url", th.StringType),
                        ),
                    ),
                ),
            ),
        ),
    ).to_dict()


class MilestonesStream(GitHubRestStream):
    name = "milestones"
    path = "/repos/{org}/{repo}/milestones"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    parent_stream_type = RepositoryStream
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    ignore_parent_replication_key = True

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        assert context is not None, f"Context cannot be empty for '{self.name}' stream."
        params = super().get_url_params(context, next_page_token)
        params["state"] = "open"

        if "milestones" in self.config.get("stream_options", {}):
            params.update(self.config["stream_options"]["milestones"])

        return params

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Rest
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("labels_url", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("number", th.IntegerType),
        th.Property("state", th.StringType),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("creator", user_object),
        th.Property("open_issues", th.IntegerType),
        th.Property("closed_issues", th.IntegerType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("closed_at", th.DateTimeType),
        th.Property("due_on", th.StringType),
    ).to_dict()


class ReleasesStream(GitHubRestStream):
    name = "releases"
    path = "/repos/{org}/{repo}/releases"
    ignore_parent_replication_key = True
    primary_keys: ClassVar[list[str]] = ["id"]
    parent_stream_type = RepositoryStream
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    replication_key = "created_at"

    MAX_RESULTS_LIMIT = 10000

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Rest
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("assets_url", th.StringType),
        th.Property("upload_url", th.StringType),
        th.Property("tarball_url", th.StringType),
        th.Property("zipball_url", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("tag_name", th.StringType),
        th.Property("target_commitish", th.StringType),
        th.Property("name", th.StringType),
        th.Property("body", th.StringType),
        th.Property("draft", th.BooleanType),
        th.Property("prerelease", th.BooleanType),
        th.Property("created_at", th.DateTimeType),
        th.Property("published_at", th.DateTimeType),
        th.Property("author", user_object),
        th.Property(
            "assets",
            th.ArrayType(
                th.ObjectType(
                    th.Property("url", th.StringType),
                    th.Property("browser_download_url", th.StringType),
                    th.Property("id", th.IntegerType),
                    th.Property("node_id", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("label", th.StringType),
                    th.Property("state", th.StringType),
                    th.Property("content_type", th.StringType),
                    th.Property("size", th.IntegerType),
                    th.Property("download_count", th.IntegerType),
                    th.Property("created_at", th.DateTimeType),
                    th.Property("updated_at", th.DateTimeType),
                    th.Property("uploader", user_object),
                )
            ),
        ),
    ).to_dict()


class LanguagesStream(GitHubRestStream):
    name = "languages"
    path = "/repos/{org}/{repo}/languages"
    primary_keys: ClassVar[list[str]] = ["repo", "org", "language_name"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the language response and reformat to return as an iterator of [{language_name: Python, bytes: 23}]."""  # noqa: E501
        if response.status_code in self.tolerated_http_errors:
            return

        languages_json = response.json()
        for key, value in languages_json.items():
            yield {"language_name": key, "bytes": value}

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # A list of languages parsed by GitHub is available here:
        # https://github.com/github/linguist/blob/master/lib/linguist/languages.yml
        th.Property("language_name", th.StringType),
        th.Property("bytes", th.IntegerType),
    ).to_dict()


class CollaboratorsStream(GitHubRestStream):
    name = "collaborators"
    path = "/repos/{org}/{repo}/collaborators"
    primary_keys: ClassVar[list[str]] = ["id", "repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Rest
        th.Property("login", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("avatar_url", th.StringType),
        th.Property("gravatar_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("type", th.StringType),
        th.Property("site_admin", th.BooleanType),
        th.Property(
            "permissions",
            th.ObjectType(
                th.Property("pull", th.BooleanType),
                th.Property("triage", th.BooleanType),
                th.Property("push", th.BooleanType),
                th.Property("maintain", th.BooleanType),
                th.Property("admin", th.BooleanType),
            ),
        ),
        th.Property("role_name", th.StringType),
    ).to_dict()


class AssigneesStream(GitHubRestStream):
    """Defines 'Assignees' stream which returns possible assignees for issues/prs following GitHub's API convention."""  # noqa: E501

    name = "assignees"
    path = "/repos/{org}/{repo}/assignees"
    primary_keys: ClassVar[list[str]] = ["id"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Rest
        th.Property("login", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("avatar_url", th.StringType),
        th.Property("gravatar_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("type", th.StringType),
        th.Property("site_admin", th.BooleanType),
    ).to_dict()


class IssuesStream(GitHubRestStream):
    """Defines 'Issues' stream which returns Issues and PRs following GitHub's API convention."""  # noqa: E501

    name = "issues"
    path = "/repos/{org}/{repo}/issues"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        assert context is not None, f"Context cannot be empty for '{self.name}' stream."
        params = super().get_url_params(context, next_page_token)
        # Fetch all issues and PRs, regardless of state (OPEN, CLOSED, MERGED).
        # To exclude PRs from the issues stream, you can use the Stream Maps in the config.  # noqa: E501
        # {
        #     // ..
        #     "stream_maps": {
        #         "issues": {
        #             "__filter__": "record['type'] = 'issue'"
        #         }
        #     }
        # }
        params["state"] = "all"
        return params

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Overridden to use beta endpoint which includes reactions as described here:
        https://developer.github.com/changes/2016-05-12-reactions-api-preview/
        """
        headers = super().http_headers
        headers["Accept"] = "application/vnd.github.squirrel-girl-preview"
        return headers

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row = super().post_process(row, context)
        row["type"] = "pull_request" if "pull_request" in row else "issue"
        if row["body"] is not None:
            # some issue bodies include control characters such as \x00
            # that some targets (such as postgresql) choke on. This ensures
            # such chars are removed from the data before we pass it on to
            # the target
            row["body"] = row["body"].replace("\x00", "")
        if row["title"] is not None:
            row["title"] = row["title"].replace("\x00", "")

        # replace +1/-1 emojis to avoid downstream column name errors.
        if "reactions" in row:
            row["reactions"]["plus_one"] = row["reactions"].pop("+1", None)
            row["reactions"]["minus_one"] = row["reactions"].pop("-1", None)
        return row

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        th.Property("number", th.IntegerType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("closed_at", th.DateTimeType),
        th.Property("state", th.StringType),
        th.Property("title", th.StringType),
        th.Property("comments", th.IntegerType),
        th.Property("author_association", th.StringType),
        th.Property("body", th.StringType),
        th.Property("type", th.StringType),
        th.Property("user", user_object),
        th.Property(
            "labels",
            th.ArrayType(label_object),
        ),
        th.Property("reactions", reactions_object),
        th.Property("assignee", user_object),
        th.Property(
            "assignees",
            th.ArrayType(user_object),
        ),
        th.Property("milestone", milestone_object),
        th.Property("locked", th.BooleanType),
        th.Property(
            "pull_request",
            th.ObjectType(
                th.Property("html_url", th.StringType),
                th.Property("url", th.StringType),
                th.Property("diff_url", th.StringType),
                th.Property("patch_url", th.StringType),
            ),
        ),
    ).to_dict()


class IssueCommentsStream(GitHubRestStream):
    """
    Defines 'IssueComments' stream.
    Issue comments are fetched from the repository level (as opposed to per issue)
    to optimize for API quota usage.
    """

    name = "issue_comments"
    path = "/repos/{org}/{repo}/issues/comments"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    parent_stream_type = RepositoryStream
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    ignore_parent_replication_key = True
    # FIXME: this allows the tap to continue on server-side timeouts but means
    # we have gaps in our data
    tolerated_http_errors: ClassVar[list[int]] = [502]

    # GitHub is not missing the "since" parameter on this endpoint.
    # But it is too expensive on large repos and results in a lot of server errors.
    use_fake_since_parameter = True

    def get_records(self, context: dict | None = None) -> Iterable[dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        if context and context.get("comments", None) == 0:
            self.logger.debug(f"No comments detected. Skipping '{self.name}' sync.")
            return []

        return super().get_records(context)

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row = super().post_process(row, context)
        row["issue_number"] = int(row["issue_url"].split("/")[-1])
        if row["body"] is not None:
            # some comment bodies include control characters such as \x00
            # that some targets (such as postgresql) choke on. This ensures
            # such chars are removed from the data before we pass it on to
            # the target
            row["body"] = row["body"].replace("\x00", "")
        return row

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Rest
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("issue_number", th.IntegerType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("issue_url", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("author_association", th.StringType),
        th.Property("body", th.StringType),
        th.Property("user", user_object),
    ).to_dict()


class IssueEventsStream(GitHubRestStream):
    """
    Defines 'IssueEvents' stream.
    Issue events are fetched from the repository level (as opposed to per issue)
    to optimize for API quota usage.
    """

    name = "issue_events"
    path = "/repos/{org}/{repo}/issues/events"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "created_at"
    parent_stream_type = RepositoryStream
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    ignore_parent_replication_key = True
    # GitHub is missing the "since" parameter on this endpoint.
    use_fake_since_parameter = True

    def get_records(self, context: dict | None = None) -> Iterable[dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        if context and context.get("events", None) == 0:
            self.logger.debug(f"No events detected. Skipping '{self.name}' sync.")
            return []

        return super().get_records(context)

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row = super().post_process(row, context)
        if issue := row.get("issue"):
            row["issue_number"] = int(issue.pop("number"))
            row["issue_url"] = issue.pop("url")
        else:
            self.logger.debug(
                f"No issue assosciated with event {row['id']} - {row['event']}."
            )

        return row

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        th.Property("issue_number", th.IntegerType),
        th.Property("issue_url", th.StringType),
        th.Property("event", th.StringType),
        th.Property("commit_id", th.StringType),
        th.Property("commit_url", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("actor", user_object),
    ).to_dict()


class CommitsStream(GitHubRestStream):
    """
    Defines the 'Commits' stream.
    The stream is fetched per repository to optimize for API quota usage.
    """

    name = "commits"
    path = "/repos/{org}/{repo}/commits"
    primary_keys: ClassVar[list[str]] = ["node_id"]
    replication_key = "commit_timestamp"
    parent_stream_type = RepositoryStream
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    ignore_parent_replication_key = True

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        """
        Add a timestamp top-level field to be used as state replication key.
        It's not clear from github's API docs which time (author or committer)
        is used to compare to the `since` argument that the endpoint supports.
        """
        assert context is not None, "CommitsStream was called without context"
        row = super().post_process(row, context)
        row["commit_timestamp"] = row["commit"]["committer"]["date"]
        return row

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            "org": context["org"] if context else None,
            "repo": context["repo"] if context else None,
            "repo_id": context["repo_id"] if context else None,
            "commit_id": record["sha"],
        }

    schema = th.PropertiesList(
        th.Property("org", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("repo_id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("sha", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("commit_timestamp", th.DateTimeType),
        th.Property(
            "commit",
            th.ObjectType(
                th.Property(
                    "author",
                    th.ObjectType(
                        th.Property("name", th.StringType),
                        th.Property("email", th.StringType),
                        th.Property("date", th.DateTimeType),
                    ),
                ),
                th.Property(
                    "committer",
                    th.ObjectType(
                        th.Property("name", th.StringType),
                        th.Property("email", th.StringType),
                        th.Property("date", th.DateTimeType),
                    ),
                ),
                th.Property("message", th.StringType),
                th.Property(
                    "tree",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("sha", th.StringType),
                    ),
                ),
                th.Property(
                    "verification",
                    th.ObjectType(
                        th.Property("verified", th.BooleanType),
                        th.Property("reason", th.StringType),
                        th.Property("signature", th.StringType),
                        th.Property("payload", th.StringType),
                    ),
                ),
            ),
        ),
        th.Property("author", user_object),
        th.Property("committer", user_object),
    ).to_dict()


class CommitCommentsStream(GitHubRestStream):
    name = "commit_comments"
    path = "/repos/{org}/{repo}/comments"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    parent_stream_type = RepositoryStream
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    ignore_parent_replication_key = True

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Rest
        th.Property("html_url", th.StringType),
        th.Property("url", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("body", th.StringType),
        th.Property("path", th.StringType),
        th.Property("position", th.IntegerType),
        th.Property("line", th.IntegerType),
        th.Property("commit_id", th.StringType),
        th.Property("user", user_object),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("author_association", th.StringType),
    ).to_dict()


class CommitDiffsStream(GitHubDiffStream):
    name = "commit_diffs"
    path = "/repos/{org}/{repo}/commits/{commit_id}"
    primary_keys: ClassVar[list[str]] = ["commit_id"]
    parent_stream_type = CommitsStream
    ignore_parent_replication_key = False
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    def post_process(self, row: dict, context: dict[str, str] | None = None) -> dict:
        row = super().post_process(row, context)
        if context is not None:
            # Get commit ID (sha) from context
            row["org"] = context["org"]
            row["repo"] = context["repo"]
            row["repo_id"] = context["repo_id"]
            row["commit_id"] = context["commit_id"]
        return row

    schema = th.PropertiesList(
        # Parent keys
        th.Property("org", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("repo_id", th.IntegerType),
        th.Property("commit_id", th.StringType),
        # Rest
        th.Property("diff", th.StringType),
        th.Property("success", th.BooleanType),
        th.Property("error_message", th.StringType),
    ).to_dict()


class LabelsStream(GitHubRestStream):
    """Defines 'labels' stream."""

    name = "labels"
    path = "/repos/{org}/{repo}/labels"
    primary_keys: ClassVar[list[str]] = ["id"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Label Keys
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("color", th.StringType),
        th.Property("default", th.BooleanType),
    ).to_dict()


class PullRequestsStream(GitHubRestStream):
    """Defines 'PullRequests' stream."""

    name = "pull_requests"
    path = "/repos/{org}/{repo}/pulls"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    # GitHub is missing the "since" parameter on this endpoint.
    use_fake_since_parameter = True

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        assert context is not None, f"Context cannot be empty for '{self.name}' stream."
        params = super().get_url_params(context, next_page_token)
        # Fetch all pull requests regardless of state (OPEN, CLOSED, MERGED).
        params["state"] = "all"
        return params

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Overridden to use beta endpoint which includes reactions as described here:
        https://developer.github.com/changes/2016-05-12-reactions-api-preview/
        """
        headers = super().http_headers
        headers["Accept"] = "application/vnd.github.squirrel-girl-preview"
        return headers

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row = super().post_process(row, context)
        if row["body"] is not None:
            # some pr bodies include control characters such as \x00
            # that some targets (such as postgresql) choke on. This ensures
            # such chars are removed from the data before we pass it on to
            # the target
            row["body"] = row["body"].replace("\x00", "")
        if row["title"] is not None:
            row["title"] = row["title"].replace("\x00", "")

        # replace +1/-1 emojis to avoid downstream column name errors.
        if "reactions" in row:
            row["reactions"]["plus_one"] = row["reactions"].pop("+1", None)
            row["reactions"]["minus_one"] = row["reactions"].pop("-1", None)
        return row

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        if context:
            return {
                "org": context["org"],
                "repo": context["repo"],
                "repo_id": context["repo_id"],
                "pull_number": record["number"],
                "pull_id": record["id"],
            }
        return {
            "pull_number": record["number"],
            "pull_id": record["id"],
            "org": record["base"]["user"]["login"],
            "repo": record["base"]["repo"]["name"],
            "repo_id": record["base"]["repo"]["id"],
        }

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # PR keys
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("diff_url", th.StringType),
        th.Property("patch_url", th.StringType),
        th.Property("number", th.IntegerType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("closed_at", th.DateTimeType),
        th.Property("merged_at", th.DateTimeType),
        th.Property("state", th.StringType),
        th.Property("title", th.StringType),
        th.Property("locked", th.BooleanType),
        th.Property("comments", th.IntegerType),
        th.Property("author_association", th.StringType),
        th.Property("body", th.StringType),
        th.Property("merge_commit_sha", th.StringType),
        th.Property("draft", th.BooleanType),
        th.Property("commits_url", th.StringType),
        th.Property("review_comments_url", th.StringType),
        th.Property("review_comment_url", th.StringType),
        th.Property("comments_url", th.StringType),
        th.Property("statuses_url", th.StringType),
        th.Property("user", user_object),
        th.Property(
            "labels",
            th.ArrayType(label_object),
        ),
        th.Property("reactions", reactions_object),
        th.Property("assignee", user_object),
        th.Property(
            "assignees",
            th.ArrayType(user_object),
        ),
        th.Property(
            "requested_reviewers",
            th.ArrayType(user_object),
        ),
        th.Property("milestone", milestone_object),
        th.Property("locked", th.BooleanType),
        th.Property(
            "pull_request",
            th.ObjectType(
                th.Property("html_url", th.StringType),
                th.Property("url", th.StringType),
                th.Property("diff_url", th.StringType),
                th.Property("patch_url", th.StringType),
            ),
        ),
        th.Property(
            "head",
            th.ObjectType(
                th.Property("label", th.StringType),
                th.Property("ref", th.StringType),
                th.Property("sha", th.StringType),
                th.Property("user", user_object),
                th.Property(
                    "repo",
                    th.ObjectType(
                        th.Property("id", th.IntegerType),
                        th.Property("node_id", th.StringType),
                        th.Property("name", th.StringType),
                        th.Property("full_name", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
            ),
        ),
        th.Property(
            "base",
            th.ObjectType(
                th.Property("label", th.StringType),
                th.Property("ref", th.StringType),
                th.Property("sha", th.StringType),
                th.Property("user", user_object),
                th.Property(
                    "repo",
                    th.ObjectType(
                        th.Property("id", th.IntegerType),
                        th.Property("node_id", th.StringType),
                        th.Property("name", th.StringType),
                        th.Property("full_name", th.StringType),
                        th.Property("html_url", th.StringType),
                    ),
                ),
            ),
        ),
    ).to_dict()


class PullRequestCommitsStream(GitHubRestStream):
    name = "pull_request_commits"
    path = "/repos/{org}/{repo}/pulls/{pull_number}/commits"
    ignore_parent_replication_key = False
    primary_keys: ClassVar[list[str]] = ["node_id"]
    parent_stream_type = PullRequestsStream
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            "org": context["org"] if context else None,
            "repo": context["repo"] if context else None,
            "repo_id": context["repo_id"] if context else None,
            "pull_number": context["pull_number"] if context else None,
            "commit_id": record["sha"],
        }

    schema = th.PropertiesList(
        # Parent keys
        th.Property("org", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("repo_id", th.IntegerType),
        th.Property("pull_number", th.IntegerType),
        # Rest
        th.Property("url", th.StringType),
        th.Property("sha", th.StringType),
        th.Property("node_id", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("comments_url", th.StringType),
        th.Property(
            "commit",
            th.ObjectType(
                th.Property("url", th.StringType),
                th.Property(
                    "author",
                    th.ObjectType(
                        th.Property("name", th.StringType),
                        th.Property("email", th.StringType),
                        th.Property("date", th.StringType),
                    ),
                ),
                th.Property(
                    "committer",
                    th.ObjectType(
                        th.Property("name", th.StringType),
                        th.Property("email", th.StringType),
                        th.Property("date", th.StringType),
                    ),
                ),
                th.Property("message", th.StringType),
                th.Property(
                    "tree",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("sha", th.StringType),
                    ),
                ),
                th.Property("comment_count", th.IntegerType),
                th.Property(
                    "verification",
                    th.ObjectType(
                        th.Property("verified", th.BooleanType),
                        th.Property("reason", th.StringType),
                        th.Property("signature", th.StringType),
                        th.Property("payload", th.StringType),
                    ),
                ),
            ),
        ),
        th.Property("author", user_object),
        th.Property("committer", user_object),
        th.Property(
            "parents",
            th.ArrayType(
                th.ObjectType(
                    th.Property("url", th.StringType), th.Property("sha", th.StringType)
                )
            ),
        ),
        th.Property("files", th.ArrayType(files_object)),
        th.Property(
            "stats",
            th.ObjectType(
                th.Property("additions", th.IntegerType),
                th.Property("deletions", th.IntegerType),
                th.Property("total", th.IntegerType),
            ),
        ),
    ).to_dict()

    def post_process(self, row: dict, context: dict[str, str] | None = None) -> dict:
        row = super().post_process(row, context)
        if context is not None and "pull_number" in context:
            row["pull_number"] = context["pull_number"]
        return row


class PullRequestDiffsStream(GitHubDiffStream):
    name = "pull_request_diffs"
    path = "/repos/{org}/{repo}/pulls/{pull_number}"
    primary_keys: ClassVar[list[str]] = ["pull_id"]
    parent_stream_type = PullRequestsStream
    ignore_parent_replication_key = False
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    def post_process(self, row: dict, context: dict[str, str] | None = None) -> dict:
        row = super().post_process(row, context)
        if context is not None:
            # Get PR ID from context
            row["org"] = context["org"]
            row["repo"] = context["repo"]
            row["repo_id"] = context["repo_id"]
            row["pull_number"] = context["pull_number"]
            row["pull_id"] = context["pull_id"]
        return row

    schema = th.PropertiesList(
        # Parent keys
        th.Property("org", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("repo_id", th.IntegerType),
        th.Property("pull_number", th.IntegerType),
        th.Property("pull_id", th.IntegerType),
        # Rest
        th.Property("diff", th.StringType),
        th.Property("success", th.BooleanType),
        th.Property("error_message", th.StringType),
    ).to_dict()


class PullRequestCommitDiffsStream(GitHubDiffStream):
    name = "pull_request_commit_diffs"
    path = "/repos/{org}/{repo}/commits/{commit_id}"
    primary_keys: ClassVar[list[str]] = ["commit_id"]
    parent_stream_type = PullRequestCommitsStream
    ignore_parent_replication_key = False
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    def post_process(self, row: dict, context: dict[str, str] | None = None) -> dict:
        row = super().post_process(row, context)
        if context is not None:
            # Get commit ID (sha) from context
            row["org"] = context["org"]
            row["repo"] = context["repo"]
            row["repo_id"] = context["repo_id"]
            row["pull_number"] = context["pull_number"]
            row["commit_id"] = context["commit_id"]
        return row

    schema = th.PropertiesList(
        # Parent keys
        th.Property("org", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("repo_id", th.IntegerType),
        th.Property("pull_number", th.IntegerType),
        th.Property("commit_id", th.StringType),
        # Rest
        th.Property("diff", th.StringType),
        th.Property("success", th.BooleanType),
        th.Property("error_message", th.StringType),
    ).to_dict()


class ReviewsStream(GitHubRestStream):
    name = "reviews"
    path = "/repos/{org}/{repo}/pulls/{pull_number}/reviews"
    primary_keys: ClassVar[list[str]] = ["id"]
    parent_stream_type = PullRequestsStream
    ignore_parent_replication_key = False
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    schema = th.PropertiesList(
        # Parent keys
        th.Property("pull_id", th.IntegerType),
        th.Property("pull_number", th.IntegerType),
        th.Property("org", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Rest
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("user", user_object),
        th.Property("body", th.StringType),
        th.Property("state", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("pull_request_url", th.StringType),
        th.Property(
            "_links",
            th.ObjectType(
                th.Property("html", th.ObjectType(th.Property("href", th.StringType))),
                th.Property(
                    "pull_request", th.ObjectType(th.Property("href", th.StringType))
                ),
            ),
        ),
        th.Property("submitted_at", th.DateTimeType),
        th.Property("commit_id", th.StringType),
        th.Property("author_association", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: dict[str, str] | None = None) -> dict:
        row = super().post_process(row, context)
        if context is not None:
            # Get PR ID from context
            row["org"] = context["org"]
            row["repo"] = context["repo"]
            row["repo_id"] = context["repo_id"]
            row["pull_number"] = context["pull_number"]
            row["pull_id"] = context["pull_id"]
        return row


class ReviewCommentsStream(GitHubRestStream):
    name = "review_comments"
    path = "/repos/{org}/{repo}/pulls/comments"
    primary_keys: ClassVar[list[str]] = ["id"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    schema = th.PropertiesList(
        # Parent keys
        th.Property("org", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Rest
        th.Property("url", th.StringType),
        th.Property("pull_request_review_id", th.IntegerType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("diff_hunk", th.StringType),
        th.Property("path", th.StringType),
        th.Property("position", th.IntegerType),
        th.Property("original_position", th.IntegerType),
        th.Property("commit_id", th.StringType),
        th.Property("original_commit_id", th.StringType),
        th.Property("in_reply_to_id", th.IntegerType),
        th.Property("user", user_object),
        th.Property("body", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("html_url", th.StringType),
        th.Property("pull_request_url", th.StringType),
        th.Property("author_association", th.StringType),
        th.Property(
            "_links",
            th.ObjectType(
                th.Property("self", th.ObjectType(th.Property("href", th.StringType))),
                th.Property("html", th.ObjectType(th.Property("href", th.StringType))),
                th.Property(
                    "pull_request", th.ObjectType(th.Property("href", th.StringType))
                ),
            ),
        ),
        th.Property("start_line", th.IntegerType),
        th.Property("original_start_line", th.IntegerType),
        th.Property("start_side", th.StringType),
        th.Property("line", th.IntegerType),
        th.Property("original_line", th.IntegerType),
        th.Property("side", th.StringType),
    ).to_dict()


class ContributorsStream(GitHubRestStream):
    """Defines 'Contributors' stream. Fetching User & Bot contributors."""

    name = "contributors"
    path = "/repos/{org}/{repo}/contributors"
    primary_keys: ClassVar[list[str]] = ["node_id", "repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    tolerated_http_errors: ClassVar[list[int]] = [204]

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # User/Bot contributor keys
        th.Property("login", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("avatar_url", th.StringType),
        th.Property("gravatar_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("type", th.StringType),
        th.Property("site_admin", th.BooleanType),
        th.Property("contributions", th.IntegerType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # TODO: update this and validate_response when
        # https://github.com/meltano/sdk/pull/1754 is merged
        if response.status_code != 200:
            return
        yield from super().parse_response(response)

    def validate_response(self, response: requests.Response) -> None:
        """Allow some specific errors."""
        if response.status_code == 403:
            contents = response.json()
            if (
                contents["message"]
                == "The history or contributor list is too large to list contributors for this repository via the API."  # noqa: E501
            ):
                self.logger.info(
                    "Skipping repo '%s'. The list of contributors is too large.",
                    self.name,
                )
                return
        super().validate_response(response)


class AnonymousContributorsStream(GitHubRestStream):
    """Defines 'AnonymousContributors' stream."""

    name = "anonymous_contributors"
    path = "/repos/{org}/{repo}/contributors"
    primary_keys: ClassVar[list[str]] = ["email", "repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    tolerated_http_errors: ClassVar[list[int]] = [204]

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        assert context is not None, f"Context cannot be empty for '{self.name}' stream."
        params = super().get_url_params(context, next_page_token)
        # Fetch all contributions, including anonymous users.
        params["anon"] = "true"
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of anonymous contributors."""
        parsed_response = super().parse_response(response)
        return filter(lambda x: x["type"] == "Anonymous", parsed_response)

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Anonymous contributor keys
        th.Property("email", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("contributions", th.IntegerType),
    ).to_dict()


class StargazersStream(GitHubRestStream):
    """Defines 'Stargazers' stream. Warning: this stream does NOT track star deletions."""  # noqa: E501

    name = "stargazers_rest"
    path = "/repos/{org}/{repo}/stargazers"
    primary_keys: ClassVar[list[str]] = ["user_id", "repo", "org"]
    parent_stream_type = RepositoryStream
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    replication_key = "starred_at"
    # GitHub is missing the "since" parameter on this endpoint.
    use_fake_since_parameter = True

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        super().__init__(*args, **kwargs)
        # TODO - remove warning with next release.
        self.logger.warning(
            "The stream 'stargazers_rest' is deprecated. Please use the Graphql version instead: 'stargazers'."  # noqa: E501
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Overridden to use an endpoint which includes starred_at property:
        https://docs.github.com/en/rest/reference/activity#custom-media-types-for-starring
        """
        headers = super().http_headers
        headers["Accept"] = "application/vnd.github.v3.star+json"
        return headers

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        """
        Add a user_id top-level field to be used as state replication key.
        """
        row = super().post_process(row, context)
        row["user_id"] = row["user"]["id"]
        return row

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        th.Property("user_id", th.IntegerType),
        # Stargazer Info
        th.Property("starred_at", th.DateTimeType),
        th.Property("user", user_object),
    ).to_dict()


class StargazersGraphqlStream(GitHubGraphqlStream):
    """Defines 'UserContributedToStream' stream. Warning: this stream 'only' gets the first 100 projects (by stars)."""  # noqa: E501

    name = "stargazers"
    query_jsonpath = "$.data.repository.stargazers.edges.[*]"
    primary_keys: ClassVar[list[str]] = ["user_id", "repo_id"]
    replication_key = "starred_at"
    parent_stream_type = RepositoryStream
    state_partitioning_keys: ClassVar[list[str]] = ["repo_id"]
    # The parent repository object changes if the number of stargazers changes.
    ignore_parent_replication_key = False

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        super().__init__(*args, **kwargs)
        # TODO - remove warning with next release.
        self.logger.warning(
            "The stream 'stargazers' might conflict with previous implementation. "
            "Looking for the older version? Use 'stargazers_rest'."
        )

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        """
        Add a user_id top-level field to be used as state replication key.
        """
        row = super().post_process(row, context)
        row["user_id"] = row["user"]["id"]
        return row

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,  # noqa: ANN401
    ) -> Any | None:  # noqa: ANN401
        """
        Exit early if a since parameter is provided.
        """
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

        # If since parameter is present, try to exit early by looking at the last "starred_at".  # noqa: E501
        # Noting that we are traversing in DESCENDING order by STARRED_AT.
        if since:
            results = list(extract_jsonpath(self.query_jsonpath, input=response.json()))
            # If no results, return None to exit early.
            if len(results) == 0:
                return None
            last = results[-1]
            if parse(last["starred_at"]) < parse(since):
                return None
        return super().get_next_page_token(response, previous_token)

    @property
    def query(self) -> str:
        """Return dynamic GraphQL query."""
        # Graphql id is equivalent to REST node_id. To keep the tap consistent, we rename "id" to "node_id".  # noqa: E501
        return """
          query repositoryStargazers($repo: String! $org: String! $nextPageCursor_0: String) {
            repository(name: $repo owner: $org) {
              stargazers(first: 100 orderBy: {field: STARRED_AT direction: DESC} after: $nextPageCursor_0) {
                pageInfo {
                  hasNextPage_0: hasNextPage
                  startCursor_0: startCursor
                  endCursor_0: endCursor
                }
                edges {
                  user: node {
                    node_id: id
                    id: databaseId
                    login
                    avatar_url: avatarUrl
                    html_url: url
                    type: __typename
                    site_admin: isSiteAdmin
                  }
                  starred_at: starredAt
                }
              }
            }
            rateLimit {
              cost
            }
          }
        """  # noqa: E501

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Stargazer Info
        th.Property("user_id", th.IntegerType),
        th.Property("starred_at", th.DateTimeType),
        th.Property("user", user_object),
    ).to_dict()


class StatsContributorsStream(GitHubRestStream):
    """
    Defines 'StatsContributors' stream. Fetching contributors activity.
    https://docs.github.com/en/rest/reference/metrics#get-all-contributor-commit-activity
    """

    name = "stats_contributors"
    path = "/repos/{org}/{repo}/stats/contributors"
    primary_keys: ClassVar[list[str]] = ["user_id", "week_start", "repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    # Note - these queries are expensive and the API might return an HTTP 202 if the response  # noqa: E501
    # has not been cached recently. https://docs.github.com/en/rest/reference/metrics#a-word-about-caching
    tolerated_http_errors: ClassVar[list[int]] = [202, 204]

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of flattened contributor activity."""  # noqa: E501
        replacement_keys = {
            "a": "additions",
            "c": "commits",
            "d": "deletions",
            "w": "week_start",
        }
        parsed_response = super().parse_response(response)
        for contributor_activity in parsed_response:
            weekly_data = contributor_activity["weeks"]
            for week in weekly_data:
                # no need to save weeks with no contributions or author.
                # if a user has deleted their account, GitHub may surprisingly return author: None.  # noqa: E501
                author = contributor_activity["author"]
                if (sum(week[key] for key in ["a", "c", "d"]) == 0) or (author is None):
                    continue
                week_with_author = {
                    replacement_keys.get(k, k): v for k, v in week.items()
                }
                week_with_author.update(author)
                week_with_author["user_id"] = week_with_author.pop("id")
                yield week_with_author

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Activity keys
        th.Property("week_start", th.IntegerType),
        th.Property("additions", th.IntegerType),
        th.Property("deletions", th.IntegerType),
        th.Property("commits", th.IntegerType),
        # Contributor keys
        th.Property("login", th.StringType),
        th.Property("user_id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("avatar_url", th.StringType),
        th.Property("gravatar_id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("site_admin", th.BooleanType),
    ).to_dict()


class WorkflowsStream(GitHubRestStream):
    """Defines 'workflows' stream."""

    MAX_PER_PAGE = 100

    name = "workflows"
    path = "/repos/{org}/{repo}/actions/workflows"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = None
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    records_jsonpath = "$.workflows[*]"

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # PR keys
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("path", th.StringType),
        th.Property("state", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("badge_url", th.StringType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())


class WorkflowRunsStream(GitHubRestStream):
    """Defines 'workflow_runs' stream."""

    name = "workflow_runs"
    path = "/repos/{org}/{repo}/actions/runs"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = None
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    records_jsonpath = "$.workflow_runs[*]"

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # PR keys
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("node_id", th.StringType),
        th.Property("head_branch", th.StringType),
        th.Property("head_sha", th.StringType),
        th.Property("run_number", th.IntegerType),
        th.Property("run_attempt", th.IntegerType),
        th.Property("event", th.StringType),
        th.Property("status", th.StringType),
        th.Property("conclusion", th.StringType),
        th.Property("workflow_id", th.IntegerType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property(
            "pull_requests",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("number", th.IntegerType),
                )
            ),
        ),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("jobs_url", th.StringType),
        th.Property("logs_url", th.StringType),
        th.Property("check_suite_url", th.StringType),
        th.Property("artifacts_url", th.StringType),
        th.Property("cancel_url", th.StringType),
        th.Property("rerun_url", th.StringType),
        th.Property("workflow_url", th.StringType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Return a child context object from the record and optional provided context.
        By default, will return context if provided and otherwise the record dict.
        Developers may override this behavior to send specific information to child
        streams for context.
        """
        return {
            "org": context["org"] if context else None,
            "repo": context["repo"] if context else None,
            "run_id": record["id"],
            "repo_id": context["repo_id"] if context else None,
        }


class WorkflowRunJobsStream(GitHubRestStream):
    """Defines 'workflow_run_jobs' stream."""

    MAX_PER_PAGE = 100

    name = "workflow_run_jobs"
    path = "/repos/{org}/{repo}/actions/runs/{run_id}/jobs"
    primary_keys: ClassVar[list[str]] = ["id"]
    parent_stream_type = WorkflowRunsStream
    ignore_parent_replication_key = False
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org", "run_id"]
    records_jsonpath = "$.jobs[*]"

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # PR keys
        th.Property("id", th.IntegerType),
        th.Property("run_id", th.IntegerType),
        th.Property("run_url", th.StringType),
        th.Property("node_id", th.StringType),
        th.Property("head_sha", th.StringType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("status", th.StringType),
        th.Property("conclusion", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("started_at", th.DateTimeType),
        th.Property("completed_at", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property(
            "steps",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("status", th.StringType),
                    th.Property("conclusion", th.StringType),
                    th.Property("number", th.IntegerType),
                    th.Property("started_at", th.DateTimeType),
                    th.Property("completed_at", th.DateTimeType),
                )
            ),
        ),
        th.Property("check_run_url", th.StringType),
        th.Property("labels", th.ArrayType(th.StringType)),
        th.Property("runner_id", th.IntegerType),
        th.Property("runner_name", th.StringType),
        th.Property("runner_group_id", th.IntegerType),
        th.Property("runner_group_name", th.StringType),
    ).to_dict()

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        super().__init__(*args, **kwargs)
        self._schema_emitted = False

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["filter"] = "all"
        return params

    def _write_schema_message(self) -> None:
        """Write out a SCHEMA message with the stream schema."""
        if not self._schema_emitted:
            super()._write_schema_message()
            self._schema_emitted = True


class ExtraMetricsStream(GitHubRestStream):
    """Defines the 'extra_metrics' stream."""

    name = "extra_metrics"
    path = "/{org}/{repo}/"
    primary_keys: ClassVar[list[str]] = ["repo_id"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo_id"]

    @property
    def url_base(self) -> str:
        return self.config.get("api_url_base", self.DEFAULT_API_BASE_URL).replace(
            "api.", ""
        )

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the repository main page to extract extra metrics."""
        yield from scrape_metrics(response, self.logger)

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row = super().post_process(row, context)
        if context is not None:
            row["repo"] = context["repo"]
            row["org"] = context["org"]
        return row

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Mock a web browser user-agent.
        """
        return {"User-agent": "Mozilla/5.0"}

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Metric keys
        th.Property("open_issues", th.IntegerType),
        th.Property("open_prs", th.IntegerType),
        th.Property("dependents", th.IntegerType),
        th.Property("contributors", th.IntegerType),
        th.Property("fetched_at", th.DateTimeType),
    ).to_dict()


class DependentsStream(GitHubRestStream):
    """Defines 'dependents' stream.

    This stream scrapes the website instead of using the API.
    """

    name = "dependents"
    path = "/{org}/{repo}/network/dependents"
    primary_keys: ClassVar[list[str]] = ["repo_id", "dependent_name_with_owner"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo_id"]

    @property
    def url_base(self) -> str:
        return self.config.get("api_url_base", self.DEFAULT_API_BASE_URL).replace(
            "api.", ""
        )

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Get the response for the first page and scrape results, potentially iterating through pages."""  # noqa: E501
        yield from scrape_dependents(response, self.logger)

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        new_row = {"dependent": row}
        new_row = super().post_process(new_row, context)
        # we extract dependent_name_with_owner to be able to use it safely as a primary key,  # noqa: E501
        # regardless of the target used.
        new_row["dependent_name_with_owner"] = row["name_with_owner"]
        return new_row

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Mock a web browser user-agent.
        """
        return {"User-Agent": "Mozilla/5.0"}

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Dependent keys
        th.Property("dependent_name_with_owner", th.StringType),
        th.Property(
            "dependent",
            th.ObjectType(
                th.Property("name_with_owner", th.StringType),
                th.Property("stars", th.IntegerType),
                th.Property("forks", th.IntegerType),
            ),
        ),
    ).to_dict()


class DependenciesStream(GitHubGraphqlStream):
    """Defines 'DependenciesStream' stream."""

    name = "dependencies"
    query_jsonpath = (
        "$.data.repository.dependencyGraphManifests.nodes.[*].dependencies.nodes.[*]"
    )
    # github's api sometimes returns multiple versions of a same package as dependencies
    # of a given repo. We use package_name instead of dependency_repo_id because
    # the latter changes as github's resolution improves, which would lead to invalid
    # duplicate values
    primary_keys: ClassVar[list[str]] = [
        "repo_id",
        "package_name",
        "package_manager",
        "requirements",
    ]
    parent_stream_type = RepositoryStream
    state_partitioning_keys: ClassVar[list[str]] = ["repo_id"]
    ignore_parent_replication_key = True

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Overridden to use the preview for Repository.dependencyGraphManifests.
        """
        headers = super().http_headers
        headers["Accept"] = "application/vnd.github.hawkgirl-preview+json"
        return headers

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        """
        Add a dependency_repo_id top-level field to be used as primary key.
        """
        row = super().post_process(row, context)
        row["dependency_repo_id"] = (
            row["dependency"]["id"] if row["dependency"] else None
        )
        if context is not None:
            row["org"] = context["org"]
            row["repo"] = context["repo"]
        return row

    @property
    def query(self) -> str:
        """Return dynamic GraphQL query."""
        # Graphql id is equivalent to REST node_id. To keep the tap consistent, we rename "id" to "node_id".  # noqa: E501
        # Due to GrapQl nested-pagination limitations, we loop through the top level dependencyGraphManifests one by one.  # noqa: E501
        return """
          query repositoryDependencies($repo: String! $org: String! $nextPageCursor_0: String $nextPageCursor_1: String) {
            repository(name: $repo owner: $org) {
              dependencyGraphManifests (first: 1 withDependencies: true after: $nextPageCursor_0) {
                totalCount
                pageInfo {
                  hasNextPage_0: hasNextPage
                  startCursor_0: startCursor
                  endCursor_0: endCursor
                }
                nodes {
                  filename
                  dependenciesCount
                  dependencies (first: 50 after: $nextPageCursor_1) {
                    pageInfo {
                      hasNextPage_1: hasNextPage
                      startCursor_1: startCursor
                      endCursor_1: endCursor
                    }
                    nodes {
                      dependency: repository {
                        node_id: id
                        id: databaseId
                        name_with_owner: nameWithOwner
                        url
                        owner {
                          node_id: id
                          login
                        }
                      }
                      package_manager: packageManager
                      package_name: packageName
                      requirements
                      has_dependencies: hasDependencies
                    }
                  }
                }
              }
            }
            rateLimit {
              cost
            }
          }

        """  # noqa: E501

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Dependency Info
        th.Property("dependency_repo_id", th.IntegerType),
        th.Property("package_name", th.StringType),
        th.Property("package_manager", th.StringType),
        th.Property("requirements", th.StringType),
        th.Property("has_dependencies", th.BooleanType),
        th.Property(
            "dependency",
            th.ObjectType(
                th.Property("node_id", th.StringType),
                th.Property("id", th.IntegerType),
                th.Property("name_with_owner", th.StringType),
                th.Property("url", th.StringType),
                th.Property(
                    "owner",
                    th.ObjectType(
                        th.Property("node_id", th.StringType),
                        th.Property("login", th.StringType),
                    ),
                ),
            ),
        ),
    ).to_dict()


class TrafficRestStream(GitHubRestStream):
    """Base class for Traffic Streams"""

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if response.status_code != 200:
            return

        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def validate_response(self, response: requests.Response) -> None:
        """Allow some specific errors.
        Do not raise exceptions if the error says "Must have push access to repository"
        as we actually expect these in this stream when we don't have write permissions into it.
        """  # noqa: E501
        if response.status_code == 403:
            contents = response.json()
            if contents["message"] == "Resource not accessible by integration":
                self.logger.info("Permissions missing to sync stream '%s'", self.name)
                return
        super().validate_response(response)


class TrafficClonesStream(TrafficRestStream):
    """Defines 'traffic_clones' stream."""

    name = "traffic_clones"
    path = "/repos/{org}/{repo}/traffic/clones"
    primary_keys: ClassVar[list[str]] = ["repo", "org", "timestamp"]
    replication_key = "timestamp"
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    records_jsonpath = "$.clones[*]"
    selected_by_default = False

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Clones Data
        th.Property("timestamp", th.DateTimeType),
        th.Property("count", th.IntegerType),
        th.Property("uniques", th.IntegerType),
    ).to_dict()


class TrafficReferralPathsStream(TrafficRestStream):
    """Defines 'traffic_referral_paths' stream."""

    name = "traffic_referral_paths"
    path = "/repos/{org}/{repo}/traffic/popular/paths"
    primary_keys: ClassVar[list[str]] = ["repo", "org", "path"]
    replication_key = None
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    records_jsonpath = "[*]"
    selected_by_default = False

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Referral path data
        th.Property("path", th.StringType),
        th.Property("title", th.StringType),
        th.Property("count", th.IntegerType),
        th.Property("uniques", th.IntegerType),
    ).to_dict()


class TrafficReferrersStream(TrafficRestStream):
    """Defines 'traffic_referrers' stream."""

    name = "traffic_referrers"
    path = "/repos/{org}/{repo}/traffic/popular/referrers"
    primary_keys: ClassVar[list[str]] = ["repo", "org", "referrer"]
    replication_key = None
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    records_jsonpath = "[*]"
    selected_by_default = False

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Referrer data
        th.Property("referrer", th.StringType),
        th.Property("count", th.IntegerType),
        th.Property("uniques", th.IntegerType),
    ).to_dict()


class TrafficPageViewsStream(TrafficRestStream):
    """Defines 'traffic_pageviews' stream."""

    name = "traffic_pageviews"
    path = "/repos/{org}/{repo}/traffic/views"
    primary_keys: ClassVar[list[str]] = ["repo", "org", "timestamp"]
    replication_key = None
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    records_jsonpath = "$.views[*]"
    selected_by_default = False

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Page view data
        th.Property("timestamp", th.DateTimeType),
        th.Property("count", th.IntegerType),
        th.Property("uniques", th.IntegerType),
    ).to_dict()


class BranchesStream(GitHubRestStream):
    """A stream dedicated to fetching the branches of a repository."""

    name = "branches"
    path = "/repos/{org}/{repo}/branches"
    primary_keys: ClassVar[list[str]] = ["repo", "org", "name"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]
    tolerated_http_errors: ClassVar[list[int]] = [404]

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Branch Keys
        th.Property("name", th.StringType),
        th.Property(
            "commit",
            th.ObjectType(
                th.Property("sha", th.StringType),
                th.Property("url", th.StringType),
            ),
        ),
        th.Property("protected", th.BooleanType),
        th.Property(
            "protection",
            th.ObjectType(
                th.Property(
                    "required_status_checks",
                    th.ObjectType(
                        th.Property("enforcement_level", th.StringType),
                        th.Property("contexts", th.ArrayType(th.StringType)),
                    ),
                ),
            ),
        ),
        th.Property("protection_url", th.StringType),
    ).to_dict()


class TagsStream(GitHubRestStream):
    """A stream dedicated to fetching tags in a repository."""

    name = "tags"
    path = "/repos/{org}/{repo}/tags"
    primary_keys: ClassVar[list[str]] = ["repo", "org", "name"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Tag Keys
        th.Property("name", th.StringType),
        th.Property(
            "commit",
            th.ObjectType(
                th.Property("sha", th.StringType),
                th.Property("url", th.StringType),
            ),
        ),
        th.Property("zipball_url", th.StringType),
        th.Property("tarball_url", th.StringType),
        th.Property("node_id", th.StringType),
    ).to_dict()


class DeploymentsStream(GitHubRestStream):
    """A stream dedicated to fetching deployments in a repository."""

    name = "deployments"
    path = "/repos/{org}/{repo}/deployments"
    primary_keys: ClassVar[list[str]] = ["node_id"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org"]

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        # Deployment Keys
        th.Property("url", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("sha", th.StringType),
        th.Property("ref", th.StringType),
        th.Property("task", th.StringType),
        th.Property("payload", th.StringType),
        th.Property("original_environment", th.StringType),
        th.Property("environment", th.StringType),
        th.Property("description", th.StringType),
        th.Property(
            "creator",
            th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("id", th.IntegerType),
                th.Property("node_id", th.StringType),
                th.Property("avatar_url", th.StringType),
                th.Property("gravatar_id", th.StringType),
                th.Property("url", th.StringType),
                th.Property("html_url", th.StringType),
                th.Property("followers_url", th.StringType),
                th.Property("following_url", th.StringType),
                th.Property("gists_url", th.StringType),
                th.Property("starred_url", th.StringType),
                th.Property("subscriptions_url", th.StringType),
                th.Property("organizations_url", th.StringType),
                th.Property("repos_url", th.StringType),
                th.Property("events_url", th.StringType),
                th.Property("received_events_url", th.StringType),
                th.Property("type", th.StringType),
                th.Property("site_admin", th.BooleanType),
            ),
        ),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("statuses_url", th.StringType),
        th.Property("repository_url", th.StringType),
        th.Property("transient_environment", th.BooleanType),
        th.Property("production_environment", th.BooleanType),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Return a child context object from the record and optional provided context.
        By default, will return context if provided and otherwise the record dict.
        Developers may override this behavior to send specific information to child
        streams for context.
        """
        return {
            "org": context["org"] if context else None,
            "repo": context["repo"] if context else None,
            "deployment_id": record["id"],
            "repo_id": context["repo_id"] if context else None,
        }


class DeploymentStatusesStream(GitHubRestStream):
    """A stream dedicated to fetching deployment statuses in a repository."""

    name = "deployment_statuses"
    path = "/repos/{org}/{repo}/deployments/{deployment_id}/statuses"
    primary_keys: ClassVar[list[str]] = ["node_id"]
    parent_stream_type = DeploymentsStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["repo", "org", "deployment_id"]
    tolerated_http_errors: ClassVar[list[int]] = [404]

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("repo_id", th.IntegerType),
        th.Property("deployment_id", th.IntegerType),
        # Deployment Status Keys
        th.Property("url", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("state", th.StringType),
        th.Property(
            "creator",
            th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("id", th.IntegerType),
                th.Property("node_id", th.StringType),
                th.Property("avatar_url", th.StringType),
                th.Property("gravatar_id", th.StringType),
                th.Property("url", th.StringType),
                th.Property("html_url", th.StringType),
                th.Property("followers_url", th.StringType),
                th.Property("following_url", th.StringType),
                th.Property("gists_url", th.StringType),
                th.Property("starred_url", th.StringType),
                th.Property("subscriptions_url", th.StringType),
                th.Property("organizations_url", th.StringType),
                th.Property("repos_url", th.StringType),
                th.Property("events_url", th.StringType),
                th.Property("received_events_url", th.StringType),
                th.Property("type", th.StringType),
                th.Property("site_admin", th.BooleanType),
            ),
        ),
        th.Property("description", th.StringType),
        th.Property("environment", th.StringType),
        th.Property("target_url", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("deployment_url", th.StringType),
        th.Property("repository_url", th.StringType),
        th.Property("environment_url", th.StringType),
        th.Property("log_url", th.StringType),
    ).to_dict()
