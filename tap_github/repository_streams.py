"""Repository Stream types classes for tap-github."""

from typing import Any, Dict, Iterable, List, Optional

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_github.client import GitHubRestStream
from tap_github.schema_objects import (
    user_object,
    label_object,
    reactions_object,
    milestone_object,
)


class RepositoryStream(GitHubRestStream):
    """Defines 'Repository' stream."""

    # Search API max: 1,000 total.
    MAX_RESULTS_LIMIT = 1000

    name = "repositories"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
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

    @property
    def partitions(self) -> Optional[List[Dict]]:
        """Return a list of partitions."""
        if "searches" in self.config:
            return [
                {"search_name": s["name"], "search_query": s["query"]}
                for s in self.config["searches"]
            ]
        if "repositories" in self.config:
            split_repo_names = map(lambda s: s.split("/"), self.config["repositories"])
            return [{"org": r[0], "repo": r[1]} for r in split_repo_names]
        if "organizations" in self.config:
            return [{"org": org} for org in self.config["organizations"]]
        return None

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.
        Developers may override this behavior to send specific information to child
        streams for context.
        """
        return {
            "org": record["owner"]["login"],
            "repo": record["name"],
        }

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
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
            yield {
                "owner": {
                    "login": context["org"],
                },
                "name": context["repo"],
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
        th.Property(
            "organization",
            th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("id", th.IntegerType),
                th.Property("node_id", th.StringType),
                th.Property("avatar_url", th.StringType),
                th.Property("gravatar_id", th.StringType),
                th.Property("url", th.StringType),
                th.Property("html_url", th.StringType),
                th.Property("type", th.StringType),
                th.Property("site_admin", th.BooleanType),
            ),
        ),
    ).to_dict()


class ReadmeStream(GitHubRestStream):
    """
    A stream dedicated to fetching the object version of a README.md.

    Inclduding its content, base64 encoded of the readme in GitHub flavored Markdown.
    For html, see ReadmeHtmlStream.
    """

    name = "readme"
    path = "/repos/{org}/{repo}/readme"
    primary_keys = ["repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org"]
    tolerated_http_errors = [404]

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
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
    primary_keys = ["repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org"]
    tolerated_http_errors = [404]

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
            return []

        yield {"raw_html": response.text}

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        # Readme HTML
        th.Property("raw_html", th.StringType),
    ).to_dict()


class CommunityProfileStream(GitHubRestStream):
    """Defines 'CommunityProfile' stream."""

    name = "community_profile"
    path = "/repos/{org}/{repo}/community/profile"
    primary_keys = ["repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org"]
    tolerated_http_errors = [404]

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
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
    primary_keys = ["id"]
    replication_key = "created_at"
    parent_stream_type = RepositoryStream
    state_partitioning_keys = ["repo", "org"]
    ignore_parent_replication_key = False
    # GitHub is missing the "since" parameter on this endpoint.
    missing_since_parameter = True

    def get_records(self, context: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.
        Each row emitted should be a dictionary of property names to their values.
        """
        if context and context.get("events", None) == 0:
            self.logger.debug(f"No events detected. Skipping '{self.name}' sync.")
            return []

        return super().get_records(context)

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        # TODO - We should think about the best approach to handle this. An alternative would be to
        # do a 'dumb' tap that just keeps the same schemas as GitHub without renaming these
        # objects to "target_". They are worth keeping, however, as they can be different from
        # the parent stream, e.g. for fork/parent PR events.
        row["target_repo"] = row.pop("repo", None)
        row["target_org"] = row.pop("org", None)
        return row

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
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
                th.Property("id", th.StringType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "target_org",
            th.ObjectType(
                th.Property("id", th.StringType),
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


class LanguagesStream(GitHubRestStream):
    name = "languages"
    path = "/repos/{org}/{repo}/languages"
    primary_keys = ["repo", "org", "language_name"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org"]

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the language response and reformat to return as an iterator of [{language_name: Python, bytes: 23}]."""
        if response.status_code in self.tolerated_http_errors:
            return []

        languages_json = response.json()
        for key, value in languages_json.items():
            yield {"language_name": key, "bytes": value}

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        # A list of languages parsed by GitHub is available here:
        # https://github.com/github/linguist/blob/master/lib/linguist/languages.yml
        th.Property("language_name", th.StringType),
        th.Property("bytes", th.IntegerType),
    ).to_dict()


class IssuesStream(GitHubRestStream):
    """Defines 'Issues' stream which returns Issues and PRs following GitHub's API convention."""

    name = "issues"
    path = "/repos/{org}/{repo}/issues"
    primary_keys = ["id"]
    replication_key = "updated_at"
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org"]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        assert context is not None, f"Context cannot be empty for '{self.name}' stream."
        params = super().get_url_params(context, next_page_token)
        # Fetch all issues and PRs, regardless of state (OPEN, CLOSED, MERGED).
        # To exclude PRs from the issues stream, you can use the Stream Maps in the config.
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

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row["type"] = "pull_request" if "pull_request" in row else "issue"
        if row["body"] is not None:
            # some issue bodies include control characters such as \x00
            # that some targets (such as postgresql) choke on. This ensures
            # such chars are removed from the data before we pass it on to
            # the target
            row["body"] = row["body"].replace("\x00", "")

        # replace +1/-1 emojis to avoid downstream column name errors.
        row["plus_one"] = row.pop("+1", None)
        row["minus_one"] = row.pop("-1", None)
        return row

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
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
    primary_keys = ["id"]
    replication_key = "updated_at"
    parent_stream_type = RepositoryStream
    state_partitioning_keys = ["repo", "org"]
    ignore_parent_replication_key = False
    # FIXME: this allows the tap to continue on server-side timeouts but means
    # we have gaps in our data
    tolerated_http_errors = [502]

    def get_records(self, context: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        if context and context.get("comments", None) == 0:
            self.logger.debug(f"No comments detected. Skipping '{self.name}' sync.")
            return []

        return super().get_records(context)

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row["issue_number"] = int(row["issue_url"].split("/")[-1])
        if row["body"] is not None:
            # some comment bodies include control characters such as \x00
            # that some targets (such as postgresql) choke on. This ensures
            # such chars are removed from the data before we pass it on to
            # the target
            row["body"] = row["body"].replace("\x00", "")
        return row

    schema = th.PropertiesList(
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
    primary_keys = ["id"]
    replication_key = "created_at"
    parent_stream_type = RepositoryStream
    state_partitioning_keys = ["repo", "org"]
    ignore_parent_replication_key = False
    # GitHub is missing the "since" parameter on this endpoint.
    missing_since_parameter = True

    def get_records(self, context: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        if context and context.get("events", None) == 0:
            self.logger.debug(f"No events detected. Skipping '{self.name}' sync.")
            return []

        return super().get_records(context)

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row["issue_number"] = int(row["issue"].pop("number"))
        row["issue_url"] = row["issue"].pop("url")
        return row

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
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
    primary_keys = ["node_id"]
    replication_key = "commit_timestamp"
    parent_stream_type = RepositoryStream
    state_partitioning_keys = ["repo", "org"]
    ignore_parent_replication_key = True

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """
        Add a timestamp top-level field to be used as state replication key.
        It's not clear from github's API docs which time (author or committer)
        is used to compare to the `since` argument that the endpoint supports.
        """
        row["commit_timestamp"] = row["commit"]["committer"]["date"]
        return row

    schema = th.PropertiesList(
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


class PullRequestsStream(GitHubRestStream):
    """Defines 'PullRequests' stream."""

    name = "pull_requests"
    path = "/repos/{org}/{repo}/pulls"
    primary_keys = ["id"]
    replication_key = "updated_at"
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org"]
    # GitHub is missing the "since" parameter on this endpoint.
    missing_since_parameter = True

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
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

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        if row["body"] is not None:
            # some pr bodies include control characters such as \x00
            # that some targets (such as postgresql) choke on. This ensures
            # such chars are removed from the data before we pass it on to
            # the target
            row["body"] = row["body"].replace("\x00", "")

        # replace +1/-1 emojis to avoid downstream column name errors.
        row["plus_one"] = row.pop("+1", None)
        row["minus_one"] = row.pop("-1", None)
        return row

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
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


class ContributorsStream(GitHubRestStream):
    """Defines 'Contributors' stream. Fetching User & Bot contributors."""

    name = "contributors"
    path = "/repos/{org}/{repo}/contributors"
    primary_keys = ["node_id", "repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org"]

    schema = th.PropertiesList(
        # Parent keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        # User/Bot contributor keys
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
        th.Property("contributions", th.IntegerType),
    ).to_dict()


class AnonymousContributorsStream(GitHubRestStream):
    """Defines 'AnonymousContributors' stream."""

    name = "anonymous_contributors"
    path = "/repos/{org}/{repo}/contributors"
    primary_keys = ["email", "repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org"]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
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
        # Anonymous contributor keys
        th.Property("email", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("contributions", th.IntegerType),
    ).to_dict()


class StargazersStream(GitHubRestStream):
    """Defines 'Stargazers' stream. Warning: this stream does NOT track star deletions."""

    name = "stargazers"
    path = "/repos/{org}/{repo}/stargazers"
    primary_keys = ["user_id", "repo", "org"]
    parent_stream_type = RepositoryStream
    state_partitioning_keys = ["repo", "org"]
    replication_key = "starred_at"
    # GitHub is missing the "since" parameter on this endpoint.
    missing_since_parameter = True

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Overridden to use an endpoint which includes starred_at property:
        https://docs.github.com/en/rest/reference/activity#custom-media-types-for-starring
        """
        headers = super().http_headers
        headers["Accept"] = "application/vnd.github.v3.star+json"
        return headers

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """
        Add a user_id top-level field to be used as state replication key.
        """
        row["user_id"] = row["user"]["id"]
        return row

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("user_id", th.IntegerType),
        # Stargazer Info
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
    primary_keys = ["user_id", "week_start", "repo", "org"]
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org"]
    # Note - these queries are expensive and the API might return an HTTP 202 if the response
    # has not been cached recently. https://docs.github.com/en/rest/reference/metrics#a-word-about-caching
    tolerated_http_errors = [202]

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of flattened contributor activity."""
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
                # if a user has deleted their account, GitHub may surprisingly return author: None.
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
