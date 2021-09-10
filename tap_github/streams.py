"""Stream type classes for tap-github."""

import requests
from typing import Any, Dict, Iterable, List, Optional

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_github.client import GitHubStream


class RepositoryStream(GitHubStream):
    """Defines 'Repository' stream."""

    # Search API max: 100 per page, 1,000 total
    MAX_PER_PAGE = 100
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
        """Return the API endpoint path."""
        if "searches" in self.config:
            return "/search/repositories"
        else:
            # the `repo` and `org` args will be parsed from the partition's `context`
            return "/repos/{org}/{repo}"

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
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """
        Parse the response which differs for this stream depending on which mode it is run in.
        """
        if "searches" in self.config:
            return super(GitHubStream, self).parse_response(response)
        else:
            return [response.json()]

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
        th.Property(
            "owner",
            th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("id", th.IntegerType),
                th.Property("node_id", th.StringType),
                th.Property("type", th.StringType),
                th.Property("avatar_url", th.StringType),
                th.Property("html_url", th.StringType),
                th.Property("site_admin", th.StringType),
            ),
        ),
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
                th.Property("site_admin", th.StringType),
            ),
        ),
    ).to_dict()


class IssuesStream(GitHubStream):
    """Defines 'Issues' stream."""

    name = "issues"
    path = "/repos/{org}/{repo}/issues"
    primary_keys = ["id"]
    replication_key = "updated_at"
    parent_stream_type = RepositoryStream
    ignore_parent_replication_key = False
    state_partitioning_keys = ["repo", "org"]

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.
        Developers may override this behavior to send specific information to child
        streams for context.
        """
        if context is None:
            raise ValueError("Issue stream should not have blank context.")

        context["issue_number"] = record["number"]
        context["comments"] = record["comments"]  # If zero, comments can be skipped
        return context

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Overridden to use beta endpoint which includes reactions as described here:
        https://developer.github.com/changes/2016-05-12-reactions-api-preview/
        """
        headers = super().http_headers
        headers["Accept"] = "application/vnd.github.squirrel-girl-preview"
        return headers

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("issue_number", th.IntegerType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        # th.Property("closed_at", th.DateTimeType),  # Nulls causing parse error
        th.Property("state", th.StringType),
        th.Property("title", th.StringType),
        th.Property("comments", th.IntegerType),
        th.Property("author_association", th.StringType),
        th.Property("body", th.StringType),
    ).to_dict()


class IssueCommentsStream(GitHubStream):
    """Defines 'Issues' stream."""

    name = "issue_comments"
    path = "/repos/{org}/{repo}/issues/{issue_number}/comments"
    primary_keys = ["id"]
    replication_key = "updated_at"
    parent_stream_type = IssuesStream
    state_partitioning_keys = ["repo", "org"]
    ignore_parent_replication_key = False

    def get_records(self, context: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        if context and context.get("comments", None) == 0:
            self.logger.debug(f"No comments detected. Skipping '{self.name}' sync.")
            return []

        return super().get_records(context)

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        if self.replication_key:
            since = self.get_starting_timestamp(context)
            if since:
                params["since"] = since
        return params

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("issue_number", th.IntegerType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("author_association", th.StringType),
        th.Property("body", th.StringType),
    ).to_dict()
