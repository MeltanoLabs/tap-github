"""Stream type classes for tap-github."""

from typing import Any, Dict, Optional

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_github.client import GitHubStream


class RepositoryStream(GitHubStream):
    """Defines 'Repository' stream."""

    # Search API max: 100 per page, 1,000 total
    MAX_PER_PAGE = 100
    MAX_RESULTS_LIMIT = 1000

    def __init__(
        self,
        tap,
        name: Optional[str] = None,
        schema=None,
        path: Optional[str] = None,
        query: str = None,
    ):
        super().__init__(tap=tap, name=name, schema=schema, path=path)
        self.query = query

    def get_url_params(
        self, partition: Optional[dict], next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(
            partition=partition, next_page_token=next_page_token
        )
        params["q"] = self.query
        return params

    def get_child_context(self, record: dict, context: dict = None) -> Optional[Dict]:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.
        Developers may override this behavior to send specific information to child
        streams for context.
        """

        return {
            "org": record["owner"]["login"],
            "repo": record["name"],
        }

    path = "/search/repositories"
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("full_name", th.StringType),
        th.Property("description", th.StringType),
        th.Property(
            "owner",
            th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("type", th.StringType),
                th.Property("avatar_url", th.StringType),
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
        th.Property("private", th.BooleanType),
        th.Property("size", th.IntegerType),
        th.Property("stargazers_count", th.IntegerType),
        th.Property("fork", th.BooleanType),
        # These `_count` metrics appear to be duplicates, or else
        # documentation bugs in results example:
        # https://docs.github.com/en/rest/reference/search
        th.Property("forks", th.IntegerType),
        th.Property("forks_count", th.IntegerType),
        th.Property("watchers", th.IntegerType),
        th.Property("watchers_count", th.IntegerType),
        th.Property("open_issues", th.IntegerType),
        th.Property("open_issues_count", th.IntegerType),
    ).to_dict()


class IssuesStream(GitHubStream):
    """Defines 'Issues' stream."""

    parent_stream_type = RepositoryStream
    name = "Issues"
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
    ).to_dict()

    path = "/repos/{org}/{repo}/issues"

    # def get_path(self, partition: dict) -> str:
    #     """TODO: This needs to be called instead of `path`."""
    #     return f"/{partition['org']}/{partition['repo']}/issues"
