"""GitHub tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_github.repository_streams import (
    CommitsStream,
    CommunityProfileStream,
    IssueCommentsStream,
    IssueEventsStream,
    IssuesStream,
    PullRequestsStream,
    ReadmeStream,
    RepositoryStream,
)

from tap_github.user_streams import (
    UserStream,
)


class TapGitHub(Tap):
    """GitHub tap class."""

    name = "tap-github"

    config_jsonschema = th.PropertiesList(
        th.Property("user_agent", th.StringType),
        th.Property("metrics_log_level", th.StringType),
        th.Property("auth_token", th.StringType),
        th.Property(
            "searches",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property("query", th.StringType, required=True),
                )
            ),
        ),
        th.Property("repositories", th.ArrayType(th.StringType)),
        th.Property("user_usernames", th.ArrayType(th.StringType)),
        th.Property("user_ids", th.ArrayType(th.StringType)),
        th.Property("start_date", th.DateTimeType),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_config", th.ObjectType()),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams for each query."""
        VALID_USER_QUERIES = {"user_usernames", "user_ids"}
        VALID_REPO_QUERIES = {"repositories", "organizations", "searches"}
        VALID_QUERIES = VALID_REPO_QUERIES.union(VALID_USER_QUERIES)

        if len(VALID_QUERIES.intersection(self.config)) != 1:
            raise ValueError(
                "This tap requires one and only one of the following path options: "
                f"{VALID_QUERIES}."
            )
        is_user_query = len(VALID_USER_QUERIES.intersection(self.config)) > 0
        if is_user_query:
            return [UserStream(tap=self)]
        else:
            return [
                CommitsStream(tap=self),
                CommunityProfileStream(tap=self),
                IssueCommentsStream(tap=self),
                IssueEventsStream(tap=self),
                IssuesStream(tap=self),
                PullRequestsStream(tap=self),
                ReadmeStream(tap=self),
                RepositoryStream(tap=self),
            ]


# CLI Execution:

cli = TapGitHub.cli
