"""GitHub tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_github.repository_streams import (
    AnonymousContributorsStream,
    CommitsStream,
    CommunityProfileStream,
    ContributorsStream,
    EventsStream,
    IssueCommentsStream,
    IssueEventsStream,
    IssuesStream,
    LanguagesStream,
    PullRequestsStream,
    ReadmeHtmlStream,
    ReadmeStream,
    RepositoryStream,
    StargazersStream,
    StatsContributorsStream,
)
from tap_github.user_streams import (
    StarredStream,
    UserStream,
)


class TapGitHub(Tap):
    """GitHub tap class."""

    name = "tap-github"

    config_jsonschema = th.PropertiesList(
        th.Property("user_agent", th.StringType),
        th.Property("metrics_log_level", th.StringType),
        # Authentication options
        th.Property(
            "auth_token",
            th.StringType,
            description="GitHub token to authenticate with.",
        ),
        th.Property(
            "additional_auth_tokens",
            th.ArrayType(th.StringType),
            description="List of GitHub tokens to authenticate with. Streams will loop through them when hitting rate limits.",
        ),
        th.Property(
            "rate_limit_buffer",
            th.ArrayType(th.IntegerType),
            description="Add a buffer to avoid consuming all query points for the token at hand. Defaults to 1000.",
        ),
        th.Property(
            "searches",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property("query", th.StringType, required=True),
                )
            ),
        ),
        th.Property("organizations", th.ArrayType(th.StringType)),
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
            return [StarredStream(tap=self), UserStream(tap=self)]
        else:
            return [
                AnonymousContributorsStream(tap=self),
                CommitsStream(tap=self),
                CommunityProfileStream(tap=self),
                ContributorsStream(tap=self),
                EventsStream(tap=self),
                IssueCommentsStream(tap=self),
                IssueEventsStream(tap=self),
                IssuesStream(tap=self),
                LanguagesStream(tap=self),
                PullRequestsStream(tap=self),
                ReadmeHtmlStream(tap=self),
                ReadmeStream(tap=self),
                RepositoryStream(tap=self),
                StargazersStream(tap=self),
                StatsContributorsStream(tap=self),
            ]


# CLI Execution:

cli = TapGitHub.cli
