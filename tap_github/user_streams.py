"""User Stream types classes for tap-github."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import FatalAPIError

from tap_github.client import GitHubGraphqlStream, GitHubRestStream
from tap_github.schema_objects import user_object

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.tap_base import Tap


class UserStream(GitHubRestStream):
    """Defines 'User' stream."""

    name = "users"
    replication_key = "updated_at"

    @property
    def path(self) -> str:  # type: ignore
        """Return the API endpoint path."""
        if "user_usernames" in self.config:
            return "/users/{username}"
        elif "user_ids" in self.config:
            return "/user/{id}"

    @property
    def partitions(self) -> list[dict] | None:
        """Return a list of partitions."""
        if "user_usernames" in self.config:
            input_user_list = self.config["user_usernames"]

            augmented_user_list = []
            # chunk requests to the graphql endpoint to avoid timeouts and other
            # obscure errors that the api doesn't say much about. The actual limit
            # seems closer to 1000, use half that to stay safe.
            chunk_size = 500
            list_length = len(input_user_list)
            self.logger.info(f"Filtering user list of {list_length} users")
            for ndx in range(0, list_length, chunk_size):
                augmented_user_list += self.get_user_ids(
                    input_user_list[ndx : ndx + chunk_size]
                )
            self.logger.info(f"Running the tap on {len(augmented_user_list)} users")
            return augmented_user_list

        elif "user_ids" in self.config:
            return [{"id": user_id} for user_id in self.config["user_ids"]]
        return None

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            "username": record["login"],
            "user_id": record["id"],
        }

    def get_user_ids(self, user_list: list[str]) -> list[dict[str, str]]:
        """Enrich the list of userse with their numeric ID from github.

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

            def __init__(self, tap: Tap, user_list: list[str]) -> None:
                super().__init__(tap)
                self.user_list = user_list

            @property
            def query(self) -> str:
                chunks = []
                for i, user in enumerate(self.user_list):
                    # we use the `repositoryOwner` query which is the only one that
                    # works on both users and orgs with graphql. REST is less picky
                    # and the /user endpoint works for all types.
                    chunks.append(
                        f'user{i}: repositoryOwner(login: "{user}") '
                        "{ login avatarUrl}"
                    )
                return "query {" + " ".join(chunks) + " rateLimit { cost } }"

        if len(user_list) < 1:
            return []

        users_with_ids: list = []
        temp_stream = TempStream(self._tap, list(user_list))

        database_id_pattern: re.Pattern = re.compile(
            r"https://avatars.githubusercontent.com/u/(\d+)?.*"
        )
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
                    username = record[item]["login"]
                except TypeError:
                    # one of the usernames returned `None`, which means it does
                    # not exist, log some details, and move on to the next one
                    invalid_username = user_list[int(item[4:])]
                    self.logger.info(
                        f"Username not found: {invalid_username} \t"
                        "Removing it from list"
                    )
                    continue
                # the databaseId (in graphql language) is not available on
                # repositoryOwner, so we parse the avatarUrl to get it :/
                m = database_id_pattern.match(record[item]["avatarUrl"])
                if m is not None:
                    db_id = m.group(1)
                    users_with_ids.append({"username": username, "user_id": db_id})
                else:
                    # If we get here, github's API is not returning what
                    # we expected, so it's most likely a breaking change on
                    # their end, and the tap's code needs updating
                    raise FatalAPIError("Unexpected GitHub API error: Breaking change?")

        self.logger.info(f"Running the tap on {len(users_with_ids)} users")
        return users_with_ids

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
            # the id is fetched in `get_user_ids` above
            yield {
                "login": context["username"],
                "id": context["user_id"],
            }
        else:
            yield from super().get_records(context)

    schema = th.PropertiesList(
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
        th.Property("name", th.StringType),
        th.Property("company", th.StringType),
        th.Property("blog", th.StringType),
        th.Property("location", th.StringType),
        th.Property("email", th.StringType),
        th.Property("hireable", th.BooleanType),
        th.Property("bio", th.StringType),
        th.Property("twitter_username", th.StringType),
        th.Property("public_repos", th.IntegerType),
        th.Property("public_gists", th.IntegerType),
        th.Property("followers", th.IntegerType),
        th.Property("following", th.IntegerType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
    ).to_dict()


class StarredStream(GitHubRestStream):
    """Defines 'Stars' stream. Warning: this stream does NOT track star deletions."""

    name = "starred"
    path = "/users/{username}/starred"
    # "repo_id" is the starred repo's id.
    primary_keys: ClassVar[list[str]] = ["repo_id", "username"]
    parent_stream_type = UserStream
    # TODO - change partitioning key to user_id?
    state_partitioning_keys: ClassVar[list[str]] = ["username"]
    replication_key = "starred_at"
    ignore_parent_replication_key = True
    # GitHub is missing the "since" parameter on this endpoint.
    use_fake_since_parameter = True

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
        Add a repo_id top-level field to be used as state replication key.
        """
        row["repo_id"] = row["repo"]["id"]
        if context is not None:
            row["user_id"] = context["user_id"]
        return row

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("username", th.StringType),
        th.Property("repo_id", th.IntegerType),
        th.Property("user_id", th.IntegerType),
        # Starred Repo Info
        th.Property("starred_at", th.DateTimeType),
        th.Property(
            "repo",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("node_id", th.StringType),
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
                th.Property("updated_at", th.DateTimeType),
                th.Property("created_at", th.DateTimeType),
                th.Property("pushed_at", th.DateTimeType),
                th.Property("stargazers_count", th.IntegerType),
                th.Property("fork", th.BooleanType),
                th.Property(
                    "topics",
                    th.ArrayType(th.StringType),
                ),
                th.Property("visibility", th.StringType),
                th.Property("language", th.StringType),
                th.Property("forks", th.IntegerType),
                th.Property("watchers", th.IntegerType),
                th.Property("open_issues", th.IntegerType),
            ),
        ),
    ).to_dict()


class UserContributedToStream(GitHubGraphqlStream):
    """Defines 'UserContributedToStream' stream."""

    name = "user_contributed_to"
    query_jsonpath = "$.data.user.repositoriesContributedTo.nodes.[*]"
    primary_keys: ClassVar[list[str]] = ["username", "name_with_owner"]
    replication_key = None
    parent_stream_type = UserStream
    # TODO - add user_id to schema
    # TODO - change partitioning key to user_id?
    state_partitioning_keys: ClassVar[list[str]] = ["username"]
    ignore_parent_replication_key = True

    @property
    def query(self) -> str:
        """Return dynamic GraphQL query."""
        # Graphql id is equivalent to REST node_id. To keep the tap consistent,
        # we rename "id" to "node_id".
        return """
          query userContributedTo($username: String! $nextPageCursor_0: String) {
            user (login: $username) {
              repositoriesContributedTo (first: 100 after: $nextPageCursor_0 includeUserRepositories: true orderBy: {field: STARGAZERS, direction: DESC}) {
                pageInfo {
                  hasNextPage_0: hasNextPage
                  startCursor_0: startCursor
                  endCursor_0: endCursor
                }
                nodes {
                  node_id: id
                  database_id: databaseId
                  name_with_owner: nameWithOwner
                  open_graph_image_url: openGraphImageUrl
                  stargazer_count: stargazerCount
                  pushed_at: pushedAt
                  owner {
                    node_id: id
                    login
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
        th.Property("node_id", th.StringType),
        th.Property("username", th.StringType),
        th.Property("name_with_owner", th.StringType),
        th.Property("open_graph_image_url", th.StringType),
        th.Property("stargazer_count", th.IntegerType),
        th.Property(
            "owner",
            th.ObjectType(
                th.Property("node_id", th.StringType),
                th.Property("login", th.StringType),
            ),
        ),
    ).to_dict()
