"""User Stream types classes for tap-github."""

from typing import Any, Dict, List, Optional
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_github.client import GitHubStream


class UserStream(GitHubStream):
    """Defines 'User' stream."""

    name = "users"

    @property
    def path(self) -> str:  # type: ignore
        """Return the API endpoint path."""
        if "user_usernames" in self.config:
            return "/users/{username}"
        elif "user_ids" in self.config:
            return "/user/{id}"

    @property
    def partitions(self) -> Optional[List[Dict]]:
        """Return a list of partitions."""
        if "user_usernames" in self.config:
            return [{"username": u} for u in self.config["user_usernames"]]
        elif "user_ids" in self.config:
            return [{"id": id} for id in self.config["user_ids"]]
        return None

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.
        Developers may override this behavior to send specific information to child
        streams for context.
        """
        return {
            "username": record["login"],
        }

    schema = th.PropertiesList(
        th.Property("login", th.IntegerType),
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
