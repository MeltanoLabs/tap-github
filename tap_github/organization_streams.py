"""User Stream types classes for tap-github."""

from typing import Dict, List, Optional, Iterable, Any

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_github.client import GitHubRestStream


class OrganizationStream(GitHubRestStream):
    name = "organizations"

    @property
    def path(self) -> str:  # type: ignore
        """Return the API endpoint path."""
        return "/orgs/{org}"

    @property
    def partitions(self) -> Optional[List[Dict]]:
        """Return a list of partitions."""
        return [{"org": org} for org in self.config["organizations"]]

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a child context object from the record and optional provided context.

        By default, will return context if provided and otherwise the record dict.
        Developers may override this behavior to send specific information to child
        streams for context.
        """
        return {
            "org": record["login"],
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
                "org": context["org"],
            }
        else:
            yield from super().get_records(context)

    schema = th.PropertiesList(
        th.Property("login", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("repos_url", th.StringType),
        th.Property("events_url", th.StringType),
        th.Property("hooks_url", th.StringType),
        th.Property("issues_url", th.StringType),
        th.Property("members_url", th.StringType),
        th.Property("public_members_url", th.StringType),
        th.Property("avatar_url", th.StringType),
        th.Property("description", th.StringType),
    ).to_dict()
