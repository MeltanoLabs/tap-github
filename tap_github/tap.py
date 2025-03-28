"""GitHub tap class."""

from __future__ import annotations

import logging
import os

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers._classproperty import classproperty

from tap_github.streams import Streams


class TapGitHub(Tap):
    """Singer tap for the GitHub API."""

    name = "tap-github"
    package_name = "meltanolabs-tap-github"

    @classproperty
    def logger(cls: type[TapGitHub]) -> logging.Logger:  # noqa: N805
        """Get logger.

        Returns:
            Logger with local LOGLEVEL. LOGLEVEL from env takes priority.
        """

        LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()  # noqa: N806
        assert LOGLEVEL in logging._levelToName.values(), (
            f"Invalid LOGLEVEL configuration: {LOGLEVEL}"
        )
        logger = logging.getLogger(cls.name)
        logger.setLevel(LOGLEVEL)
        return logger

    config_jsonschema = th.PropertiesList(
        th.Property(
            "user_agent",
            th.StringType,
            description="User agent to use for API requests.",
        ),
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
            description="List of GitHub tokens to authenticate with. Streams will loop through them when hitting rate limits.",  # noqa: E501
        ),
        th.Property(
            "auth_app_keys",
            th.ArrayType(th.StringType),
            description=(
                "List of GitHub App credentials to authenticate with. Each credential "
                "can be constructed by combining an App ID and App private key into "
                "the format `:app_id:;;-----BEGIN RSA PRIVATE KEY-----\n_YOUR_P_KEY_\n-----END RSA PRIVATE KEY-----`."  # noqa: E501
            ),
        ),
        th.Property(
            "rate_limit_buffer",
            th.IntegerType,
            description="Add a buffer to avoid consuming all query points for the token at hand. Defaults to 1000.",  # noqa: E501
        ),
        th.Property(
            "expiry_time_buffer",
            th.IntegerType,
            description=(
                "When authenticating as a GitHub App, this buffer controls how many "
                "minutes before expiry the GitHub app tokens will be refreshed. "
                "Defaults to 10 minutes.",
            ),
        ),
        th.Property(
            "searches",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property("query", th.StringType, required=True),
                )
            ),
            description="List of searches to perform.",
        ),
        th.Property("organizations", th.ArrayType(th.StringType)),
        th.Property("repositories", th.ArrayType(th.StringType)),
        th.Property("user_usernames", th.ArrayType(th.StringType)),
        th.Property("user_ids", th.ArrayType(th.StringType)),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Start date for incremental sync.",
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_config", th.ObjectType()),
        th.Property(
            "skip_parent_streams",
            th.BooleanType,
            description=(
                "Set to true to skip API calls for the parent "
                "streams (such as repositories) if it is not selected but children are"
            ),
        ),
        th.Property(
            "stream_options",
            th.ObjectType(
                th.Property(
                    "milestones",
                    th.ObjectType(
                        th.Property(
                            "state",
                            th.StringType,
                            description=(
                                "Configures which states are of interest. "
                                "Must be one of [open, closed, all], defaults to open."
                            ),
                            default="open",
                            allowed_values=["open", "closed", "all"],
                        ),
                        additional_properties=False,
                    ),
                    description="Options specific to the 'milestones' stream.",
                ),
                additional_properties=False,
            ),
            description="Options which change the behaviour of a specific stream.",
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams for each query."""

        # If the config is empty, assume we are running --help or --capabilities.
        if (
            self.config
            and len(Streams.all_valid_queries().intersection(self.config)) != 1
        ):
            raise ValueError(
                "This tap requires one and only one of the following path options: "
                f"{Streams.all_valid_queries()}."
            )
        streams = []
        for stream_type in Streams:
            if (not self.config) or len(
                stream_type.valid_queries.intersection(self.config)
            ) > 0:
                streams += [
                    StreamClass(tap=self) for StreamClass in stream_type.streams
                ]

        if not streams:
            raise ValueError("No valid streams found.")
        return streams


# CLI Execution:

cli = TapGitHub.cli
