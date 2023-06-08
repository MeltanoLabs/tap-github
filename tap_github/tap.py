"""GitHub tap class."""

import logging
import os
from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers._classproperty import classproperty

from tap_github.streams import Streams


class TapGitHub(Tap):
    """GitHub tap class."""

    name = "tap-github"

    @classproperty
    def logger(cls) -> logging.Logger:
        """Get logger.

        Returns:
            Logger with local LOGLEVEL. LOGLEVEL from env takes priority.
        """

        LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
        assert (
            LOGLEVEL in logging._levelToName.values()
        ), f"Invalid LOGLEVEL configuration: {LOGLEVEL}"
        logger = logging.getLogger(cls.name)
        logger.setLevel(LOGLEVEL)
        return logger

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
            th.IntegerType,
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
        th.Property(
            "skip_parent_streams",
            th.BooleanType,
            description=(
                "Set to true to skip API calls for the parent "
                "streams (such as repositories) if it is not selected but children are"
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams for each query."""

        specified_options = {key:value for key, value in self.config.items() if not value is None}

        # If the config is empty, assume we are running --help or --capabilities.
        if (
            self.config
            and len(Streams.all_valid_queries().intersection(specified_options)) != 1
        ):
            raise ValueError(
                "This tap requires one and only one of the following path options: "
                f"{Streams.all_valid_queries()}."
            )
        streams = []
        for stream_type in Streams:
            if (not specified_options) or len(
                stream_type.valid_queries.intersection(specified_options)
            ) > 0:
                streams += [
                    StreamClass(tap=self) for StreamClass in stream_type.streams
                ]

        if not streams:
            raise ValueError("No valid streams found.")
        return streams


# CLI Execution:

cli = TapGitHub.cli
