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
    def logger(cls) -> logging.Logger:  # noqa: N805
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
            default=200,
            description="Add a buffer to avoid consuming all query points for the token at hand. Defaults to 200 for aggressive performance.",  # noqa: E501
        ),
        th.Property(
            "expiry_time_buffer",
            th.IntegerType,
            description=(
                "When authenticating as a GitHub App, this buffer controls how many "
                "minutes before expiry the GitHub app tokens will be refreshed. "
                "Defaults to 10 minutes."
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
            description=(
                "An array of search descriptor objects with the following properties:\n"
                '"name" - a human readable name for the search query.\n'
                '"query" -  a github search string (generally the same as would come after ?q= in the URL)"'  # noqa: E501
            ),
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
        th.Property(
            "search_count_queries",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property("query", th.StringType, required=True),
                    th.Property(
                        "type",
                        th.StringType,
                        allowed_values=["issue", "pr"],
                        default="issue",
                    ),
                    th.Property("month", th.StringType),
                )
            ),
            description=(
                "Array of search count query objects for statistics collection:\n"
                '"name" - human readable identifier\n'
                '"query" - GitHub search syntax (e.g., "org:Automattic type:issue state:open")\n'
                '"type" - either "issue" or "pr"\n'
                '"month" - optional month filter in YYYY-MM format'
            ),
        ),
        th.Property(
            "github_instances",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property("api_url_base", th.StringType, required=True),
                    th.Property("auth_token", th.StringType, required=True),
                )
            ),
            description=(
                "Array of GitHub instance configurations for multi-instance support:\n"
                '"name" - instance identifier (e.g., "github.com", "github.example.com")\n'
                '"api_url_base" - base API URL (e.g., "https://api.github.com")\n'
                '"auth_token" - authentication token for this instance'
            ),
        ),
        th.Property(
            "search_orgs",
            th.ArrayType(th.StringType),
            description="List of GitHub organization names for programmatic search count generation",
        ),
        th.Property(
            "date_range",
            th.ObjectType(
                th.Property("start", th.StringType, required=True),
                th.Property("end", th.StringType, required=False),
            ),
            description=(
                "Date range for programmatic query generation:\n"
                '"start" - start date in YYYY-MM-DD format\n'
                '"end" - end date in YYYY-MM-DD format (optional, defaults to last complete month)'
            ),
        ),
        th.Property(
            "search_streams",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property("query_template", th.StringType, required=True),
                    th.Property("count_field", th.StringType, required=True),
                    th.Property("description", th.StringType),
                )
            ),
            description=(
                "Array of search stream definitions:\n"
                '"name" - unique stream name (will be suffixed with _search_counts)\n'
                '"query_template" - GitHub search query with placeholders {org}, {start}, {end}\n'
                '"count_field" - name of the count field in output (e.g., issue_count)\n'
                '"description" - optional description for the stream'
            ),
        ),
        th.Property(
            "search_scope",
            th.ObjectType(
                th.Property(
                    "instances",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("instance", th.StringType, required=True),
                            th.Property("api_url_base", th.StringType, required=True),
                            th.Property("auth_token", th.StringType),
                            th.Property(
                                "streams",
                                th.ArrayType(th.StringType),
                                description="List of stream names this instance supports",
                            ),
                            th.Property(
                                "org",
                                th.ArrayType(th.StringType),
                                description="Organizations for org-level aggregated queries",
                            ),
                            th.Property(
                                "repo_level",
                                th.ArrayType(th.StringType),
                                description="List of specific repositories (format: org/repo) for repo-level queries",
                            ),
                            th.Property("repo_breakdown", th.BooleanType, default=False),
                            th.Property("max_partitions", th.IntegerType, default=5000),
                            th.Property("partition_warning_threshold", th.IntegerType, default=2000),
                            th.Property("enforce_partition_limit", th.BooleanType, default=False),
                            th.Property("repo_discovery_cache_ttl", th.IntegerType, default=60),
                        )
                    ),
                    description="List of GitHub instances with their specific org/repo configurations and supported streams",
                ),
            ),
            description=(
                "Search scope configuration for multi-instance GitHub environments:\n"
                '"instances" - array of GitHub instances with instance->org->repo mapping and stream support'
            ),
        ),
        # Performance and validation configuration for search count streams
        th.Property(
            "enforce_lookback_limit",
            th.BooleanType,
            default=False,
            description="Enforce maximum lookback period for date ranges (default: warn only)",
        ),
        th.Property(
            "max_lookback_years",
            th.IntegerType,
            default=1,
            description="Maximum lookback period in years for date range validation",
        ),
        th.Property(
            "max_partitions",
            th.IntegerType,
            default=5000,
            description="Maximum number of partitions allowed for search count streams (increased for performance)",
        ),
        th.Property(
            "partition_warning_threshold",
            th.IntegerType,
            default=500,
            description="Partition count threshold for performance warnings",
        ),
        th.Property(
            "enforce_partition_limit",
            th.BooleanType,
            default=False,
            description="Enforce maximum partition limit (disabled by default for performance)",
        ),
        th.Property(
            "repo_discovery_cache_ttl",
            th.IntegerType,
            default=60,
            description="Cache TTL in minutes for repository discovery results",
        ),
        th.Property(
            "batch_query_size",
            th.IntegerType,
            default=10,
            description="Number of search queries to batch in a single GraphQL request for performance",
        ),
        th.Property(
            "max_concurrent_requests",
            th.IntegerType,
            default=4,
            description="Maximum number of concurrent API requests per token",
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        streams = []
        
        # Use configurable streams for both search_streams and search_scope
        if self.config and ("search_streams" in self.config or "search_scope" in self.config):
            from tap_github.search_count_streams import create_configurable_streams
            return create_configurable_streams(self)
        
        if not self.config:
            return streams
            
        if len(Streams.all_valid_queries().intersection(self.config)) != 1:
            raise ValueError(
                "This tap requires one and only one of the following path options: "
                f"{Streams.all_valid_queries()}, provided config: {self.config}"
            )
            
        for stream_type in Streams:
            if len(stream_type.valid_queries.intersection(self.config)) > 0:
                streams += [
                    StreamClass(tap=self) for StreamClass in stream_type.streams
                ]

        if not streams:
            raise ValueError("No valid streams found.")
        return streams


# CLI Execution:

cli = TapGitHub.cli
