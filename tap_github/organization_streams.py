"""User Stream types classes for tap-github."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import FatalAPIError

from tap_github.client import GitHubGraphqlStream, GitHubRestStream

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context


# Reusable GraphQL fragment for Actor fields
# https://docs.github.com/en/graphql/reference/interfaces#actor
ACTOR_FRAGMENT = """
    login
    resource_path: resourcePath
    url
    type: __typename
    ... on Bot {
      node_id: id
      id: databaseId
    }
    ... on User {
      node_id: id
      id: databaseId
    }
    ... on Organization {
      node_id: id
      id: databaseId
    }
    ... on Mannequin {
      node_id: id
      id: databaseId
    }
    ... on EnterpriseUserAccount {
      node_id: id
    }
"""


class OrganizationStream(GitHubRestStream):
    """Defines a GitHub Organization Stream.
    API Reference: https://docs.github.com/en/rest/reference/orgs#get-an-organization
    """

    name = "organizations"
    path = "/orgs/{org}"

    @property
    def partitions(self) -> list[dict] | None:
        return [{"org": org} for org in self.config["organizations"]]

    def get_child_context(self, record: dict, context: Context | None) -> dict:
        return {
            "org": record["login"],
        }

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
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


class OrganizationMembersStream(GitHubRestStream):
    """
    API Reference: https://docs.github.com/en/rest/orgs/members?apiVersion=2022-11-28#list-organization-members
    """

    name = "organization_members"
    primary_keys: ClassVar[list[str]] = ["id"]
    path = "/orgs/{org}/members"
    ignore_parent_replication_key = True
    parent_stream_type = OrganizationStream
    state_partitioning_keys: ClassVar[list[str]] = ["org"]
    schema = th.PropertiesList(
        # Parent keys
        th.Property("org", th.StringType),
        # Rest
        th.Property("login", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("avatar_url", th.StringType),
        th.Property("gravatar_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("type", th.StringType),
        th.Property("site_admin", th.BooleanType),
    ).to_dict()


class TeamsStream(GitHubRestStream):
    """
    API Reference: https://docs.github.com/en/rest/reference/teams#list-teams
    """

    name = "teams"
    primary_keys: ClassVar[list[str]] = ["id"]
    path = "/orgs/{org}/teams"
    ignore_parent_replication_key = True
    parent_stream_type = OrganizationStream
    state_partitioning_keys: ClassVar[list[str]] = ["org"]

    def get_child_context(self, record: dict, context: Context | None) -> dict:
        new_context = {"team_slug": record["slug"]}
        if context:
            return {
                **context,
                **new_context,
            }
        return new_context

    schema = th.PropertiesList(
        # Parent Keys
        th.Property("org", th.StringType),
        # Rest
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("name", th.StringType),
        th.Property("slug", th.StringType),
        th.Property("description", th.StringType),
        th.Property("privacy", th.StringType),
        th.Property("permission", th.StringType),
        th.Property("members_url", th.StringType),
        th.Property("repositories_url", th.StringType),
        th.Property(
            "parent",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("node_id", th.StringType),
                th.Property("url", th.StringType),
                th.Property("html_url", th.StringType),
                th.Property("name", th.StringType),
                th.Property("slug", th.StringType),
                th.Property("description", th.StringType),
                th.Property("privacy", th.StringType),
                th.Property("permission", th.StringType),
                th.Property("members_url", th.StringType),
                th.Property("repositories_url", th.StringType),
            ),
        ),
    ).to_dict()


class TeamMembersStream(GitHubRestStream):
    """
    API Reference: https://docs.github.com/en/rest/reference/teams#list-team-members
    """

    name = "team_members"
    primary_keys: ClassVar[list[str]] = ["id", "team_slug"]
    path = "/orgs/{org}/teams/{team_slug}/members"
    ignore_parent_replication_key = True
    parent_stream_type = TeamsStream
    state_partitioning_keys: ClassVar[list[str]] = ["team_slug", "org"]

    def get_child_context(self, record: dict, context: Context | None) -> dict:
        new_context = {"username": record["login"]}
        if context:
            return {
                **context,
                **new_context,
            }
        return new_context

    schema = th.PropertiesList(
        # Parent keys
        th.Property("org", th.StringType),
        th.Property("team_slug", th.StringType),
        # Rest
        th.Property("login", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("avatar_url", th.StringType),
        th.Property("gravatar_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("type", th.StringType),
        th.Property("site_admin", th.BooleanType),
    ).to_dict()


class TeamRolesStream(GitHubRestStream):
    """
    API Reference: https://docs.github.com/en/rest/reference/teams#get-team-membership-for-a-user
    """

    name = "team_roles"
    path = "/orgs/{org}/teams/{team_slug}/memberships/{username}"
    ignore_parent_replication_key = True
    primary_keys: ClassVar[list[str]] = ["url"]
    parent_stream_type = TeamMembersStream
    state_partitioning_keys: ClassVar[list[str]] = ["username", "team_slug", "org"]

    schema = th.PropertiesList(
        # Parent keys
        th.Property("org", th.StringType),
        th.Property("team_slug", th.StringType),
        th.Property("username", th.StringType),
        # Rest
        th.Property("url", th.StringType),
        th.Property("role", th.StringType),
        th.Property("state", th.StringType),
    ).to_dict()


class ProjectsStream(GitHubGraphqlStream):
    """Fetches GitHub projects (new projects aka ProjectsV2) for an organization.

    API Reference: https://docs.github.com/en/graphql/reference/objects#projectv2
    """

    name = "projects"
    primary_keys: ClassVar[list[str]] = ["org", "id"]
    parent_stream_type = OrganizationStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["org"]
    query_jsonpath = "$.data.organization.projectsV2.nodes[*]"

    @property
    def query(self) -> str:
        """GraphQL query to fetch projects."""
        return f"""
        query OrganizationProjects($org: String!, $nextPageCursor_0: String) {{
          organization(login: $org) {{
            projectsV2(first: 100, after: $nextPageCursor_0) {{
              nodes {{
                closed
                closed_at: closedAt
                created_at: createdAt
                creator {{
                  {ACTOR_FRAGMENT}
                }}
                id: fullDatabaseId
                node_id: id
                number
                owner {{
                  node_id: id
                  type: __typename
                }}
                public
                readme
                resource_path: resourcePath
                short_description: shortDescription
                template
                title
                updated_at: updatedAt
                url
                viewer_can_close: viewerCanClose
                viewer_can_reopen: viewerCanReopen
                viewer_can_update: viewerCanUpdate
              }}
              pageInfo {{
                hasNextPage_0: hasNextPage
                endCursor_0: endCursor
                startCursor_0: startCursor
              }}
              totalCount
            }}
          }}
          rateLimit {{
            cost
          }}
        }}
        """

    def get_child_context(self, record: dict, context: Context | None) -> dict:
        """Return context for child streams."""
        new_context = {"project_number": record["number"]}
        if context:
            return {**context, **new_context}
        return new_context

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        """Post-process a fetched record."""
        row = super().post_process(row, context)
        if context:
            row["org"] = context["org"]
        return row

    schema = th.PropertiesList(
        # Parent keys
        th.Property("org", th.StringType),
        # Project fields
        th.Property(
            "id", th.StringType, nullable=False
        ),  # using fullDatabaseId from GraphQL as id, but is nullable in GraphQL
        th.Property(
            "node_id", th.StringType
        ),  # using id from GraphQL as node_id, it is required (ID!)
        th.Property("number", th.IntegerType),
        th.Property("title", th.StringType),
        th.Property("url", th.StringType),
        th.Property("resource_path", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("closed", th.BooleanType),
        th.Property(
            "closed_at", th.DateTimeType, required=False
        ),  # closedAt is nullable in GraphQL
        th.Property("public", th.BooleanType),
        th.Property(
            "readme", th.StringType, required=False
        ),  # readme is nullable in GraphQL
        th.Property(
            "short_description", th.StringType, required=False
        ),  # shortDescription is nullable in GraphQL
        th.Property("template", th.BooleanType),
        th.Property("viewer_can_close", th.BooleanType),
        th.Property("viewer_can_reopen", th.BooleanType),
        th.Property("viewer_can_update", th.BooleanType),
        th.Property(
            "owner",
            th.ObjectType(
                th.Property("node_id", th.StringType),
                th.Property("type", th.StringType),
            ),
        ),
        th.Property(
            "creator",
            th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("resource_path", th.StringType),
                th.Property("url", th.StringType),
                th.Property("type", th.StringType),
                th.Property("node_id", th.StringType),
                th.Property("id", th.StringType, required=False),
            ),
            required=False,  # creator is nullable in GraphQL
        ),
    ).to_dict()


class ProjectFieldConfigurationsStream(GitHubGraphqlStream):
    """Fetches all fields defined within a GitHub organization's project and outputs
    a single record per project containing all its fields.

    This stream is a child of ProjectsStream. For each project, it retrieves all its
    field configurations, including configurations for iteration and single-select
    fields, and consolidates them into one record.

    API Reference: https://docs.github.com/en/graphql/reference/objects#projectv2fieldconfiguration
    """

    name = "project_field_configurations"
    primary_keys: ClassVar[list[str]] = ["org", "project_number"]
    parent_stream_type = ProjectsStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["org", "project_number"]
    query_jsonpath = "$.data.organization.projectV2.fields.nodes[*]"

    @property
    def query(self) -> str:
        """GraphQL query to fetch a page of project fields."""
        return """
        query ProjectFieldsPage(
            $org: String!,
            $project_number: Int!,
            $nextPageCursor_0: String
        ) {
          organization(login: $org) {
            projectV2(number: $project_number) {
              fields(first: 100, after: $nextPageCursor_0) {
                nodes {
                  ... on ProjectV2Field {
                    id: databaseId
                    node_id: id
                    name
                    data_type: dataType
                    created_at: createdAt
                    updated_at: updatedAt
                  }
                  ... on ProjectV2IterationField {
                    id: databaseId
                    node_id: id
                    name
                    data_type: dataType
                    created_at: createdAt
                    updated_at: updatedAt
                    configuration {
                      duration
                      start_day: startDay
                      iterations {
                        id
                        title
                        start_date: startDate
                        duration
                      }
                      completed_iterations: completedIterations {
                        id
                        title
                        start_date: startDate
                        duration
                      }
                    }
                  }
                  ... on ProjectV2SingleSelectField {
                    id: databaseId
                    node_id: id
                    name
                    data_type: dataType
                    created_at: createdAt
                    updated_at: updatedAt
                    options {
                      id
                      name
                      color
                      description
                    }
                  }
                }
                pageInfo {
                  hasNextPage_0: hasNextPage
                  endCursor_0: endCursor
                }
                totalCount
              }
            }
          }
          rateLimit {
            cost
          }
        }
        """

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """
        Fetch all fields for a project, handling pagination, and yield a single record.
        """
        if not context:
            self.logger.warning("Received no context, skipping.")
            return

        org = context.get("org")
        project_number = context.get("project_number")

        if not org or project_number is None:
            self.logger.warning(f"Missing org or project_number in context: {context}")
            return

        all_field_configurations: list[dict] = []
        next_page_token: Any = None

        # Can't use BaseAPIPaginator - here we need to aggregate all pages of
        # fields of a project into one record, while BaseAPIPaginator yields
        # records incrementally as pages are fetched.
        while True:
            prepared_request = self.prepare_request(
                context=context, next_page_token=next_page_token
            )
            resp = self._request(prepared_request, context)
            page_fields = list(self.parse_response(resp))
            all_field_configurations.extend(page_fields)

            current_page_info = (
                resp.json()
                .get("data", {})
                .get("organization", {})
                .get("projectV2", {})
                .get("fields", {})
                .get("pageInfo", {})
            )
            if current_page_info.get("hasNextPage_0"):
                next_page_token = {
                    "nextPageCursor_0": current_page_info.get("endCursor_0")
                }
            else:
                break

        yield {
            "org": org,
            "project_number": project_number,
            "all_field_configurations": all_field_configurations,
        }

    def get_child_context(self, record: dict, context: Context | None) -> dict:
        """Return context for child streams."""
        child_context = dict(context or {})  # Includes org, project_number
        child_context["project_field_configurations"] = record.get(
            "all_field_configurations", []
        )

        return child_context

    schema = th.PropertiesList(
        th.Property("org", th.StringType),
        th.Property("project_number", th.IntegerType),
        th.Property(
            "all_field_configurations",
            th.ArrayType(
                th.ObjectType(
                    # Schema for a single field definition
                    th.Property(
                        "id", th.StringType, nullable=False
                    ),  # using databaseId from GraphQL as id, nullable in GraphQL
                    th.Property(
                        "node_id", th.StringType
                    ),  # using id from GraphQL as node_id, it is required (ID!)
                    th.Property("name", th.StringType),
                    th.Property("data_type", th.StringType),
                    th.Property("created_at", th.DateTimeType),
                    th.Property("updated_at", th.DateTimeType),
                    th.Property(
                        "configuration",
                        th.ObjectType(
                            th.Property("duration", th.IntegerType),
                            th.Property("start_day", th.IntegerType),
                            th.Property(
                                "iterations",
                                th.ArrayType(
                                    th.ObjectType(
                                        th.Property("id", th.StringType),
                                        th.Property("title", th.StringType),
                                        th.Property("start_date", th.DateType),
                                        th.Property("duration", th.IntegerType),
                                    )
                                ),
                            ),
                            th.Property(
                                "completed_iterations",
                                th.ArrayType(
                                    th.ObjectType(
                                        th.Property("id", th.StringType),
                                        th.Property("title", th.StringType),
                                        th.Property("start_date", th.DateType),
                                        th.Property("duration", th.IntegerType),
                                    )
                                ),
                            ),
                        ),
                        required=False,  # Only present for ProjectV2IterationField
                    ),
                    th.Property(
                        "options",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("id", th.StringType),
                                th.Property("name", th.StringType),
                                th.Property("color", th.StringType),
                                th.Property("description", th.StringType),
                            )
                        ),
                        required=False,  # Only present for ProjectV2SingleSelectField
                    ),
                )
            ),
        ),
    ).to_dict()


class ProjectItemsStream(GitHubGraphqlStream):
    """Fetches items for a project and their field values.

    This stream is a child of ProjectFieldConfigurationsStream. For each project,
    it fetches all items and then for each item, it queries the values of all
    known fields.

    API Reference: https://docs.github.com/en/graphql/reference/objects#projectv2item
    """

    name = "project_items"
    primary_keys: ClassVar[list[str]] = ["org", "project_number", "node_id"]
    parent_stream_type = ProjectFieldConfigurationsStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["org", "project_number"]
    query_jsonpath = "$.data.organization.projectV2.items.nodes[*]"

    # To store field configurations from context for query and post_process
    _current_project_field_configurations: ClassVar[list[dict]] = []

    # Project's custom fields supports types: Text, Number, Date, SingleSelect,
    # Iteration, so we fetch values from the corresponding types.
    #
    # Note: Other types are available in issues/pull requests so not included.
    #   - ProjectV2ItemFieldRepositoryValue,
    #   - ProjectV2ItemFieldUserValue,
    #   - ProjectV2ItemFieldLabelValue,
    #   - ProjectV2ItemFieldReviewerValue,
    #   - ProjectV2ItemFieldPullRequestValue,
    #   - ProjectV2ItemFieldMilestoneValue
    _supported_project_item_field_value_types: ClassVar[tuple[str, ...]] = (
        "ProjectV2ItemFieldTextValue",
        "ProjectV2ItemFieldDateValue",
        "ProjectV2ItemFieldNumberValue",
        "ProjectV2ItemFieldSingleSelectValue",
        "ProjectV2ItemFieldIterationValue",
    )

    # These fields are automatically created by GitHub and expected to present in
    # the project items.
    _common_fields: ClassVar[dict[str, dict[str, str]]] = {
        "Title": {"column": "title", "type": "ProjectV2ItemFieldTextValue"},
        "Status": {"column": "status", "type": "ProjectV2ItemFieldSingleSelectValue"},
    }

    def request_records(self, context: Context | None) -> Iterable[dict]:
        """Request records from the API, handling FORBIDDEN errors gracefully.

        TODO: should rewrite to use validate_response once
        https://github.com/meltano/sdk/issues/280 is implemented.
        """
        try:
            yield from super().request_records(context)
        except FatalAPIError as e:
            # Check if the error is FORBIDDEN. This error is raised when
            # the organization has security settings that block access to
            # the nodes of an item of a project, e.g. allowed IP list.
            error_message = str(e.args[0]) if e.args else ""
            if "FORBIDDEN" in error_message:
                self.logger.warning(
                    f"Skipping project due to FORBIDDEN error. "
                    f"Context: {context}. Error: {e}"
                )
                return

            raise

    def _escape_graphql_string(self, value: str) -> str:
        """
        Escape special characters in a string for use in GraphQL queries.
        """
        # Escape backslashes first, then quotes
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def _generate_gql_alias(self, field_name: str) -> str:
        """
        Generate a unique GraphQL-safe alias from a field name.
        """
        # Create a hash of the field name
        hash_obj = hashlib.sha256(field_name.encode("utf-8"))
        # Take first 8 characters of hex digest for a short but unique identifier
        hash_suffix = hash_obj.hexdigest()[:8]

        # GraphQL aliases must start with a letter or underscore
        # Prefix with 'field_' to ensure it's always valid
        return f"field_{hash_suffix}"

    @property
    def query(self) -> str:
        """Dynamically build GraphQL query to fetch item and its field values."""
        field_value_queries = []
        for field_config in self._current_project_field_configurations:
            original_field_name = field_config.get("name")
            if not original_field_name:
                continue

            alias = self._generate_gql_alias(original_field_name)
            escaped_field_name = self._escape_graphql_string(original_field_name)
            # Comprehensive inline fragments for ProjectV2ItemFieldValue. Project's
            # custom fields supports types: Text, Number, Date, SingleSelect, Iteration,
            # so we fetch values from the corresponding types.
            field_value_query = f'''
            {alias}: fieldValueByName(name: "{escaped_field_name}") {{
                __typename
                ... on ProjectV2ItemFieldTextValue {{
                  node_id: id
                  id: databaseId
                  text
                  creator {{
                    {ACTOR_FRAGMENT}
                  }}
                  created_at: createdAt
                  updated_at: updatedAt
                }}
                ... on ProjectV2ItemFieldDateValue {{
                  node_id: id
                  id: databaseId
                  date
                  creator {{
                    {ACTOR_FRAGMENT}
                  }}
                  created_at: createdAt
                  updated_at: updatedAt
                }}
                ... on ProjectV2ItemFieldNumberValue {{
                  node_id: id
                  id: databaseId
                  number
                  creator {{
                    {ACTOR_FRAGMENT}
                  }}
                  created_at: createdAt
                  updated_at: updatedAt
                }}
                ... on ProjectV2ItemFieldSingleSelectValue {{
                  node_id: id
                  id: databaseId
                  color
                  description
                  name
                  option_id: optionId
                  creator {{
                    {ACTOR_FRAGMENT}
                  }}
                  created_at: createdAt
                  updated_at: updatedAt
                }}
                ... on ProjectV2ItemFieldIterationValue {{
                  node_id: id
                  id: databaseId
                  duration
                  start_date: startDate
                  iteration_id: iterationId
                  title
                  creator {{
                    {ACTOR_FRAGMENT}
                  }}
                  created_at: createdAt
                  updated_at: updatedAt
                }}
            }}'''
            field_value_queries.append(field_value_query)

        all_field_values_query_part = "\n".join(field_value_queries)

        return f"""
        query ProjectItemsWithFieldValues(
            $org: String!,
            $project_number: Int!,
            $nextPageCursor_0: String
        ) {{
          organization(login: $org) {{
            projectV2(number: $project_number) {{
              items(first: 100, after: $nextPageCursor_0) {{
                nodes {{
                  node_id: id
                  id: fullDatabaseId
                  created_at: createdAt
                  updated_at: updatedAt
                  is_archived: isArchived
                  type
                  creator {{
                    {ACTOR_FRAGMENT}
                  }}
                  content {{
                    ... on Issue {{
                      type: __typename
                      node_id: id
                    }}
                    ... on DraftIssue {{
                      type: __typename
                      node_id: id
                    }}
                    ... on PullRequest {{
                      type: __typename
                      node_id: id
                    }}
                  }}
                  {all_field_values_query_part}
                }}
                pageInfo {{
                  hasNextPage_0: hasNextPage
                  endCursor_0: endCursor
                }}
                totalCount
              }}
            }}
          }}
          rateLimit {{
            cost
          }}
        }}
        """

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        if not context:
            # This should not happen if parent_stream_type is correctly set
            self.logger.warning("ProjectItemFieldValuesStream received no context.")
            return {}

        self._current_project_field_configurations = context.get(
            "project_field_configurations", []
        )

        params = super().get_url_params(context, next_page_token)
        # org and project_number are already in params via context from parent
        return params

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        """Process the fetched record to extract field values and add context."""
        row = super().post_process(row, context)
        if not context:
            return row

        # Add context fields
        row["org"] = context["org"]
        row["project_number"] = context["project_number"]

        # Process dynamic field values into a list of objects
        field_values_output: list[dict[str, Any]] = []

        # Initialize dedicated fields for common project fields
        for field_config in self._common_fields.values():
            row[field_config["column"]] = None

        for field_config in self._current_project_field_configurations:
            original_field_name = field_config.get("name")
            if not original_field_name:
                continue

            alias = self._generate_gql_alias(original_field_name)
            field_value_data = row.pop(alias, None)  # Pop the aliased data

            if field_value_data:
                value_type = field_value_data.get("__typename")
                if value_type not in self._supported_project_item_field_value_types:
                    continue

                entry: dict[str, Any] = {
                    "field_name": original_field_name,
                    "value_type": value_type,
                }

                # Copy all the values
                for key in ["node_id", "id", "created_at", "updated_at"]:
                    if key in field_value_data:
                        entry[key] = field_value_data[key]

                # Copy creator if present
                if "creator" in field_value_data:
                    entry["creator"] = field_value_data["creator"]

                # Extract the actual value based on type
                if value_type == "ProjectV2ItemFieldTextValue":
                    text_value = field_value_data.get("text")
                    entry["value"] = str(text_value) if text_value is not None else None
                elif value_type == "ProjectV2ItemFieldDateValue":
                    date_value = field_value_data.get("date")
                    entry["value"] = str(date_value) if date_value is not None else None
                elif value_type == "ProjectV2ItemFieldNumberValue":
                    number_value = field_value_data.get("number")
                    entry["value"] = (
                        str(number_value) if number_value is not None else None
                    )
                elif value_type == "ProjectV2ItemFieldSingleSelectValue":
                    name_value = field_value_data.get("name")
                    entry["value"] = str(name_value) if name_value is not None else None
                    entry["option_id"] = field_value_data.get("option_id")
                    entry["color"] = field_value_data.get("color")
                    entry["description"] = field_value_data.get("description")
                elif value_type == "ProjectV2ItemFieldIterationValue":
                    title_value = field_value_data.get("title")
                    entry["value"] = (
                        str(title_value) if title_value is not None else None
                    )
                    entry["iteration_id"] = field_value_data.get("iteration_id")
                    entry["start_date"] = field_value_data.get("start_date")
                    entry["duration"] = field_value_data.get("duration")

                # Check if this is a common field that should be extracted separately
                is_common_field = (
                    self._common_fields.get(original_field_name)
                    and self._common_fields[original_field_name]["type"] == value_type
                )

                if is_common_field:
                    column_name = self._common_fields[original_field_name]["column"]
                    row[column_name] = entry
                else:
                    field_values_output.append(entry)

        row["field_values"] = field_values_output

        return row

    @property
    def schema(self) -> dict:
        """Define schema with dynamic_fields as an array of name/value objects."""
        properties = th.PropertiesList(
            th.Property("org", th.StringType),
            th.Property("project_number", th.IntegerType),
            th.Property("node_id", th.StringType),  # id from GraphQL
            th.Property(
                "id", th.StringType, nullable=False
            ),  # fullDatabaseId from GraphQL, nullable
            th.Property("created_at", th.DateTimeType),
            th.Property("updated_at", th.DateTimeType),
            th.Property("is_archived", th.BooleanType),
            th.Property("type", th.StringType),
            # Dedicated fields for common project fields
            th.Property(
                "title",
                th.ObjectType(
                    th.Property("value_type", th.StringType),
                    th.Property("node_id", th.StringType),
                    th.Property(
                        "id", th.StringType, required=False
                    ),  # databaseId is nullable
                    th.Property("created_at", th.DateTimeType),
                    th.Property("updated_at", th.DateTimeType),
                    th.Property(
                        "value", th.StringType, required=False
                    ),  # text value is nullable
                    th.Property(
                        "creator",
                        th.ObjectType(
                            th.Property("login", th.StringType),
                            th.Property("resource_path", th.StringType),
                            th.Property("url", th.StringType),
                            th.Property("type", th.StringType),
                            th.Property("node_id", th.StringType),
                            th.Property("id", th.StringType, required=False),
                        ),
                        required=False,  # creator is nullable
                    ),
                ),
                required=False,
            ),
            th.Property(
                "status",
                th.ObjectType(
                    th.Property("value_type", th.StringType),
                    th.Property("node_id", th.StringType),
                    th.Property(
                        "id", th.StringType, required=False
                    ),  # databaseId is nullable
                    th.Property("created_at", th.DateTimeType),
                    th.Property("updated_at", th.DateTimeType),
                    th.Property(
                        "value", th.StringType, required=False
                    ),  # name value is nullable
                    th.Property("option_id", th.StringType, required=False),  # nullable
                    th.Property("color", th.StringType, required=False),  # nullable
                    th.Property(
                        "description", th.StringType, required=False
                    ),  # nullable
                    th.Property(
                        "creator",
                        th.ObjectType(
                            th.Property("login", th.StringType),
                            th.Property("resource_path", th.StringType),
                            th.Property("url", th.StringType),
                            th.Property("type", th.StringType),
                            th.Property("node_id", th.StringType),
                            th.Property("id", th.StringType, required=False),
                        ),
                        required=False,  # creator is nullable
                    ),
                ),
                required=False,
            ),
            th.Property(
                "creator",
                th.ObjectType(
                    th.Property("login", th.StringType),
                    th.Property("resource_path", th.StringType),
                    th.Property("url", th.StringType),
                    th.Property("type", th.StringType),
                    th.Property("node_id", th.StringType),
                    th.Property("id", th.StringType, required=False),
                ),
                required=False,  # creator can be null
            ),
            th.Property(
                "content",
                th.ObjectType(
                    th.Property("type", th.StringType),
                    th.Property("node_id", th.StringType),
                ),
                required=False,  # content can be null for some items
            ),
            th.Property(
                "field_values",
                th.ArrayType(
                    th.ObjectType(
                        th.Property("field_name", th.StringType),
                        th.Property("value_type", th.StringType),
                        th.Property("node_id", th.StringType),
                        th.Property(
                            "id", th.StringType, required=False
                        ),  # databaseId is nullable
                        th.Property("created_at", th.DateTimeType),
                        th.Property("updated_at", th.DateTimeType),
                        th.Property(
                            "value", th.StringType, required=False
                        ),  # All value fields are nullable in GraphQL
                        th.Property(
                            "creator",
                            th.ObjectType(
                                th.Property("login", th.StringType),
                                th.Property("resource_path", th.StringType),
                                th.Property("url", th.StringType),
                                th.Property("type", th.StringType),
                                th.Property("node_id", th.StringType),
                                th.Property("id", th.StringType, required=False),
                            ),
                            required=False,  # creator is nullable in GraphQL
                        ),
                        # Type-specific fields
                        th.Property(
                            "option_id", th.StringType, required=False
                        ),  # SingleSelect - nullable
                        th.Property(
                            "color", th.StringType, required=False
                        ),  # SingleSelect - only required for SingleSelect type
                        th.Property(
                            "description", th.StringType, required=False
                        ),  # SingleSelect - nullable
                        th.Property(
                            "iteration_id", th.StringType, required=False
                        ),  # Iteration - required when present
                        th.Property(
                            "start_date", th.DateType, required=False
                        ),  # Iteration - required when present
                        th.Property(
                            "duration", th.IntegerType, required=False
                        ),  # Iteration - required when present
                    )
                ),
            ),
        )
        return properties.to_dict()
