"""User Stream types classes for tap-github."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

import requests

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_github.client import GitHubGraphqlStream, GitHubRestStream

if TYPE_CHECKING:
    from collections.abc import Iterable


class OrganizationStream(GitHubRestStream):
    """Defines a GitHub Organization Stream.
    API Reference: https://docs.github.com/en/rest/reference/orgs#get-an-organization
    """

    name = "organizations"
    path = "/orgs/{org}"

    @property
    def partitions(self) -> list[dict] | None:
        return [{"org": org} for org in self.config["organizations"]]

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            "org": record["login"],
        }

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

    def get_child_context(self, record: dict, context: dict | None) -> dict:
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

    def get_child_context(self, record: dict, context: dict | None) -> dict:
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

class OrganizationProjectsV2Stream(GitHubGraphqlStream):
    """Defines the 'projects_v2' stream for GitHub Projects at org level."""

    name = "projects_v2"
    parent_stream_type = OrganizationStream
    ignore_parent_replication_key = True
    state_partitioning_keys: ClassVar[list[str]] = ["org"]
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"
    
    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Get records from the source.
        
        Overridden to add rate limit checking before processing projects.
        """
        # Check rate limits before starting to process projects
        self.check_rate_limits()
        
        # Continue with normal record retrieval
        yield from super().get_records(context)
    
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        return super().get_url_params(context, next_page_token)
    
    def post_process(self, row: dict, context: dict | None = None) -> dict:
        """Process organization project record."""
        row = super().post_process(row, context)
        self.logger.debug(f"Processing organization project: {row.get('id')} - {row.get('title')}")
        return row
        
    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Return a context dictionary for child streams."""
        if context is None:
            context = {}
            
        self.logger.debug(f"Creating child context from project record: {record.get('id')} - {record.get('title')}")
        return {
            "org": record.get("org", context.get("org")),
            "project_id": record.get("id")
        }
    
    @property
    def query_jsonpath(self) -> str:
        """Return the jsonpath for the query results based on configuration."""
        return "$.data.organization.projectsV2.nodes[*]"
    
    @property
    def query(self) -> str:
        """Return the GraphQL query for Organization ProjectsV2."""
        return """
        query($org: String!, $per_page: Int!, $nextPageCursor_0: String) {
          organization(login: $org) {
            projectsV2(first: $per_page, after: $nextPageCursor_0) {
              nodes {
                id
                databaseId
                number
                title
                shortDescription
                url
                public
                closed
                createdAt
                updatedAt
                creator {
                  login
                  __typename
                }
              }
              pageInfo {
                hasNextPage_0: hasNextPage
                endCursor_0: endCursor
                startCursor_0: startCursor
              }
            }
          }
        }
        """
        
    schema = th.PropertiesList(
        # Parent keys
        th.Property("org", th.StringType),
        # Rest
        th.Property("id", th.StringType),
        th.Property("databaseId", th.IntegerType),
        th.Property("number", th.IntegerType),
        th.Property("title", th.StringType),
        th.Property("shortDescription", th.StringType),
        th.Property("url", th.StringType),
        th.Property("public", th.BooleanType),
        th.Property("closed", th.BooleanType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property(
            "creator",
            th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("__typename", th.StringType),
            ),
        ),
    ).to_dict()


class ProjectV2ItemsStream(GitHubGraphqlStream):
    """Defines the 'project_v2_items' stream for GitHub Project items."""

    MAX_PER_PAGE = 20
    name = "project_v2_items"
    parent_stream_type = OrganizationProjectsV2Stream
    ignore_parent_replication_key = True
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "updatedAt"
    state_partitioning_keys: ClassVar[list[str]] = ["org", "project_id"]
    
    query_jsonpath = "$.data.node.items.nodes[*]"
    
    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Get records from the source.
        
        Overridden to add rate limit checking before processing items,
        as this stream can be expensive in terms of API quota.
        """
        # Check rate limits before starting to process items
        self.check_rate_limits()
        
        # Continue with normal record retrieval
        yield from super().get_records(context)
    
    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Return a context dictionary for child streams."""
        if context is None:
            context = {}
            
        return {
            "org": record.get("org", context.get("org")),
            "project_id": record.get("id")
        }

    @property
    def query(self) -> str:
        """Return the GraphQL query for ProjectV2Items."""
        return """
        query($project_id: ID!, $per_page: Int!, $nextPageCursor_0: String) {
          node(id: $project_id) {
            ... on ProjectV2 {
              items(first: $per_page, after: $nextPageCursor_0) {
                nodes {
                  id
                  type
                  fieldValues(first: 5) {
                    nodes {
                      ... on ProjectV2ItemFieldTextValue {
                        text
                        field {
                          ... on ProjectV2FieldCommon {
                            name
                            id
                          }
                        }
                      }
                      ... on ProjectV2ItemFieldDateValue {
                        date
                        field {
                          ... on ProjectV2FieldCommon {
                            name
                            id
                          }
                        }
                      }
                      ... on ProjectV2ItemFieldSingleSelectValue {
                        name
                        field {
                          ... on ProjectV2FieldCommon {
                            name
                            id
                          }
                        }
                      }
                      ... on ProjectV2ItemFieldNumberValue {
                        number
                        field {
                          ... on ProjectV2FieldCommon {
                            name
                            id
                          }
                        }
                      }
                      ... on ProjectV2ItemFieldIterationValue {
                        title
                        startDate
                        duration
                        field {
                          ... on ProjectV2FieldCommon {
                            name
                            id
                          }
                        }
                      }
                      ... on ProjectV2ItemFieldLabelValue {
                        labels(first: 5) {
                          nodes {
                            name
                            color
                          }
                        }
                        field {
                          ... on ProjectV2FieldCommon {
                            name
                            id
                          }
                        }
                      }
                      ... on ProjectV2ItemFieldMilestoneValue {
                        milestone {
                          title
                          dueOn
                        }
                        field {
                          ... on ProjectV2FieldCommon {
                            name
                            id
                          }
                        }
                      }
                      ... on ProjectV2ItemFieldPullRequestValue {
                        pullRequests(first: 5) {
                          nodes {
                            title
                            number
                          }
                        }
                        field {
                          ... on ProjectV2FieldCommon {
                            name
                            id
                          }
                        }
                      }
                      ... on ProjectV2ItemFieldRepositoryValue {
                        repository {
                          name
                          owner {
                            login
                          }
                        }
                        field {
                          ... on ProjectV2FieldCommon {
                            name
                            id
                          }
                        }
                      }
                      ... on ProjectV2ItemFieldUserValue {
                        users(first: 5) {
                          nodes {
                            login
                          }
                        }
                        field {
                          ... on ProjectV2FieldCommon {
                            name
                            id
                          }
                        }
                      }
                    }
                  }
                  content {
                    ... on Issue {
                      id
                      number
                      title
                      url
                      state
                      repository {
                        id
                        databaseId
                        name
                        owner {
                          login
                        }
                      }
                    }
                    ... on PullRequest {
                      id
                      number
                      title
                      url
                      state
                      repository {
                        id
                        databaseId
                        name
                        owner {
                          login
                        }
                      }
                    }
                    ... on DraftIssue {
                      id
                      title
                    }
                  }
                  createdAt
                  updatedAt
                }
                pageInfo {
                  hasNextPage_0: hasNextPage
                  endCursor_0: endCursor
                  startCursor_0: startCursor
                }
              }
            }
          }
        }
        """
        
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        
        if context is None:
            self.logger.error("Context is None when trying to get project_id for API call")
            raise ValueError("Context is required for ProjectV2ItemsStream")
            
        if "project_id" not in context:
            self.logger.error(f"Missing project_id in context: {context}")
            raise ValueError("project_id is required in context for ProjectV2ItemsStream")
            
        params["project_id"] = context["project_id"]
        return params

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,  # noqa: ANN401
    ) -> Any | None:  # noqa: ANN401
        """Return a token for identifying next page or None if no more pages."""
        next_token = super().get_next_page_token(response, previous_token)
        
        # Extra handling for pagination issues
        if next_token == previous_token:
            self.logger.warning(f"Identical pagination token detected: {next_token}. Stopping pagination.")
            return None
            
        return next_token
    
    def post_process(self, row: dict, context: dict | None = None) -> dict:
        """Process ProjectV2 item records."""
        assert context is not None, f"Context cannot be empty for '{self.name}' stream."
        row = super().post_process(row, context)
        
        if "project_id" in context:
            row["project_id"] = context["project_id"]
        else:
            self.logger.warning(f"Missing project_id in context during post_process: {context}")
        
        # Extract repository ID if available
        if row.get("content") and row["content"].get("repository"):
            # Use the numeric database ID for consistency with other streams
            if "databaseId" in row["content"]["repository"]:
                row["repo_id"] = row["content"]["repository"]["databaseId"]
            
            # Also add repository name and owner for convenience
            if "name" in row["content"]["repository"]:
                row["repo"] = row["content"]["repository"]["name"]
                
            if "owner" in row["content"]["repository"] and "login" in row["content"]["repository"]["owner"]:
                row["repo_owner"] = row["content"]["repository"]["owner"]["login"]
        
        # Process field values into a more usable structure
        if row.get("fieldValues") and row["fieldValues"].get("nodes"):
            # Create a fields object to hold field values in a more accessible format
            row["fields"] = {}
            
            for field_value in row["fieldValues"]["nodes"]:
                if not field_value:
                    # Skip empty field values
                    continue
                    
                if field_value.get("field") and field_value["field"].get("name"):
                    field_name = field_value["field"]["name"]
                    
                    # Extract the appropriate value based on field type
                    if "text" in field_value and field_value["text"] is not None:
                        row["fields"][field_name] = field_value["text"]
                    elif "date" in field_value and field_value["date"] is not None:
                        row["fields"][field_name] = field_value["date"]
                    elif "name" in field_value and field_value["name"] is not None:
                        row["fields"][field_name] = field_value["name"]
                    elif "number" in field_value and field_value["number"] is not None:
                        row["fields"][field_name] = field_value["number"]
                    elif "title" in field_value and field_value["title"] is not None:
                        # For iteration fields, we combine relevant info
                        if "startDate" in field_value and "duration" in field_value:
                            row["fields"][field_name] = {
                                "title": field_value["title"],
                                "startDate": field_value["startDate"],
                                "duration": field_value["duration"]
                            }
                        else:
                            row["fields"][field_name] = field_value["title"]
                    elif "labels" in field_value and field_value["labels"] is not None:
                        if field_value["labels"].get("nodes"):
                            row["fields"][field_name] = [
                                node["name"] for node in field_value["labels"]["nodes"]
                            ]
                    elif "milestone" in field_value and field_value["milestone"] is not None:
                        row["fields"][field_name] = field_value["milestone"]["title"]
                    elif "pullRequests" in field_value and field_value["pullRequests"] is not None:
                        if field_value["pullRequests"].get("nodes"):
                            row["fields"][field_name] = [
                                {"number": pr["number"], "title": pr["title"]} 
                                for pr in field_value["pullRequests"]["nodes"]
                            ]
                    elif "repository" in field_value and field_value["repository"] is not None:
                        if field_value["repository"].get("name") and field_value["repository"].get("owner"):
                            row["fields"][field_name] = f"{field_value['repository']['owner']['login']}/{field_value['repository']['name']}"
                    elif "users" in field_value and field_value["users"] is not None:
                        if field_value["users"].get("nodes"):
                            row["fields"][field_name] = [
                                user["login"] for user in field_value["users"]["nodes"]
                            ]
            
            # Remove the raw fieldValues object from the output
            del row["fieldValues"]
                
        return row

    schema = th.PropertiesList(
        # Parent keys
        th.Property("org", th.StringType),
        th.Property("project_id", th.StringType),
        # Repository info extracted from content
        th.Property("repo_id", th.IntegerType),
        th.Property("repo", th.StringType),
        th.Property("repo_owner", th.StringType),
        # Rest
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        # Field values as a structured object with all field values
        th.Property("fields", th.ObjectType(additional_properties=True)),
        th.Property(
            "content",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("number", th.IntegerType),
                th.Property("title", th.StringType),
                th.Property("url", th.StringType),
                th.Property("state", th.StringType),
                th.Property(
                    "repository", 
                    th.ObjectType(
                        th.Property("id", th.StringType),
                        th.Property("databaseId", th.IntegerType),
                        th.Property("name", th.StringType),
                        th.Property(
                            "owner",
                            th.ObjectType(
                                th.Property("login", th.StringType),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()