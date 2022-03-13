from typing import Optional

from singer_sdk import typing as th

from tap_github.client import GitHubRestStream
from tap_github.repository_streams import PullRequestsStream, RepositoryStream


class ProjectsStream(GitHubRestStream):
    name = "projects"
    path = "/repos/{org}/{repo}/projects"
    ignore_parent_replication_key = False
    primary_keys = ["id"]
    parent_stream_type = RepositoryStream

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"project_id": record["id"]}

    schema = th.PropertiesList(
        th.Property("owner_url", th.StringType),
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("columns_url", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("body", th.StringType),
        th.Property("number", th.IntegerType),
        th.Property("state", th.StringType),
        th.Property(
            "creator",
            th.ObjectType(
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
            ),
        ),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
    ).to_dict()


class ProjectColumnsStream(GitHubRestStream):
    name = "project_columns"
    path = "/projects/{project_id}/columns"
    ignore_parent_replication_key = False
    primary_keys = ["id"]
    parent_stream_type = ProjectsStream

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"column_id": record["id"]}

    schema = th.PropertiesList(
        th.Property("url", th.StringType),
        th.Property("project_url", th.StringType),
        th.Property("cards_url", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
    ).to_dict()


class ProjectCardsStream(GitHubRestStream):
    name = "project_cards"
    path = "/projects/columns/{column_id}/cards"
    ignore_parent_replication_key = False
    primary_keys = ["id"]
    parent_stream_type = ProjectColumnsStream
    schema = th.PropertiesList(
        th.Property("url", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("note", th.StringType),
        th.Property(
            "creator",
            th.ObjectType(
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
            ),
        ),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
        th.Property("archived", th.BooleanType),
        th.Property("column_url", th.StringType),
        th.Property("content_url", th.StringType),
        th.Property("project_url", th.StringType),
    ).to_dict()


class PrCommitsStream(GitHubRestStream):
    name = "pr_commits"
    path = "/repos/{org}/{repo}/pulls/{pull_number}/commits"
    ignore_parent_replication_key = False
    primary_keys = ["node_id"]
    parent_stream_type = PullRequestsStream
    schema = th.PropertiesList(
        th.Property("url", th.StringType),
        th.Property("sha", th.StringType),
        th.Property("node_id", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("comments_url", th.StringType),
        th.Property(
            "commit",
            th.ObjectType(
                th.Property("url", th.StringType),
                th.Property(
                    "author",
                    th.ObjectType(
                        th.Property("name", th.StringType),
                        th.Property("email", th.StringType),
                        th.Property("date", th.StringType),
                    ),
                ),
                th.Property(
                    "committer",
                    th.ObjectType(
                        th.Property("name", th.StringType),
                        th.Property("email", th.StringType),
                        th.Property("date", th.StringType),
                    ),
                ),
                th.Property("message", th.StringType),
                th.Property(
                    "tree",
                    th.ObjectType(
                        th.Property("url", th.StringType),
                        th.Property("sha", th.StringType),
                    ),
                ),
                th.Property("comment_count", th.IntegerType),
                th.Property(
                    "verification",
                    th.ObjectType(
                        th.Property("verified", th.BooleanType),
                        th.Property("reason", th.StringType),
                        th.Property("signature", th.StringType),
                        th.Property("payload", th.StringType),
                    ),
                ),
            ),
        ),
        th.Property(
            "author",
            th.ObjectType(
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
            ),
        ),
        th.Property(
            "committer",
            th.ObjectType(
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
            ),
        ),
        th.Property(
            "parents",
            th.ArrayType(
                th.ObjectType(
                    th.Property("url", th.StringType), th.Property("sha", th.StringType)
                )
            ),
        ),
    ).to_dict()


class ReleasesStream(GitHubRestStream):
    name = "releases"
    path = "/repos/{org}/{repo}/releases"
    ignore_parent_replication_key = False
    primary_keys = ["id"]
    parent_stream_type = RepositoryStream
    schema = th.PropertiesList(
        th.Property("url", th.StringType),
        th.Property("html_url", th.StringType),
        th.Property("assets_url", th.StringType),
        th.Property("upload_url", th.StringType),
        th.Property("tarball_url", th.StringType),
        th.Property("zipball_url", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("node_id", th.StringType),
        th.Property("tag_name", th.StringType),
        th.Property("target_commitish", th.StringType),
        th.Property("name", th.StringType),
        th.Property("body", th.StringType),
        th.Property("draft", th.BooleanType),
        th.Property("prerelease", th.BooleanType),
        th.Property("created_at", th.StringType),
        th.Property("published_at", th.StringType),
        th.Property(
            "author",
            th.ObjectType(
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
            ),
        ),
        th.Property(
            "assets",
            th.ArrayType(
                th.ObjectType(
                    th.Property("url", th.StringType),
                    th.Property("browser_download_url", th.StringType),
                    th.Property("id", th.IntegerType),
                    th.Property("node_id", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("label", th.StringType),
                    th.Property("state", th.StringType),
                    th.Property("content_type", th.StringType),
                    th.Property("size", th.IntegerType),
                    th.Property("download_count", th.IntegerType),
                    th.Property("created_at", th.StringType),
                    th.Property("updated_at", th.StringType),
                    th.Property(
                        "uploader",
                        th.ObjectType(
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
                        ),
                    ),
                )
            ),
        ),
    ).to_dict()


# TODO not sure what the parent stream is here.
class TeamsStream(GitHubRestStream):
    name = "teams"
    primary_keys = ['id']
    path = "/orgs/{org}/teams"
    schema = th.PropertiesList(
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
        th.Property("parent", th.StringType),
    ).to_dict()


class TeamMembersStream(GitHubRestStream):
    name = "team_members"
    primary_keys = ['id']
    path = "/orgs/{org}/teams/{team_slug}/members"
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
    ).to_dict()


class TeamRolesStream(GitHubRestStream):
    name = "team_roles"
    path = "/orgs/{org}/teams/{team_slug}/memberships/{username}"
    schema = th.PropertiesList(
        th.Property("url", th.StringType),
        th.Property("role", th.StringType),
        th.Property("state", th.StringType),
    ).to_dict()
