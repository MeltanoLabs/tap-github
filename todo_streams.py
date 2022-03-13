from singer_sdk import typing as th

from tap_github.client import GitHubRestStream


# TODO not sure what the parent stream is here.
class TeamsStream(GitHubRestStream):
    name = "teams"
    primary_keys = ["id"]
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
    primary_keys = ["id"]
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
