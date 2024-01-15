"""Reusable schema objects for tap-github.

Below are a few common patterns in the github API
factored out as reusable objects. They help in making the
schema more readable and error-free.
"""

from singer_sdk import typing as th  # JSON Schema typing helpers

# This user object is common throughout the API results
user_object = th.ObjectType(
    th.Property("login", th.StringType),
    th.Property("id", th.IntegerType),
    th.Property("node_id", th.StringType),
    th.Property("avatar_url", th.StringType),
    th.Property("gravatar_id", th.StringType),
    th.Property("html_url", th.StringType),
    th.Property("type", th.StringType),
    th.Property("site_admin", th.BooleanType),
)

# some objects are shared between issues and pull requests
label_object = th.ObjectType(
    th.Property("id", th.IntegerType),
    th.Property("node_id", th.StringType),
    th.Property("url", th.StringType),
    th.Property("name", th.StringType),
    th.Property("description", th.StringType),
    th.Property("color", th.StringType),
    th.Property("default", th.BooleanType),
)

milestone_object = th.ObjectType(
    th.Property("html_url", th.StringType),
    th.Property("node_id", th.StringType),
    th.Property("id", th.IntegerType),
    th.Property("number", th.IntegerType),
    th.Property("state", th.StringType),
    th.Property("title", th.StringType),
    th.Property("description", th.StringType),
    th.Property("creator", user_object),
    th.Property("open_issues", th.IntegerType),
    th.Property("closed_issues", th.IntegerType),
    th.Property("created_at", th.DateTimeType),
    th.Property("updated_at", th.DateTimeType),
    th.Property("closed_at", th.DateTimeType),
    th.Property("due_on", th.DateTimeType),
)

reactions_object = th.ObjectType(
    th.Property("url", th.StringType),
    th.Property("total_count", th.IntegerType),
    th.Property("plus_one", th.IntegerType),
    th.Property("minus_one", th.IntegerType),
    th.Property("laugh", th.IntegerType),
    th.Property("hooray", th.IntegerType),
    th.Property("confused", th.IntegerType),
    th.Property("heart", th.IntegerType),
    th.Property("rocket", th.IntegerType),
    th.Property("eyes", th.IntegerType),
)

files_object = th.ObjectType(
    th.Property("sha", th.StringType),
    th.Property("filename", th.StringType),
    th.Property("status", th.StringType),
    th.Property("additions", th.IntegerType),
    th.Property("deletions", th.IntegerType),
    th.Property("changes", th.IntegerType),
    th.Property("blob_url", th.StringType),
    th.Property("raw_url", th.StringType),
    th.Property("contents_url", th.StringType),
    th.Property("patch", th.StringType),
    th.Property("previous_filename", th.StringType),
)
