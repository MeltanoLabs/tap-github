"""Stream type classes for tap-github."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_github.client import GitHubStream


class RepositoryStream(GitHubStream):
    """Define custom stream."""

    def __init__(
        self,
        tap,
        name: Optional[str] = None,
        schema=None,
        path: Optional[str] = None,
        query: str = None,
    ):
        super().__init__(tap=tap, name=name, schema=schema, path=path)
        self.query = query

    path = "/search/repositories"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("full_name", th.StringType),
        th.Property(
            "owner",
            th.ObjectType(
                th.Property("login", th.StringType),
                th.Property("type", th.StringType),
            ),
        ),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
    ).to_dict()

    def get_url_params(
        self, partition: Optional[dict], next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(
            partition=partition, next_page_token=next_page_token
        )
        params["q"] = self.query
        return params
