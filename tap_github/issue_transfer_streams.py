"""Repository Stream types classes for tap-github."""

from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests
from dateutil.parser import parse
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_github.client import GitHubGraphqlStream, GitHubRestStream
from tap_github.schema_objects import (
    label_object,
    milestone_object,
    reactions_object,
    user_object,
)


class IssueTransfersStream(GitHubRestStream):
    """Defines 'IssueTransfers' stream which returns Issues that have been transferred to another repository."""

    name = "issue_transfers"
    path = "/repos/{org}/{repo}/issues/{issue_number}"
    primary_keys = ["id"]
    # Do not fail if the issue has been transferred from a deleted repository
    tolerated_http_errors = [404]

    @property
    def partitions(self) -> Optional[List[Dict]]:
        """Return a list of partitions."""
        if "issues_check_transfer" in self.config:
            issues_check = []
            for issue in self.config["issues_check_transfer"]:
                issue_data = issue.split('|')
                issues_check.append({"org": issue_data[0], "repo": issue_data[1], "issue_number": issue_data[2]})
            return issues_check
        return None

    def get_url_params(
        self, context: Optional[Dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        assert context is not None, f"Context cannot be empty for '{self.name}' stream."
        params = super().get_url_params(context, next_page_token)
        return params
    

    def post_process(self, row: dict, context: Optional[Dict] = None) -> dict:
        row = super().post_process(row, context)


        requested_url = f"https://api.github.com/repos/{context['org']}/{context['repo']}/issues/{context['issue_number']}"
        if requested_url == row['url']:
            return None
        outData = {
            'org': context['org'],
            'repo': context['repo'],
            'issue_number': int(context['issue_number']),
            'transferred_to_url': row['url']
        }
        return outData

    schema = th.PropertiesList(
        th.Property("repo", th.StringType),
        th.Property("org", th.StringType),
        th.Property("issue_number", th.IntegerType),
        th.Property("transferred_to_url", th.StringType)
    ).to_dict()
