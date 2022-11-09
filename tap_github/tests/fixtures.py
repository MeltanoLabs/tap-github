import datetime
import logging
import os
import sys

import pytest

from ..utils.filter_stdout import FilterStdOutput

# Filter out singer output during tests
sys.stdout = FilterStdOutput(sys.stdout, r'{"type": ')  # type: ignore


@pytest.fixture
def search_config():
    return {
        "metrics_log_level": "warning",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "searches": [
            {
                "name": "tap_something",
                "query": "tap-+language:Python",
            }
        ],
    }


@pytest.fixture
def repo_list_config(request):
    """
    Get a default list of repos or pass your own by decorating your test with
    @pytest.mark.repo_list(['org1/repo1', 'org2/repo2'])
    """
    marker = request.node.get_closest_marker("repo_list")
    if marker is None:
        repo_list = ["MeltanoLabs/tap-github", "mapswipe/mapswipe"]
    else:
        repo_list = marker.args[0]

    return {
        "metrics_log_level": "warning",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "repositories": repo_list,
        "rate_limit_buffer": 100,
    }


@pytest.fixture
def username_list_config(request):
    """
    Get a default list of usernames or pass your own by decorating your test with
    @pytest.mark.username_list(['ericboucher', 'aaronsteers'])
    """
    marker = request.node.get_closest_marker("username_list")
    if marker is None:
        username_list = ["ericboucher", "aaronsteers"]
    else:
        username_list = marker.args[0]

    return {
        "metrics_log_level": "warning",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "user_usernames": username_list,
        "rate_limit_buffer": 100,
    }


@pytest.fixture
def user_id_list_config(request):
    """
    Get a default list of usernames or pass your own by decorating your test with
    @pytest.mark.user_id_list(['ericboucher', 'aaronsteers'])
    """
    marker = request.node.get_closest_marker("user_id_list")
    if marker is None:
        user_id_list = [1, 2]
    else:
        user_id_list = marker.args[0]

    return {
        "metrics_log_level": "warning",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "user_ids": user_id_list,
        "rate_limit_buffer": 100,
    }


@pytest.fixture
def organization_list_config(request):
    """
    Get a default list of organizations or pass your own by decorating your test with
    @pytest.mark.organization_list(['MeltanoLabs', 'oviohub'])
    """
    marker = request.node.get_closest_marker("organization_list")

    organization_list = ["MeltanoLabs"] if marker is None else marker.args[0]

    return {
        "metrics_log_level": "warning",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "organizations": organization_list,
        "rate_limit_buffer": 100,
    }


def alternative_sync_chidren(self, child_context: dict, no_sync: bool = True) -> None:
    """
    Override for Stream._sync_children.
    Enabling us to use an ORG_LEVEL_TOKEN for the collaborators stream.
    """
    for child_stream in self.child_streams:
        # Use org:write access level credentials for collaborators stream
        if child_stream.name in ["collaborators"]:
            ORG_LEVEL_TOKEN = os.environ.get("ORG_LEVEL_TOKEN")
            # TODO - Fix collaborators tests, likely by mocking API responses directly.
            # Currently we have to bypass them as they are failing frequently.
            if not ORG_LEVEL_TOKEN or no_sync:
                logging.warning(
                    'No "ORG_LEVEL_TOKEN" found. Skipping collaborators stream sync.'
                )
                continue
            SAVED_GTHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
            os.environ["GITHUB_TOKEN"] = ORG_LEVEL_TOKEN
            child_stream.sync(context=child_context)
            os.environ["GITHUB_TOKEN"] = SAVED_GTHUB_TOKEN or ""
            continue

        # default behavior:
        if child_stream.selected or child_stream.has_selected_descendents:
            child_stream.sync(context=child_context)


@pytest.fixture
def mock_commits_response():
    """
    Return a fixed API response for the commits stream.
    Used to test descending streams and bookmarks interaction.
    This mock is trimmed down a fair bit from the actual response to save space.
    """
    return [
        {
            "sha": "1a984f582592fc5979d78bb0151707deefadde6f",
            "node_id": "C_kwDOFcLMsNoAKDFhOTg0ZjU4MjU5MmZjNTk3OWQ3OGJiMDE1MTcwN2RlZWZhZGRlNmY",
            "commit": {
                "author": {
                    "date": "2022-07-01T13:47:56Z",
                },
                "committer": {
                    "name": "GitHub",
                    "email": "noreply@github.com",
                    "date": "2022-07-01T13:47:56Z",
                },
                "message": "Wait and retry on secondary limits (#153)",
                "tree": {
                    "sha": "d3b0aa136fff77eaf84684dd25116f59fba2c2e6",
                },
                "verification": {
                    "verified": True,
                },
            },
            "author": {},
            "committer": {},
            "parents": [
                {
                    "sha": "14ed2b3401a73ea25bfb5206acba836a0e7a8683",
                }
            ],
        },
        {
            "sha": "14ed2b3401a73ea25bfb5206acba836a0e7a8683",
            "node_id": "C_kwDOFcLMsNoAKDE0ZWQyYjM0MDFhNzNlYTI1YmZiNTIwNmFjYmE4MzZhMGU3YTg2ODM",
            "commit": {
                "author": {},
                "committer": {
                    "name": "GitHub",
                    "email": "noreply@github.com",
                    "date": "2022-06-24T20:18:19Z",
                },
                "message": "Update sdk to fix bug in api costs hook (#151)",
            },
            "author": {},
            "committer": {
                "login": "web-flow",
                "id": 19864447,
                "node_id": "MDQ6VXNlcjE5ODY0NDQ3",
            },
            "parents": [
                {
                    "sha": "16d6d7a91f520bb33e3590daf90fc9c699bc92a8",
                }
            ],
        },
        {
            "sha": "16d6d7a91f520bb33e3590daf90fc9c699bc92a8",
            "node_id": "C_kwDOFcLMsNoAKDE2ZDZkN2E5MWY1MjBiYjMzZTM1OTBkYWY5MGZjOWM2OTliYzkyYTg",
            "commit": {
                "author": {
                    "date": "2022-06-24T19:28:46Z",
                },
                "committer": {
                    "name": "GitHub",
                    "email": "noreply@github.com",
                    "date": "2022-06-24T19:28:46Z",
                },
                "message": "Bypass since parameter in issue_comments to avoid server errors (#150)",
                "tree": {
                    "sha": "5140a94f7436dbf230133c83410155b57b787739",
                },
                "verification": {
                    "verified": True,
                },
            },
            "author": {},
            "committer": {},
            "parents": [
                {
                    "sha": "438a2346cf91612dad553d7cdd6b9ebd27a7e1d4",
                }
            ],
        },
    ]
