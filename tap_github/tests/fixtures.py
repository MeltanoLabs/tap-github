import os
import logging
import datetime

import pytest


@pytest.fixture
def search_config():
    return {
        "metrics_log_level": "none",
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
        "metrics_log_level": "none",
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
        "metrics_log_level": "none",
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
        "metrics_log_level": "none",
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
        "metrics_log_level": "none",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "organizations": organization_list,
        "rate_limit_buffer": 100,
    }


def alternative_sync_chidren(self, child_context: dict) -> None:
    """
    Override for Stream._sync_children.
    Enabling us to use an ORG_LEVEL_TOKEN for the collaborators stream.
    """
    for child_stream in self.child_streams:
        # Use org:write access level credentials for collaborators stream
        if child_stream.name in ["collaborators"]:
            ORG_LEVEL_TOKEN = os.environ.get("ORG_LEVEL_TOKEN")
            if not ORG_LEVEL_TOKEN:
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
