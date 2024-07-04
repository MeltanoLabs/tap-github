from __future__ import annotations

import datetime

import pytest


@pytest.fixture()
def repo_list_config(request):
    """Get a default list of repos or pass your own by decorating your test with
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


@pytest.fixture()
def username_list_config(request):
    """Get a default list of usernames or pass your own by decorating your test with
    @pytest.mark.username_list(['ericboucher', 'aaronsteers'])
    """
    marker = request.node.get_closest_marker("username_list")
    username_list = ["ericboucher", "aaronsteers"] if marker is None else marker.args[0]

    return {
        "metrics_log_level": "warning",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "user_usernames": username_list,
        "rate_limit_buffer": 100,
    }
