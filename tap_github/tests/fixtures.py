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
        repo_list = ["octocat/hello-world", "mapswipe/mapswipe"]
    else:
        repo_list = marker.args[0]

    return {
        "metrics_log_level": "none",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "repositories": repo_list,
    }


@pytest.fixture
def usernames_list_config(request):
    """
    Get a default list of usernames or pass your own by decorating your test with
    @pytest.mark.usernames_list(['ericboucher', 'aaronsteers'])
    """
    marker = request.node.get_closest_marker("usernames_list")
    if marker is None:
        usernames_list = ["ericboucher", "aaronsteers"]
    else:
        usernames_list = marker.args[0]

    return {
        "metrics_log_level": "none",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "user_usernames": usernames_list,
    }


@pytest.fixture
def user_ids_list_config(request):
    """
    Get a default list of usernames or pass your own by decorating your test with
    @pytest.mark.user_ids_list(['ericboucher', 'aaronsteers'])
    """
    marker = request.node.get_closest_marker("user_ids_list")
    if marker is None:
        user_ids_list = [1, 2]
    else:
        user_ids_list = marker.args[0]

    return {
        "metrics_log_level": "none",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "user_ids": user_ids_list,
    }
