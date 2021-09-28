import datetime
import pytest


@pytest.fixture
def search_config():
    return {
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
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "repositories": repo_list,
    }
