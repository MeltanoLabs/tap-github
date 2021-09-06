import datetime

from tap_github.tap import TapGitHub
from singer_sdk.helpers._catalog import set_catalog_stream_selected

SEARCH_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "searches": [
        {
            "name": "tap_something",
            "query": "tap-+language:Python",
        }
    ],
}
repo_list = ["octocat/hello-world", "MeltanoLabs/tap-github"]
repo_list_context = [
    {"org": "octocat", "repo": "hello-world"},
    {"org": "MeltanoLabs", "repo": "tap-github"},
]
REPO_LIST_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "repositories": repo_list,
}


def test_validate_repo_list_config():
    """Verify that the repositories list is parsed correctly"""
    tap = TapGitHub(config=REPO_LIST_CONFIG)
    partitions = tap.streams["repositories"].partitions
    assert partitions == repo_list_context


def test_get_a_repository_in_repo_list_mode(capsys):
    """
    Discover the catalog, and request 2 repository records
    """
    tap1 = TapGitHub(config=REPO_LIST_CONFIG)
    tap1.run_discovery()
    catalog = tap1.catalog_dict
    # disable child streams
    set_catalog_stream_selected(catalog, "repositories", True)
    set_catalog_stream_selected(catalog, "issues", False)
    set_catalog_stream_selected(catalog, "issue_comments", False)
    tap2 = TapGitHub(config=REPO_LIST_CONFIG, catalog=catalog)
    tap2.sync_all()
    captured = capsys.readouterr()
    # Verify we got the right number of records
    assert captured.out.count("RECORD") == 2
