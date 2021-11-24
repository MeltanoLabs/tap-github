import pytest

from tap_github.tap import TapGitHub

repo_list_2 = ["octocat/hello-world", "MeltanoLabs/tap-github", "mapswipe/mapswipe"]


@pytest.mark.repo_list(repo_list_2)
def test_validate_repo_list_config(repo_list_config):
    """Verify that the repositories list is parsed correctly"""
    repo_list_context = [
        {"org": "octocat", "repo": "hello-world"},
        {"org": "MeltanoLabs", "repo": "tap-github"},
        {"org": "mapswipe", "repo": "mapswipe"},
    ]
    tap = TapGitHub(config=repo_list_config)
    partitions = tap.streams["repositories"].partitions
    assert partitions == repo_list_context


@pytest.mark.repo_list(repo_list_2)
def test_get_a_repository_in_repo_list_mode(capsys, repo_list_config):
    """
    Discover the catalog, and request 2 repository records
    """
    tap1 = TapGitHub(config=repo_list_config)
    tap1.run_discovery()
    catalog = tap1.catalog_dict
    # disable child streams
    # FIXME: this does not work, the child streams are still fetched
    # deselect_all_streams(catalog)
    # set_catalog_stream_selected(
    #     catalog=catalog, stream_name="repositories", selected=True
    # )
    # discard previous output to stdout (potentially from other tests)
    capsys.readouterr()
    tap2 = TapGitHub(config=repo_list_config, catalog=catalog)
    tap2.sync_all()
    captured = capsys.readouterr()
    # Verify we got the right number of records (one per repo in the list)
    assert captured.out.count('{"type": "RECORD", "stream": "repositories"') == len(
        repo_list_2
    )
