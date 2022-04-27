import os
import logging
import pytest

from unittest.mock import patch

from tap_github.tap import TapGitHub

from .fixtures import repo_list_config

repo_list_2 = [
    "MeltanoLabs/tap-github",
    "MeltanoLabs/tap-gitlab",
    "MeltanoLabs/target-athena",
]
# the github repo ids that match the repo names above
# in the same order
repo_list_2_ids = [
    365087920,
    416891176,
    361619143,
]


@pytest.mark.repo_list(repo_list_2)
def test_validate_repo_list_config(repo_list_config):
    """Verify that the repositories list is parsed correctly"""
    repo_list_context = [
        {
            "org": repo[0].split("/")[0],
            "repo": repo[0].split("/")[1],
            "repo_id": repo[1],
        }
        for repo in zip(repo_list_2, repo_ids)
    ]
    tap = TapGitHub(config=repo_list_config)
    partitions = tap.streams["repositories"].partitions
    assert partitions == repo_list_context


def alternative_sync_chidren(self, child_context: dict) -> None:
    """
    Override for Stream._sync_children.
    Enabling us to use an ORG_LEVEL_TOKEN for the collaborators sream.
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
    with patch(
        "singer_sdk.streams.core.Stream._sync_children", alternative_sync_chidren
    ):
        tap2 = TapGitHub(config=repo_list_config, catalog=catalog)
        tap2.sync_all()
    captured = capsys.readouterr()
    # Verify we got the right number of records (one per repo in the list)
    assert captured.out.count('{"type": "RECORD", "stream": "repositories"') == len(
        repo_list_2
    )
