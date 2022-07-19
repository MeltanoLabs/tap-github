import json
from typing import Optional
from unittest.mock import patch

import pytest
import requests_mock
from freezegun import freeze_time
from singer_sdk.helpers import _catalog as cat_helpers
from singer_sdk.helpers._singer import Catalog

from tap_github.tap import TapGitHub

from .fixtures import (
    alternative_sync_chidren,
    mock_commits_response,
    repo_list_config,
    username_list_config,
)

repo_list_2 = [
    "MeltanoLabs/tap-github",
    # mistype the repo name so we can check that the tap corrects it
    "MeltanoLabs/Tap-GitLab",
    # mistype the org
    "meltanolabs/target-athena",
]
# the same list, but without typos, for validation
repo_list_2_corrected = [
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
        for repo in zip(repo_list_2_corrected, repo_list_2_ids)
    ]
    tap = TapGitHub(config=repo_list_config)
    partitions = tap.streams["repositories"].partitions
    assert partitions == repo_list_context


def run_tap_with_config(capsys, config_obj: dict, skip_stream: Optional[str]) -> str:
    """
    Run the tap with the given config and capture stdout, optionally
    skipping a stream (this is meant to be the top level stream)
    """
    tap1 = TapGitHub(config=config_obj)
    tap1.run_discovery()
    catalog = Catalog.from_dict(tap1.catalog_dict)
    # Reset and re-initialize with an input catalog
    if skip_stream is not None:
        cat_helpers.set_catalog_stream_selected(
            catalog=catalog,
            stream_name=skip_stream,
            selected=False,
        )
    # discard previous output to stdout (potentially from other tests)
    capsys.readouterr()
    with patch(
        "singer_sdk.streams.core.Stream._sync_children", alternative_sync_chidren
    ):
        tap2 = TapGitHub(config=config_obj, catalog=catalog.to_dict())
        tap2.sync_all()
    captured = capsys.readouterr()
    return captured.out


@pytest.mark.parametrize("skip_parent_streams", [False, True])
@pytest.mark.repo_list(repo_list_2)
def test_get_a_repository_in_repo_list_mode(
    capsys, repo_list_config, skip_parent_streams
):
    """
    Discover the catalog, and request 2 repository records.
    The test is parametrized to run twice, with and without
    syncing the top level `repositories` stream.
    """
    repo_list_config["skip_parent_streams"] = skip_parent_streams
    captured_out = run_tap_with_config(
        capsys, repo_list_config, "repositories" if skip_parent_streams else None
    )
    # Verify we got the right number of records
    # one per repo in the list only if we sync the "repositories" stream, 0 if not
    assert captured_out.count('{"type": "RECORD", "stream": "repositories"') == len(
        repo_list_2 * (not skip_parent_streams)
    )
    # check that the tap corrects invalid case in config input
    assert '"repo": "Tap-GitLab"' not in captured_out
    assert '"org": "meltanolabs"' not in captured_out


# case is incorrect on purpose, so we can check that the tap corrects it
# and run the test twice, with and without syncing the `users` stream
@pytest.mark.parametrize("skip_parent_streams", [False, True])
@pytest.mark.username_list(["EricBoucher", "aaRONsTeeRS"])
def test_get_a_user_in_user_usernames_mode(
    capsys, username_list_config, skip_parent_streams
):
    """
    Discover the catalog, and request 2 repository records
    """
    username_list_config["skip_parent_streams"] = skip_parent_streams
    captured_out = run_tap_with_config(
        capsys, username_list_config, "users" if skip_parent_streams else None
    )
    # Verify we got the right number of records:
    # one per user in the list if we sync the root stream, 0 otherwise
    assert captured_out.count('{"type": "RECORD", "stream": "users"') == len(
        username_list_config["user_usernames"] * (not skip_parent_streams)
    )
    # these 2 are inequalities as number will keep changing :)
    assert captured_out.count('{"type": "RECORD", "stream": "starred"') > 150
    assert captured_out.count('{"type": "RECORD", "stream": "user_contributed_to"') > 50
    assert '{"username": "aaronsteers"' in captured_out
    assert '{"username": "aaRONsTeeRS"' not in captured_out
    assert '{"username": "EricBoucher"' not in captured_out


@pytest.mark.repo_list(["MeltanoLabs/tap-github"])
def test_replication_key_for_desc_streams(
    repo_list_config: dict, mock_commits_response
):
    """Verify that the stream correctly saves bookmarks for streams
    that are ordered in descending order.
    """
    # instantiate a tap for the commits stream, with 1 single repo
    tap1 = TapGitHub(config=repo_list_config)
    tap1.run_discovery()
    catalog = Catalog.from_dict(tap1.catalog_dict)
    cat_helpers.deselect_all_streams(catalog)
    cat_helpers.set_catalog_stream_selected(
        catalog=catalog,
        stream_name="commits",
        selected=True,
    )
    tap2 = TapGitHub(config=repo_list_config, catalog=catalog.to_dict())
    # set pagination to 3 records/page (to reduce fixture size)
    for _, stream in tap2.streams.items():
        stream.MAX_PER_PAGE = 3  # type: ignore

    # mock all calls to the commits endpoint (this test should just use 1)
    mocked_url = "https://api.github.com/repos/MeltanoLabs/tap-github/commits"

    # pretend that the date is 2022-07-01T14:00:00 (just after the latest expected
    # commit in the repo (in the mock))
    with freeze_time("2022-07-01 14:00:00"):
        # non-mocked calls are forwarded to the actual server
        with requests_mock.Mocker(real_http=True) as m:
            m.get(mocked_url, json=mock_commits_response)
            # sync the stream, which will return the mocked response
            # that contains 3 records
            tap2.sync_all()

        # get the final state for the commits stream
        s = "not found"
        for name, stream in tap2.streams.items():
            if name == "commits":
                s = stream.stream_state
        # the bookmark should be the timestamp of the latest commit
        assert s == {
            "partitions": [
                {
                    "context": {"org": "MeltanoLabs", "repo": "tap-github"},
                    "replication_key": "commit_timestamp",
                    "replication_key_value": "2022-07-01T13:47:56Z",
                }
            ]
        }
