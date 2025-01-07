from __future__ import annotations

import json
import re
from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup
from dateutil.parser import isoparse
from singer_sdk._singerlib import Catalog
from singer_sdk.helpers import _catalog as cat_helpers

from tap_github.scraping import parse_counter
from tap_github.tap import TapGitHub

from .fixtures import (  # noqa: F401
    alternative_sync_chidren,
    repo_list_config,
    username_list_config,
)

repo_list_2 = [
    "MeltanoLabs/tap-github",
    # mistype the repo name so we can check that the tap corrects it
    "MeltanoLabs/Tap-GitLab",
    # mistype the org
    "meltanolabs/target-athena",
    # a repo that does not exist at all
    # this one has no matching record below as it should be removed
    # from the list by the TempStream
    "brokenOrg/does_not_exist",
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
def test_validate_repo_list_config(repo_list_config):  # noqa: F811
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


def run_tap_with_config(
    capsys, config_obj: dict, skip_stream: str | None, single_stream: str | None
) -> str:
    """
    Run the tap with the given config and capture stdout, optionally
    skipping a stream (this is meant to be the top level stream), or
    running a single one.
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
    elif single_stream is not None:
        cat_helpers.deselect_all_streams(catalog)
        cat_helpers.set_catalog_stream_selected(catalog, "repositories", selected=True)
        cat_helpers.set_catalog_stream_selected(
            catalog, stream_name=single_stream, selected=True
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
    capsys,
    repo_list_config,  # noqa: F811
    skip_parent_streams,
):
    """
    Discover the catalog, and request 2 repository records.
    The test is parametrized to run twice, with and without
    syncing the top level `repositories` stream.
    """
    repo_list_config["skip_parent_streams"] = skip_parent_streams
    captured_out = run_tap_with_config(
        capsys,
        repo_list_config,
        "repositories" if skip_parent_streams else None,
        single_stream=None,
    )
    # Verify we got the right number of records
    # one per repo in the list only if we sync the "repositories" stream, 0 if not
    assert captured_out.count('{"type":"RECORD","stream":"repositories"') == len(
        repo_list_2_ids * (not skip_parent_streams)
    )
    # check that the tap corrects invalid case in config input
    assert '"repo": "Tap-GitLab"' not in captured_out
    assert '"org": "meltanolabs"' not in captured_out


@pytest.mark.repo_list(["MeltanoLabs/tap-github"])
def test_last_state_message_is_valid(capsys, repo_list_config):  # noqa: F811
    """
    Validate that the last state message is not a temporary one and contains the
    expected values for a stream with overridden state partitioning keys.
    Run this on a single repo to avoid having to filter messages too much.
    """
    repo_list_config["skip_parent_streams"] = True
    captured_out = run_tap_with_config(
        capsys, repo_list_config, "repositories", single_stream=None
    )
    # capture the messages we're interested in
    state_messages = re.findall(r'{"type":"STATE","value":.*}', captured_out)
    issue_comments_records = re.findall(
        r'{"type":"RECORD","stream":"issue_comments",.*}', captured_out
    )
    assert state_messages is not None
    last_state_msg = state_messages[-1]

    # make sure we don't have a temporary state message at the very end
    assert "progress_markers" not in last_state_msg

    last_state = json.loads(last_state_msg)
    last_state_updated_at = isoparse(
        last_state["value"]["bookmarks"]["issue_comments"]["partitions"][0][
            "replication_key_value"
        ]
    )
    latest_updated_at = max(
        isoparse(json.loads(record)["record"]["updated_at"])
        for record in issue_comments_records
    )
    assert last_state_updated_at == latest_updated_at


# case is incorrect on purpose, so we can check that the tap corrects it
# and run the test twice, with and without syncing the `users` stream
@pytest.mark.parametrize("skip_parent_streams", [False, True])
@pytest.mark.username_list(["EricBoucher", "aaRONsTeeRS"])
def test_get_a_user_in_user_usernames_mode(
    capsys,
    username_list_config,  # noqa: F811
    skip_parent_streams,
):
    """
    Discover the catalog, and request 2 repository records
    """
    username_list_config["skip_parent_streams"] = skip_parent_streams
    captured_out = run_tap_with_config(
        capsys,
        username_list_config,
        "users" if skip_parent_streams else None,
        single_stream=None,
    )
    # Verify we got the right number of records:
    # one per user in the list if we sync the root stream, 0 otherwise
    assert captured_out.count('{"type":"RECORD","stream":"users"') == len(
        username_list_config["user_usernames"] * (not skip_parent_streams)
    )
    # these 2 are inequalities as number will keep changing :)
    assert captured_out.count('{"type":"RECORD","stream":"starred"') > 150
    assert captured_out.count('{"type":"RECORD","stream":"user_contributed_to"') > 25
    assert '{"username":"aaronsteers"' in captured_out
    assert '{"username":"aaRONsTeeRS"' not in captured_out
    assert '{"username":"EricBoucher"' not in captured_out


@pytest.mark.repo_list(["torvalds/linux"])
def test_large_list_of_contributors(capsys, repo_list_config):  # noqa: F811
    """
    Check that the github error message for very large lists of contributors
    is handled properly (does not return any records).
    """
    captured_out = run_tap_with_config(
        capsys, repo_list_config, skip_stream=None, single_stream="contributors"
    )
    assert captured_out.count('{"type":"RECORD","stream":"contributors"') == 0


def test_web_tag_parse_counter():
    """
    Check that the parser runs ok on various forms of counters.
    Used in extra_metrics stream.
    """
    # regular int
    tag = BeautifulSoup(
        '<span id="issues-repo-tab-count" title="57" class="Counter">57</span>',
        "html.parser",
    ).span
    assert parse_counter(tag) == 57

    # 2k
    tag = BeautifulSoup(
        '<span id="issues-repo-tab-count" title="2028" class="Counter">2k</span>',
        "html.parser",
    ).span
    assert parse_counter(tag) == 2028

    # 5k+. The real number is not available in the page, use this approx value
    tag = BeautifulSoup(
        '<span id="issues-repo-tab-count" title="5,000+" class="Counter">5k+</span>',
        "html.parser",
    ).span
    assert parse_counter(tag) == 5_000
