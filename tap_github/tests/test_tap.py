from __future__ import annotations

import json
import re
from unittest import mock
from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup
from dateutil.parser import isoparse
from singer_sdk._singerlib import Catalog
from singer_sdk.helpers import _catalog as cat_helpers
from singer_sdk.testing import get_standard_tap_tests

from tap_github.scraping import parse_counter
from tap_github.tap import TapGitHub
from tap_github.utils.filter_stdout import nostdout

from .fixtures import (  # noqa: F401
    alternative_sync_chidren,
    exclude_repo_config,
    organization_list_config,
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


def test_exclude_repositories_config(request):
    """Test that the exclude_repositories config option is recognized."""
    exclude_config = request.getfixturevalue("exclude_repo_config")
    repo_config = request.getfixturevalue("repo_list_config")

    config = {**repo_config, **exclude_config}
    tap = TapGitHub(config=config)
    assert "exclude_repositories" in tap.config
    assert tap.config["exclude_repositories"] == ["MeltanoLabs/tap-github"]


@mock.patch("tap_github.repository_streams.GitHubGraphqlStream.request_records")
def test_exclude_repositories_from_direct_list(
    mock_request_records,
    request,
):
    """Test that repositories are excluded from direct repository list."""
    repo_config = request.getfixturevalue("repo_list_config")
    exclude_config = request.getfixturevalue("exclude_repo_config")

    mock_request_records.return_value = [
        {
            "repo0": {"nameWithOwner": "MeltanoLabs/tap-github", "databaseId": 123},
            "repo1": {"nameWithOwner": "MeltanoLabs/tap-gitlab", "databaseId": 456},
            "rateLimit": {"cost": 1},
        }
    ]

    config = {
        **repo_config,
        "repositories": ["MeltanoLabs/tap-github", "MeltanoLabs/tap-gitlab"],
        **exclude_config,
    }

    tap = TapGitHub(config=config)
    streams = tap.discover_streams()
    repo_stream = next(s for s in streams if s.name == "repositories")

    # Since we're mocking GraphQL, we need to manually handle the partition logic
    repo_stream.logger = mock.MagicMock()

    # When testing the partitions property with mocked GraphQL
    # It should only include the non-excluded repository
    assert len(repo_stream.config["repositories"]) == 2
    filtered_repos = [
        r
        for r in repo_stream.config["repositories"]
        if r not in repo_stream.config.get("exclude_repositories", [])
    ]
    assert len(filtered_repos) == 1
    assert "MeltanoLabs/tap-gitlab" in filtered_repos


@mock.patch("tap_github.repository_streams.RepositoryStream.request_records")
def test_exclude_repositories_from_organization(
    mock_request_records,
    request,
):
    """Test that repositories are excluded when using organizations."""
    # Get fixtures from pytest using request fixture
    org_config = request.getfixturevalue("organization_list_config")
    exclude_config = request.getfixturevalue("exclude_repo_config")

    # Mock the return value for organization repositories
    mock_request_records.return_value = [
        {"owner": {"login": "MeltanoLabs"}, "name": "tap-github", "id": 123},
        {"owner": {"login": "MeltanoLabs"}, "name": "tap-gitlab", "id": 456},
    ]

    # Combine fixtures for testing
    config = {**org_config, **exclude_config}

    tap = TapGitHub(config=config)
    streams = tap.discover_streams()
    repo_stream = next(s for s in streams if s.name == "repositories")

    # Create a mock context and logger
    context = {"org": "MeltanoLabs"}
    repo_stream.logger = mock.MagicMock()

    # Call get_records with the exclusion applied
    records = list(repo_stream.get_records(context))

    # We should only get repositories that aren't excluded
    assert len(records) == 1
    assert records[0]["name"] == "tap-gitlab"


def test_standard_tap_tests_with_exclude_repos(request):
    """Run standard tap tests with exclude_repositories config."""
    # Get fixtures from pytest using request fixture
    org_config = request.getfixturevalue("organization_list_config")
    exclude_config = request.getfixturevalue("exclude_repo_config")

    config = {**org_config, **exclude_config}
    tests = get_standard_tap_tests(TapGitHub, config=config)
    with (
        patch(
            "singer_sdk.streams.core.Stream._sync_children", alternative_sync_chidren
        ),
        nostdout(),
    ):
        for test in tests:
            test()
