from __future__ import annotations

import json
import re
from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup
from dateutil.parser import isoparse
from requests import Response
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.helpers import _catalog as cat_helpers
from singer_sdk.singerlib import Catalog

from tap_github.organization_streams import ProjectItemsStream
from tap_github.repository_streams import GitHubRestStream
from tap_github.scraping import parse_counter
from tap_github.tap import TapGitHub

from .fixtures import (  # noqa: F401
    alternative_sync_chidren,
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
        for repo in zip(repo_list_2_corrected, repo_list_2_ids, strict=False)
    ]
    tap = TapGitHub(config=repo_list_config)
    partitions = tap.streams["repositories"].partitions
    assert partitions == repo_list_context


def test_project_items_query_uses_default_page_size(organization_list_config):  # noqa: F811
    """Verify that project_items uses GitHub's max page size by default."""
    tap = TapGitHub(config=organization_list_config)
    stream = ProjectItemsStream(tap=tap)

    assert "items(first: 100, after: $nextPageCursor_0)" in stream.query


def test_project_items_query_uses_configured_page_size(organization_list_config):  # noqa: F811
    """Verify that project_items page size can be configured."""
    organization_list_config["stream_options"] = {
        "project_items": {
            "page_size": 50,
        },
    }
    tap = TapGitHub(config=organization_list_config)
    stream = ProjectItemsStream(tap=tap)

    assert "items(first: 50, after: $nextPageCursor_0)" in stream.query


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


def test_repository_child_context_includes_pull_request_capability(repo_list_config):  # noqa: F811
    """Verify repository context includes the pull request capability flag."""
    tap = TapGitHub(config=repo_list_config)
    repository_stream = tap.streams["repositories"]

    context = repository_stream.get_child_context(
        {
            "id": 123,
            "name": "issues-pi",
            "owner": {"login": "shop"},
            "has_pull_requests": False,
        },
        None,
    )

    assert context["has_pull_requests"] is False


def test_repository_child_context_defaults_pull_request_capability_to_enabled(
    repo_list_config,  # noqa: F811
):
    """Preserve existing behavior when GitHub does not return the capability flag."""
    tap = TapGitHub(config=repo_list_config)
    repository_stream = tap.streams["repositories"]

    context = repository_stream.get_child_context(
        {
            "id": 123,
            "name": "tap-github",
            "owner": {"login": "MeltanoLabs"},
        },
        None,
    )

    assert context["has_pull_requests"] is True


def test_repository_child_context_includes_issue_capability(repo_list_config):  # noqa: F811
    """Verify repository context includes the issue capability flag."""
    tap = TapGitHub(config=repo_list_config)
    repository_stream = tap.streams["repositories"]

    context = repository_stream.get_child_context(
        {
            "id": 123,
            "name": "no-issues",
            "owner": {"login": "octo-org"},
            "has_issues": False,
        },
        None,
    )

    assert context["has_issues"] is False


def test_repository_child_context_defaults_issue_capability_to_enabled(
    repo_list_config,  # noqa: F811
):
    """Preserve existing behavior when GitHub does not return the issue flag."""
    tap = TapGitHub(config=repo_list_config)
    repository_stream = tap.streams["repositories"]

    context = repository_stream.get_child_context(
        {
            "id": 123,
            "name": "tap-github",
            "owner": {"login": "MeltanoLabs"},
        },
        None,
    )

    assert context["has_issues"] is True


def test_pull_requests_stream_skips_repos_with_pull_requests_disabled(
    repo_list_config,  # noqa: F811
):
    """Do not call the pull requests API when repo metadata says it is disabled."""
    tap = TapGitHub(config=repo_list_config)
    pull_requests_stream = tap.streams["pull_requests"]
    context = {
        "org": "shop",
        "repo": "issues-pi",
        "repo_id": 123,
        "has_pull_requests": False,
    }

    with (
        patch.object(GitHubRestStream, "get_records") as get_records,
        patch.object(pull_requests_stream.logger, "debug") as log_debug,
    ):
        records = list(pull_requests_stream.get_records(context))

    assert records == []
    get_records.assert_not_called()
    log_debug.assert_called_once_with(
        "Repository shop/issues-pi: Pull requests not enabled, skipping API call",
    )


@pytest.mark.parametrize("has_pull_requests", [True, None, 0, "missing"])
def test_pull_requests_stream_delegates_when_pull_request_capability_is_not_false(
    repo_list_config,  # noqa: F811
    has_pull_requests,
):
    """Fail open when the capability flag is enabled, missing, or unknown."""
    tap = TapGitHub(config=repo_list_config)
    pull_requests_stream = tap.streams["pull_requests"]
    context = {
        "org": "MeltanoLabs",
        "repo": "tap-github",
        "repo_id": 123,
    }
    if has_pull_requests != "missing":
        context["has_pull_requests"] = has_pull_requests

    with patch.object(
        GitHubRestStream,
        "get_records",
        return_value=iter([{"id": 456}]),
    ) as get_records:
        records = list(pull_requests_stream.get_records(context))

    assert records == [{"id": 456}]
    get_records.assert_called_once_with(context)


def test_issue_comments_stream_skips_when_issues_and_pull_requests_are_disabled(
    repo_list_config,  # noqa: F811
):
    """Do not call the issue comments API when repo metadata says it is disabled."""
    tap = TapGitHub(config=repo_list_config)
    issue_comments_stream = tap.streams["issue_comments"]
    context = {
        "org": "octo-org",
        "repo": "no-issues-or-pull-requests",
        "repo_id": 123,
        "has_issues": False,
        "has_pull_requests": False,
    }

    with (
        patch.object(GitHubRestStream, "get_records") as get_records,
        patch.object(issue_comments_stream.logger, "debug") as log_debug,
    ):
        records = list(issue_comments_stream.get_records(context))

    assert records == []
    get_records.assert_not_called()
    log_debug.assert_called_once_with(
        "Repository octo-org/no-issues-or-pull-requests: "
        "Issues and pull requests not enabled, skipping issue comments API call",
    )


@pytest.mark.parametrize(
    ("has_issues", "has_pull_requests"),
    [
        (True, False),
        (None, False),
        (0, False),
        ("missing", False),
        (False, True),
        (False, None),
        (False, 0),
        (False, "missing"),
    ],
)
def test_issue_comments_stream_delegates_when_capabilities_are_not_false(
    repo_list_config,  # noqa: F811
    has_issues,
    has_pull_requests,
):
    """Fail open when either capability flag is enabled, missing, or unknown."""
    tap = TapGitHub(config=repo_list_config)
    issue_comments_stream = tap.streams["issue_comments"]
    context = {
        "org": "MeltanoLabs",
        "repo": "tap-github",
        "repo_id": 123,
    }
    if has_issues != "missing":
        context["has_issues"] = has_issues
    if has_pull_requests != "missing":
        context["has_pull_requests"] = has_pull_requests

    with patch.object(
        GitHubRestStream,
        "get_records",
        return_value=iter([{"id": 456}]),
    ) as get_records:
        records = list(issue_comments_stream.get_records(context))

    assert records == [{"id": 456}]
    get_records.assert_called_once_with(context)


@pytest.mark.parametrize(
    "stream_name",
    ["issues", "issue_events", "milestones", "labels"],
)
def test_issue_adjacent_streams_delegate_when_has_issues_is_false(
    repo_list_config,  # noqa: F811
    stream_name,
):
    """Document streams intentionally not gated by the repository Issues flag."""
    tap = TapGitHub(config=repo_list_config)
    stream = tap.streams[stream_name]
    context = {
        "org": "octo-org",
        "repo": "no-issues",
        "repo_id": 123,
        "has_issues": False,
    }

    with patch.object(
        GitHubRestStream,
        "get_records",
        return_value=iter([{"id": 789}]),
    ) as get_records:
        records = list(stream.get_records(context))

    assert records == [{"id": 789}]
    get_records.assert_called_once_with(context)


def test_pull_requests_stream_keeps_generic_404_retriable(
    repo_list_config,  # noqa: F811
):
    """Pull request gating must not broaden tolerated GitHub errors."""
    tap = TapGitHub(config=repo_list_config)
    pull_requests_stream = tap.streams["pull_requests"]
    response = Response()
    response.status_code = 404
    response.url = "https://api.github.com/repos/shop/issues-pi/pulls"
    response.reason = "Not Found"
    response._content = b'{"message": "Not Found"}'
    response.headers["X-GitHub-Request-Id"] = "request-id"

    with pytest.raises(RetriableAPIError, match="404 Client Error"):
        pull_requests_stream.validate_response(response)


def test_issue_comments_stream_keeps_generic_404_retriable(
    repo_list_config,  # noqa: F811
):
    """Issue comments gating must not broaden tolerated GitHub errors."""
    tap = TapGitHub(config=repo_list_config)
    issue_comments_stream = tap.streams["issue_comments"]
    response = Response()
    response.status_code = 404
    response.url = "https://api.github.com/repos/octo-org/no-issues/issues/comments"
    response.reason = "Not Found"
    response._content = b'{"message": "Not Found"}'
    response.headers["X-GitHub-Request-Id"] = "request-id"

    with pytest.raises(RetriableAPIError, match="404 Client Error"):
        issue_comments_stream.validate_response(response)


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
