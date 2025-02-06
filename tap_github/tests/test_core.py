"""Tests standard tap features using the built-in SDK tests library."""

import logging
import os
from unittest import mock
from unittest.mock import patch

from singer_sdk.testing import get_standard_tap_tests

from tap_github.tap import TapGitHub
from tap_github.utils.filter_stdout import nostdout

from .fixtures import (  # noqa: F401
    alternative_sync_chidren,
    organization_list_config,
    repo_list_config,
    search_config,
    username_list_config,
)


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests_for_search_mode(search_config):  # noqa: F811
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapGitHub, config=search_config)
    with (
        patch(
            "singer_sdk.streams.core.Stream._sync_children", alternative_sync_chidren
        ),
        nostdout(),
    ):
        for test in tests:
            test()


def test_standard_tap_tests_for_repo_list_mode(repo_list_config):  # noqa: F811
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapGitHub, config=repo_list_config)
    with (
        patch(
            "singer_sdk.streams.core.Stream._sync_children", alternative_sync_chidren
        ),
        nostdout(),
    ):
        for test in tests:
            test()


def test_standard_tap_tests_for_username_list_mode(username_list_config):  # noqa: F811
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapGitHub, config=username_list_config)
    with nostdout():
        for test in tests:
            test()


# This token needs to have read:org access for the organization listed in fixtures.py
# Default is "MeltanoLabs"
ORG_LEVEL_TOKEN = os.environ.get("ORG_LEVEL_TOKEN")


@mock.patch.dict(os.environ, {"GITHUB_TOKEN": ORG_LEVEL_TOKEN or ""})
def test_standard_tap_tests_for_organization_list_mode(organization_list_config):  # noqa: F811
    """Run standard tap tests from the SDK."""
    if not ORG_LEVEL_TOKEN:
        logging.warning('No "ORG_LEVEL_TOKEN" found. Skipping organization tap tests.')
        return
    tests = get_standard_tap_tests(TapGitHub, config=organization_list_config)
    with nostdout():
        for test in tests:
            test()
