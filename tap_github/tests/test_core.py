"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_github.tap import TapGitHub

SEARCH_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "searches": [
        {
            "name": "tap_something",
            "query": "tap-+language:Python",
        }
    ],
}

REPO_LIST_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "repositories": ["octocat/hello-world"],
}

# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests_for_search_mode():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapGitHub, config=SEARCH_CONFIG)
    for test in tests:
        test()


def test_standard_tap_tests_for_repo_list_mode():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapGitHub, config=REPO_LIST_CONFIG)
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
