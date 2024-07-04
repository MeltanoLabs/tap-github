import datetime
import logging
import os
import sys

import pytest

from ..utils.filter_stdout import FilterStdOutput

# Filter out singer output during tests
sys.stdout = FilterStdOutput(sys.stdout, r'{"type": ')  # type: ignore


@pytest.fixture()
def search_config():
    return {
        "metrics_log_level": "warning",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "searches": [
            {
                "name": "tap_something",
                "query": "tap-+language:Python",
            },
        ],
    }


@pytest.fixture()
def user_id_list_config(request):
    """Get a default list of usernames or pass your own by decorating your test with
    @pytest.mark.user_id_list(['ericboucher', 'aaronsteers'])
    """
    marker = request.node.get_closest_marker("user_id_list")
    user_id_list = [1, 2] if marker is None else marker.args[0]
    return {
        "metrics_log_level": "warning",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "user_ids": user_id_list,
        "rate_limit_buffer": 100,
    }


@pytest.fixture()
def organization_list_config(request):
    """Get a default list of organizations or pass your own by decorating your test with
    @pytest.mark.organization_list(['MeltanoLabs', 'oviohub'])
    """
    marker = request.node.get_closest_marker("organization_list")

    organization_list = ["MeltanoLabs"] if marker is None else marker.args[0]

    return {
        "metrics_log_level": "warning",
        "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
        "organizations": organization_list,
        "rate_limit_buffer": 100,
    }


def alternative_sync_children(self, child_context: dict, no_sync: bool = True) -> None:
    """Override for Stream._sync_children.
    Enabling us to use an ORG_LEVEL_TOKEN for the collaborators stream.
    """
    for child_stream in self.child_streams:
        # Use org:write access level credentials for collaborators stream
        if child_stream.name == "collaborators":
            """
            The `ORG_LEVEL_TOKEN` variable is used to store an organization-level GitHub API token. This token is used when syncing the "collaborators" stream, as it requires a higher level of access than the standard user token.

            If the `ORG_LEVEL_TOKEN` is not found in the environment, a warning is logged and the collaborators stream sync is skipped.
            """  # noqa: E501
            ORG_LEVEL_TOKEN = os.environ.get("ORG_LEVEL_TOKEN")  # noqa: N806
            # TODO - Fix collaborators tests, likely by mocking API responses directly.
            # Currently we have to bypass them as they are failing frequently.
            if not ORG_LEVEL_TOKEN or no_sync:
                logging.warning(
                    'No "ORG_LEVEL_TOKEN" found. Skipping collaborators stream sync.',
                )
                continue
            SAVED_GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")  # noqa: N806
            os.environ["GITHUB_TOKEN"] = ORG_LEVEL_TOKEN
            child_stream.sync(context=child_context)
            os.environ["GITHUB_TOKEN"] = SAVED_GITHUB_TOKEN or ""
            continue

        # default behavior:
        if child_stream.selected or child_stream.has_selected_descendents:
            child_stream.sync(context=child_context)
