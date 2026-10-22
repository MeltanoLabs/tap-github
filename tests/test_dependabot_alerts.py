"""Tests for the Dependabot alerts stream."""

from unittest.mock import patch

import pytest
from requests import Response
from singer_sdk.exceptions import RetriableAPIError

from tap_github.repository_streams import DependabotAlertsStream
from tap_github.tap import TapGitHub

from .fixtures import repo_list_config  # noqa: F401


def test_dependabot_alerts_stream_is_incremental_and_cursor_paginated(repo_list_config):  # noqa: F811
    """Dependabot alerts use the repository endpoint and updated-time state."""
    stream = DependabotAlertsStream(TapGitHub(config=repo_list_config))

    assert stream.path == "/repos/{org}/{repo}/dependabot/alerts"
    assert stream.replication_key == "updated_at"
    assert stream.use_fake_since_parameter is True
    assert stream.use_cursor_pagination is True
    assert stream.selected_by_default is False
    assert stream.parent_stream_type is not None
    assert stream.parent_stream_type.__name__ == "RepositoryStream"
    assert "dependency" in stream.schema["properties"]
    assert "security_advisory" in stream.schema["properties"]
    assert "security_vulnerability" in stream.schema["properties"]
    assert "dependabot_alerts" in TapGitHub(config=repo_list_config).streams


def test_dependabot_alerts_stream_tolerates_unavailable_alerts_permission(
    repo_list_config,  # noqa: F811
):
    """Do not fail a sync when the token cannot access Dependabot alerts."""
    stream = DependabotAlertsStream(TapGitHub(config=repo_list_config))
    response = Response()
    response.status_code = 403
    response.url = (
        "https://api.github.com/repos/MeltanoLabs/tap-github/dependabot/alerts"
    )
    response.reason = "Forbidden"
    response._content = b'{"message": "Resource not accessible"}'

    stream.validate_response(response)

    assert stream.tolerated_http_errors == [404, 403]
    assert list(stream.parse_response(response)) == []


@pytest.mark.parametrize(
    ("message", "is_primary_rate_limit"),
    [
        ("API rate limit exceeded", True),
        ("You have exceeded a secondary rate limit", False),
    ],
)
def test_dependabot_alerts_stream_retries_rate_limits(
    repo_list_config,  # noqa: F811
    message,
    is_primary_rate_limit,
):
    """Retry rate-limit 403s even though permission-denied 403s are tolerated."""
    stream = DependabotAlertsStream(TapGitHub(config=repo_list_config))
    response = Response()
    response.status_code = 403
    response.url = (
        "https://api.github.com/repos/MeltanoLabs/tap-github/dependabot/alerts"
    )
    response.reason = "Forbidden"
    response._content = f'{{"message": "{message}"}}'.encode()

    with (
        patch.object(stream.authenticator, "get_next_auth_token") as rotate_token,
        patch("tap_github.client.time.sleep") as sleep,
        pytest.raises(RetriableAPIError, match="403 Client Error"),
    ):
        stream.validate_response(response)

    if is_primary_rate_limit:
        rotate_token.assert_called_once_with()
        sleep.assert_not_called()
    else:
        rotate_token.assert_not_called()
        sleep.assert_called_once()


def test_dependabot_alerts_stream_requests_updated_alerts_descending(repo_list_config):  # noqa: F811
    """Ensure pagination can stop after the saved incremental bookmark."""
    stream = DependabotAlertsStream(TapGitHub(config=repo_list_config))

    params = stream.get_url_params(
        {"org": "MeltanoLabs", "repo": "tap-github", "repo_id": 365087920},
        None,
    )

    assert params["sort"] == "updated"
    assert params["direction"] == "desc"
