"""Tests for the Dependabot alerts stream."""

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
    assert stream.parent_stream_type is not None
    assert stream.parent_stream_type.__name__ == "RepositoryStream"
    assert "dependency" in stream.schema["properties"]
    assert "security_advisory" in stream.schema["properties"]
    assert "security_vulnerability" in stream.schema["properties"]
    assert "dependabot_alerts" in TapGitHub(config=repo_list_config).streams


def test_dependabot_alerts_stream_requests_updated_alerts_descending(repo_list_config):  # noqa: F811
    """Ensure pagination can stop after the saved incremental bookmark."""
    stream = DependabotAlertsStream(TapGitHub(config=repo_list_config))

    params = stream.get_url_params(
        {"org": "MeltanoLabs", "repo": "tap-github", "repo_id": 365087920},
        None,
    )

    assert params["sort"] == "updated"
    assert params["direction"] == "desc"
