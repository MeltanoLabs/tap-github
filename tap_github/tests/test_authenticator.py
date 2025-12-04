import logging
import re
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests
import requests.exceptions
from singer_sdk.streams import RESTStream

from tap_github.authenticator import (
    AppTokenManager,
    GitHubTokenAuthenticator,
    PersonalTokenManager,
    TokenManager,
)


def _now():
    return datetime.now(tz=timezone.utc)


class TestTokenManager:
    def test_default_rate_limits(self):
        token_manager = TokenManager("mytoken", rate_limit_buffer=700)

        assert token_manager.rate_limit == 5000
        assert token_manager.rate_limit_remaining == 5000
        assert token_manager.rate_limit_reset is None
        assert token_manager.rate_limit_used == 0
        assert token_manager.rate_limit_buffer == 700

        token_manager_2 = TokenManager("mytoken")
        assert token_manager_2.rate_limit_buffer == 1000

    def test_update_rate_limit(self):
        mock_response_headers = {
            "X-RateLimit-Limit": "5000",
            "X-RateLimit-Remaining": "4999",
            "X-RateLimit-Reset": "1372700873",
            "X-RateLimit-Used": "1",
        }

        token_manager = TokenManager("mytoken")
        token_manager.update_rate_limit(mock_response_headers)

        assert token_manager.rate_limit == 5000
        assert token_manager.rate_limit_remaining == 4999
        assert token_manager.rate_limit_reset == datetime(
            2013,
            7,
            1,
            17,
            47,
            53,
            tzinfo=timezone.utc,
        )
        assert token_manager.rate_limit_used == 1

    def test_is_valid_token_successful(self):
        with patch("requests.get") as mock_get:
            mock_response = mock_get.return_value
            mock_response.raise_for_status.return_value = None

            token_manager = TokenManager("validtoken")

            assert token_manager.is_valid_token()
            mock_get.assert_called_once_with(
                url="https://api.github.com/rate_limit",
                headers={"Authorization": "token validtoken"},
            )

    def test_is_valid_token_failure(self, caplog: pytest.LogCaptureFixture):
        with patch("requests.get") as mock_get:
            # Setup for a failed request
            mock_response = mock_get.return_value
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
            mock_response.status_code = 401
            mock_response.content = b"Unauthorized Access"
            mock_response.reason = "Unauthorized"

            token_manager = TokenManager("invalidtoken")

            with caplog.at_level(logging.WARNING):
                assert not token_manager.is_valid_token()

            assert "401" in caplog.text

    def test_has_calls_remaining_succeeds_if_token_never_used(self):
        token_manager = TokenManager("mytoken")
        assert token_manager.has_calls_remaining()

    def test_has_calls_remaining_succeeds_if_lots_remaining(self):
        mock_response_headers = {
            "X-RateLimit-Limit": "5000",
            "X-RateLimit-Remaining": "4999",
            "X-RateLimit-Reset": "1372700873",
            "X-RateLimit-Used": "1",
        }

        token_manager = TokenManager("mytoken")
        token_manager.update_rate_limit(mock_response_headers)

        assert token_manager.has_calls_remaining()

    def test_has_calls_remaining_succeeds_if_reset_time_reached(self):
        mock_response_headers = {
            "X-RateLimit-Limit": "5000",
            "X-RateLimit-Remaining": "1",
            "X-RateLimit-Reset": "1372700873",
            "X-RateLimit-Used": "4999",
        }

        token_manager = TokenManager("mytoken", rate_limit_buffer=1000)
        token_manager.update_rate_limit(mock_response_headers)

        assert token_manager.has_calls_remaining()

    def test_has_calls_remaining_fails_if_few_calls_remaining_and_reset_time_not_reached(  # noqa: E501
        self,
    ):
        mock_response_headers = {
            "X-RateLimit-Limit": "5000",
            "X-RateLimit-Remaining": "1",
            "X-RateLimit-Reset": str(int((_now() + timedelta(days=100)).timestamp())),
            "X-RateLimit-Used": "4999",
        }

        token_manager = TokenManager("mytoken", rate_limit_buffer=1000)
        token_manager.update_rate_limit(mock_response_headers)

        assert not token_manager.has_calls_remaining()


class TestAppTokenManager:
    def test_initialization_with_3_part_env_key(self):
        with patch.object(AppTokenManager, "claim_token", return_value=None):
            token_manager = AppTokenManager("12345;;key\\ncontent;;67890")
            assert token_manager.github_app_id == "12345"
            assert token_manager.github_private_key == "key\ncontent"
            assert token_manager.github_installation_id == "67890"

    def test_initialization_with_2_part_env_key(self):
        with patch.object(AppTokenManager, "claim_token", return_value=None):
            token_manager = AppTokenManager("12345;;key\\ncontent")
            assert token_manager.github_app_id == "12345"
            assert token_manager.github_private_key == "key\ncontent"
            assert token_manager.github_installation_id is None

    def test_initialization_with_malformed_env_key(self):
        expected_error_expression = re.escape(
            "GITHUB_APP_PRIVATE_KEY could not be parsed. The expected format is "
            '":app_id:;;-----BEGIN RSA PRIVATE KEY-----\\n_YOUR_P_KEY_\\n-----END RSA PRIVATE KEY-----"'  # noqa: E501
        )
        with pytest.raises(ValueError, match=expected_error_expression):
            AppTokenManager("12345key\\ncontent")

    def test_generate_token_with_invalid_credentials(self):
        with (
            patch.object(AppTokenManager, "is_valid_token", return_value=False),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("some_token", MagicMock()),
            ),
        ):
            token_manager = AppTokenManager("12345;;key\\ncontent;;67890")
            assert token_manager.token is None
            assert token_manager.token_expires_at is None

    def test_successful_token_generation(self):
        token_time = MagicMock()
        with (
            patch.object(AppTokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("valid_token", token_time),
            ),
        ):
            token_manager = AppTokenManager("12345;;key\\ncontent;;67890")
            token_manager.claim_token()
            assert token_manager.token == "valid_token"
            assert token_manager.token_expires_at == token_time

    def test_has_calls_remaining_regenerates_a_token_if_close_to_expiry(
        self,
        caplog: pytest.LogCaptureFixture,
    ):
        unexpired_time = _now() + timedelta(days=1)
        expired_time = _now() - timedelta(days=1)
        with (
            patch.object(AppTokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("valid_token", unexpired_time),
            ),
        ):
            mock_response_headers = {
                "X-RateLimit-Limit": "5000",
                "X-RateLimit-Remaining": "4999",
                "X-RateLimit-Reset": "1372700873",
                "X-RateLimit-Used": "1",
            }

            token_manager = AppTokenManager("12345;;key\\ncontent;;67890")
            token_manager.token_expires_at = expired_time
            token_manager.update_rate_limit(mock_response_headers)

            with caplog.at_level(logging.INFO):
                assert token_manager.has_calls_remaining()

            # calling has_calls_remaining() will trigger the token generation function to be called again,  # noqa: E501
            # so token_expires_at should have been reset back to the mocked unexpired_time  # noqa: E501
            assert token_manager.token_expires_at == unexpired_time
            assert "GitHub app token refresh succeeded." in caplog.text

    def test_has_calls_remaining_logs_warning_if_token_regeneration_fails(
        self, caplog: pytest.LogCaptureFixture
    ):
        unexpired_time = _now() + timedelta(days=1)
        expired_time = _now() - timedelta(days=1)
        with (
            patch.object(
                AppTokenManager, "is_valid_token", return_value=True
            ) as mock_is_valid,
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("valid_token", unexpired_time),
            ),
        ):
            mock_response_headers = {
                "X-RateLimit-Limit": "5000",
                "X-RateLimit-Remaining": "4999",
                "X-RateLimit-Reset": "1372700873",
                "X-RateLimit-Used": "1",
            }

            token_manager = AppTokenManager("12345;;key\\ncontent;;67890")
            token_manager.token_expires_at = expired_time
            token_manager.update_rate_limit(mock_response_headers)

            mock_is_valid.return_value = False
            with caplog.at_level(logging.WARNING):
                assert not token_manager.has_calls_remaining()
                assert "GitHub app token refresh failed." in caplog.text

    def test_has_calls_remaining_succeeds_if_token_new_and_never_used(self):
        unexpired_time = _now() + timedelta(days=1)
        with (
            patch.object(AppTokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("valid_token", unexpired_time),
            ),
        ):
            token_manager = AppTokenManager("12345;;key\\ncontent;;67890")
            assert token_manager.has_calls_remaining()

    def test_has_calls_remaining_succeeds_if_time_and_requests_left(self):
        unexpired_time = _now() + timedelta(days=1)
        with (
            patch.object(AppTokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("valid_token", unexpired_time),
            ),
        ):
            mock_response_headers = {
                "X-RateLimit-Limit": "5000",
                "X-RateLimit-Remaining": "4999",
                "X-RateLimit-Reset": "1372700873",
                "X-RateLimit-Used": "1",
            }

            token_manager = AppTokenManager("12345;;key\\ncontent;;67890")
            token_manager.update_rate_limit(mock_response_headers)

            assert token_manager.has_calls_remaining()

    def test_has_calls_remaining_succeeds_if_time_left_and_reset_time_reached(self):
        unexpired_time = _now() + timedelta(days=1)
        with (
            patch.object(AppTokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("valid_token", unexpired_time),
            ),
        ):
            mock_response_headers = {
                "X-RateLimit-Limit": "5000",
                "X-RateLimit-Remaining": "1",
                "X-RateLimit-Reset": "1372700873",
                "X-RateLimit-Used": "4999",
            }

            token_manager = AppTokenManager(
                "12345;;key\\ncontent;;67890", rate_limit_buffer=1000
            )
            token_manager.update_rate_limit(mock_response_headers)

            assert token_manager.has_calls_remaining()

    def test_has_calls_remaining_fails_if_time_left_and_few_calls_remaining_and_reset_time_not_reached(  # noqa: E501
        self,
    ):
        unexpired_time = _now() + timedelta(days=1)
        with (
            patch.object(AppTokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("valid_token", unexpired_time),
            ),
        ):
            mock_response_headers = {
                "X-RateLimit-Limit": "5000",
                "X-RateLimit-Remaining": "1",
                "X-RateLimit-Reset": str(
                    int((_now() + timedelta(days=100)).timestamp())
                ),
                "X-RateLimit-Used": "4999",
            }

            token_manager = AppTokenManager(
                "12345;;key\\ncontent;;67890", rate_limit_buffer=1000
            )
            token_manager.update_rate_limit(mock_response_headers)

        assert not token_manager.has_calls_remaining()


@pytest.fixture
def mock_stream():
    stream = MagicMock(spec=RESTStream)
    stream.tap_name = "tap_github"
    stream.config = {"rate_limit_buffer": 5}
    return stream


class TestGitHubTokenAuthenticator:
    @staticmethod
    def _count_total_tokens(token_managers):
        """Count total tokens across all organizations."""
        return sum(len(tokens) for tokens in token_managers.values())

    @staticmethod
    def _flatten_token_managers(token_managers):
        """Flatten token_managers dict to a list of all TokenManager objects."""
        return [tm for tokens in token_managers.values() for tm in tokens]

    def test_prepare_tokens_returns_empty_if_none_found(self, mock_stream):
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={"GITHUB_TLJKJFDS": "gt1"},
            ),
            patch.object(PersonalTokenManager, "is_valid_token", return_value=True),
        ):
            auth = GitHubTokenAuthenticator.from_stream(stream=mock_stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 0

    def test_config_auth_token_only(self, mock_stream):
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={"OTHER_TOKEN": "blah", "NOT_THE_RIGHT_TOKEN": "meh"},
            ),
            patch.object(PersonalTokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update({"auth_token": "gt5"})
            auth = GitHubTokenAuthenticator.from_stream(stream=stream)
            token_managers = auth.prepare_tokens()

            assert self._count_total_tokens(token_managers) == 1
            assert token_managers[None][0].token == "gt5"

    def test_config_additional_auth_tokens_only(self, mock_stream):
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={"OTHER_TOKEN": "blah", "NOT_THE_RIGHT_TOKEN": "meh"},
            ),
            patch.object(PersonalTokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update({"additional_auth_tokens": ["gt7", "gt8", "gt9"]})
            auth = GitHubTokenAuthenticator.from_stream(stream=stream)
            token_managers = auth.prepare_tokens()

            assert self._count_total_tokens(token_managers) == 3
            all_tokens = self._flatten_token_managers(token_managers)
            assert sorted({tm.token for tm in all_tokens}) == ["gt7", "gt8", "gt9"]

    def test_env_personal_tokens_only(self, mock_stream):
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={
                    "GITHUB_TOKEN1": "gt1",
                    "GITHUB_TOKENxyz": "gt2",
                    "OTHER_TOKEN": "blah",
                },
            ),
            patch.object(PersonalTokenManager, "is_valid_token", return_value=True),
        ):
            auth = GitHubTokenAuthenticator.from_stream(stream=mock_stream)
            token_managers = auth.prepare_tokens()

            assert self._count_total_tokens(token_managers) == 2
            all_tokens = self._flatten_token_managers(token_managers)
            assert sorted({tm.token for tm in all_tokens}) == ["gt1", "gt2"]

    def test_config_app_keys(self, mock_stream):
        def generate_token_mock(app_id, private_key, installation_id):
            return (f"installationtokenfor{app_id}", MagicMock())

        with (
            patch.object(TokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                side_effect=generate_token_mock,
            ),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "auth_token": "gt5",
                    "additional_auth_tokens": ["gt7", "gt8", "gt9"],
                    "auth_app_keys": [
                        "123;;gak1;;13",
                        "456;;gak1;;46",
                        "789;;gak1;;79",
                    ],
                }
            )
            auth = GitHubTokenAuthenticator.from_stream(stream=stream)
            token_managers = auth.prepare_tokens()

            assert self._count_total_tokens(token_managers) == 7

            all_tokens = self._flatten_token_managers(token_managers)
            app_token_managers = {
                tm for tm in all_tokens if isinstance(tm, AppTokenManager)
            }
            assert len(app_token_managers) == 3

            app_tokens = {tm.token for tm in app_token_managers}
            assert app_tokens == {
                "installationtokenfor123",
                "installationtokenfor456",
                "installationtokenfor789",
            }

    def test_env_app_key_only(self, mock_stream):
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={
                    "GITHUB_APP_PRIVATE_KEY": "123;;key",
                    "OTHER_TOKEN": "blah",
                },
            ),
            patch.object(AppTokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("installationtoken12345", MagicMock()),
            ),
        ):
            auth = GitHubTokenAuthenticator.from_stream(stream=mock_stream)
            token_managers = auth.prepare_tokens()

            assert self._count_total_tokens(token_managers) == 1
            assert token_managers[None][0].token == "installationtoken12345"

    def test_all_token_types(self, mock_stream):
        # Expectations:
        # - the presence of additional_auth_tokens causes personal tokens in the environment to be ignored.  # noqa: E501
        # - the other types all coexist
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={
                    "GITHUB_TOKEN1": "gt1",
                    "GITHUB_TOKENxyz": "gt2",
                    "GITHUB_APP_PRIVATE_KEY": "123;;key;;install_id",
                    "OTHER_TOKEN": "blah",
                },
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("installationtoken12345", MagicMock()),
            ),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "auth_token": "gt5",
                    "additional_auth_tokens": ["gt7", "gt8", "gt9"],
                }
            )
            auth = GitHubTokenAuthenticator.from_stream(stream=stream)
            token_managers = auth.prepare_tokens()

            assert self._count_total_tokens(token_managers) == 5
            all_tokens = self._flatten_token_managers(token_managers)
            assert sorted({tm.token for tm in all_tokens}) == [
                "gt5",
                "gt7",
                "gt8",
                "gt9",
                "installationtoken12345",
            ]

    def test_all_token_types_except_additional_auth_tokens(self, mock_stream):
        # Expectations:
        # - in the absence of additional_auth_tokens, all the other types can coexist
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={
                    "GITHUB_TOKEN1": "gt1",
                    "GITHUB_TOKENxyz": "gt2",
                    "GITHUB_APP_PRIVATE_KEY": "123;;key;;install_id",
                    "OTHER_TOKEN": "blah",
                },
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("installationtoken12345", MagicMock()),
            ),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "auth_token": "gt5",
                }
            )
            auth = GitHubTokenAuthenticator.from_stream(stream=stream)
            token_managers = auth.prepare_tokens()

            assert self._count_total_tokens(token_managers) == 4
            all_tokens = self._flatten_token_managers(token_managers)
            assert sorted({tm.token for tm in all_tokens}) == [
                "gt1",
                "gt2",
                "gt5",
                "installationtoken12345",
            ]

    def test_auth_token_and_additional_auth_tokens_deduped(self, mock_stream):
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={
                    "GITHUB_TOKEN1": "gt1",
                    "GITHUB_TOKENxyz": "gt2",
                    "OTHER_TOKEN": "blah",
                },
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("installationtoken12345", MagicMock()),
            ),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "auth_token": "gt1",
                    "additional_auth_tokens": ["gt1", "gt1", "gt8", "gt8", "gt9"],
                }
            )
            auth = GitHubTokenAuthenticator.from_stream(stream=stream)
            token_managers = auth.prepare_tokens()

            assert self._count_total_tokens(token_managers) == 3
            all_tokens = self._flatten_token_managers(token_managers)
            assert sorted({tm.token for tm in all_tokens}) == ["gt1", "gt8", "gt9"]

    def test_auth_token_and_env_tokens_deduped(self, mock_stream):
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={
                    "GITHUB_TOKEN1": "gt1",
                    "GITHUB_TOKENa": "gt2",
                    "GITHUB_TOKENxyz": "gt2",
                    "OTHER_TOKEN": "blah",
                },
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("installationtoken12345", MagicMock()),
            ),
        ):
            stream = mock_stream
            stream.config.update({"auth_token": "gt1"})
            auth = GitHubTokenAuthenticator.from_stream(stream=stream)
            token_managers = auth.prepare_tokens()

            assert self._count_total_tokens(token_managers) == 2
            all_tokens = self._flatten_token_managers(token_managers)
            assert sorted({tm.token for tm in all_tokens}) == ["gt1", "gt2"]

    def test_handle_error_if_app_key_invalid(
        self,
        mock_stream,
        caplog: pytest.LogCaptureFixture,
    ):
        # Confirm expected behaviour if an error is raised while setting up the app token manager:  # noqa: E501
        # - don"t crash
        # - print the error as a warning
        # - continue with any other obtained tokens
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={"GITHUB_APP_PRIVATE_KEY": "123garbagekey"},
            ),
            patch("tap_github.authenticator.AppTokenManager") as mock_app_manager,
        ):
            mock_app_manager.side_effect = ValueError("Invalid key format")

            auth = GitHubTokenAuthenticator.from_stream(stream=mock_stream)
            auth.prepare_tokens()

            msg = "An error was thrown while preparing an app token: Invalid key format"
            with caplog.at_level(logging.WARNING):
                assert msg in caplog.text

    def test_exclude_generated_app_token_if_invalid(self, mock_stream):
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={"GITHUB_APP_PRIVATE_KEY": "123;;key"},
            ),
            patch.object(AppTokenManager, "is_valid_token", return_value=False),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("installationtoken12345", MagicMock()),
            ),
        ):
            auth = GitHubTokenAuthenticator.from_stream(stream=mock_stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 0

    def test_prepare_tokens_returns_empty_if_all_tokens_invalid(self, mock_stream):
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={
                    "GITHUB_TOKEN1": "gt1",
                    "GITHUB_APP_PRIVATE_KEY": "123;;key",
                },
            ),
            patch.object(PersonalTokenManager, "is_valid_token", return_value=False),
            patch.object(AppTokenManager, "is_valid_token", return_value=False),
            patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("installationtoken12345", MagicMock()),
            ),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "auth_token": "gt5",
                    "additional_auth_tokens": ["gt7", "gt8", "gt9"],
                }
            )
            auth = GitHubTokenAuthenticator.from_stream(stream=stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 0

    def test_get_next_auth_token_rotates_within_org(self, mock_stream):
        """Test that token rotation works correctly with org-specific token pools."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "org_auth_app_keys": {
                        "acme-corp": ["app1;;key1", "app2;;key2"],
                    }
                }
            )

            def mock_generate_token(app_id, private_key, installation_id):
                return (f"token_for_{app_id}", MagicMock())

            with patch(
                "tap_github.authenticator.generate_app_access_token",
                side_effect=mock_generate_token,
            ):
                auth = GitHubTokenAuthenticator.from_stream(stream=stream)

                # Get the two tokens for acme-corp org
                org_tokens = auth.token_managers["acme-corp"]
                assert len(org_tokens) == 2

                # Set current org and active token
                auth.current_organization = "acme-corp"
                auth.active_token = org_tokens[0]

                # Mock first token as exhausted, second as available
                with (
                    patch.object(
                        org_tokens[0], "has_calls_remaining", return_value=False
                    ),
                    patch.object(
                        org_tokens[1], "has_calls_remaining", return_value=True
                    ),
                ):
                    initial_token = auth.active_token

                    # Should rotate to second token
                    auth.get_next_auth_token()

                    assert auth.active_token != initial_token
                    assert auth.active_token == org_tokens[1]
                    assert auth.current_organization == "acme-corp"

    def test_get_next_auth_token_falls_back_to_org_agnostic(self, mock_stream):
        """Test that token rotation falls back to org-agnostic tokens."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "additional_auth_tokens": ["personal_token"],
                    "org_auth_app_keys": {
                        "acme-corp": ["app1;;key1"],
                    },
                }
            )

            with patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("org_app_token", MagicMock()),
            ):
                auth = GitHubTokenAuthenticator.from_stream(stream=stream)

                # Get tokens
                org_token = auth.token_managers["acme-corp"][0]
                agnostic_token = auth.token_managers[None][0]

                # Set current org with exhausted token
                auth.current_organization = "acme-corp"
                auth.active_token = org_token

                # Mock org-specific token as exhausted, agnostic as available
                with (
                    patch.object(org_token, "has_calls_remaining", return_value=False),
                    patch.object(
                        agnostic_token, "has_calls_remaining", return_value=True
                    ),
                ):
                    # Should fall back to agnostic token
                    auth.get_next_auth_token()

                    assert auth.active_token == agnostic_token
                    assert auth.current_organization == "acme-corp"

    def test_get_next_auth_token_raises_when_all_exhausted(self, mock_stream):
        """Test that get_next_auth_token raises when all tokens are exhausted."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "org_auth_app_keys": {
                        "acme-corp": ["app1;;key1", "app2;;key2"],
                    }
                }
            )

            with patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("org_token", MagicMock()),
            ):
                auth = GitHubTokenAuthenticator.from_stream(stream=stream)

                org_tokens = auth.token_managers["acme-corp"]
                auth.current_organization = "acme-corp"
                auth.active_token = org_tokens[0]

                # Mock all tokens as exhausted
                with (
                    patch.object(
                        org_tokens[0], "has_calls_remaining", return_value=False
                    ),
                    patch.object(
                        org_tokens[1], "has_calls_remaining", return_value=False
                    ),
                    pytest.raises(
                        RuntimeError,
                        match="All GitHub tokens have hit their rate limit",
                    ),
                ):
                    auth.get_next_auth_token()

    def test_auth_app_keys_array_format_stores_under_none_key(self, mock_stream):
        """Test that array format for auth_app_keys stores tokens under None key."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "auth_app_keys": ["app1;;key1", "app2;;key2"],
                }
            )

            with patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("array_format_token", MagicMock()),
            ):
                auth = GitHubTokenAuthenticator.from_stream(stream=stream)
                token_managers = auth.prepare_tokens()

                # Tokens should be stored under None key (org-agnostic)
                assert None in token_managers
                assert len(token_managers[None]) == 2
                assert self._count_total_tokens(token_managers) == 2

                # Should not have any org-specific keys
                org_keys = [k for k in token_managers if k is not None]
                assert len(org_keys) == 0

    def test_auth_app_keys_object_format_stores_by_org(self, mock_stream):
        """Test that org_auth_app_keys stores tokens by organization."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "org_auth_app_keys": {
                        "org1": ["app1;;key1"],
                        "org2": ["app2;;key2", "app3;;key3"],
                    }
                }
            )

            with patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("org_token", MagicMock()),
            ):
                auth = GitHubTokenAuthenticator.from_stream(stream=stream)
                token_managers = auth.prepare_tokens()

                # Tokens should be stored under org-specific keys
                assert "org1" in token_managers
                assert "org2" in token_managers
                assert len(token_managers["org1"]) == 1
                assert len(token_managers["org2"]) == 2
                assert self._count_total_tokens(token_managers) == 3

                # Should not have any tokens under None key
                assert None not in token_managers or len(token_managers[None]) == 0

    def test_auth_app_keys_mixed_with_personal_tokens(self, mock_stream):
        """Test that org-specific app keys and personal tokens coexist correctly."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "auth_token": "personal_token",
                    "org_auth_app_keys": {
                        "org1": ["app1;;key1"],
                    },
                }
            )

            with patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("org_token", MagicMock()),
            ):
                auth = GitHubTokenAuthenticator.from_stream(stream=stream)
                token_managers = auth.prepare_tokens()

                # Should have both org-specific and org-agnostic tokens
                assert "org1" in token_managers
                assert None in token_managers
                assert len(token_managers["org1"]) == 1
                assert len(token_managers[None]) == 1
                assert self._count_total_tokens(token_managers) == 2

                # Verify personal token is under None key
                personal_tokens = [
                    tm
                    for tm in token_managers[None]
                    if isinstance(tm, PersonalTokenManager)
                ]
                assert len(personal_tokens) == 1
                assert personal_tokens[0].token == "personal_token"

    def test_set_organization_switches_to_org_specific_token(self, mock_stream):
        """Test that set_organization switches to org-specific token."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "org_auth_app_keys": {
                        "org1": ["app1;;key1"],
                        "org2": ["app2;;key2"],
                    }
                }
            )

            with patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("org_token", MagicMock()),
            ):
                auth = GitHubTokenAuthenticator.from_stream(stream=stream)

                # Initially should be on org1 (alphabetically first)
                assert auth.current_organization is None  # Not set yet
                org1_token = auth.token_managers["org1"][0]
                org2_token = auth.token_managers["org2"][0]

                # Mock has_calls_remaining to return True for all tokens
                with (
                    patch.object(org1_token, "has_calls_remaining", return_value=True),
                    patch.object(org2_token, "has_calls_remaining", return_value=True),
                ):
                    # Switch to org1
                    auth.set_organization("org1")
                    assert auth.current_organization == "org1"
                    assert auth.active_token == org1_token

                    # Switch to org2
                    auth.set_organization("org2")
                    assert auth.current_organization == "org2"
                    assert auth.active_token == org2_token

                    # Switch back to org1
                    auth.set_organization("org1")
                    assert auth.current_organization == "org1"
                    assert auth.active_token == org1_token

    def test_set_organization_falls_back_to_org_agnostic(self, mock_stream):
        """Test set_organization falls back to org-agnostic tokens when no
        org-specific tokens."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "additional_auth_tokens": ["personal_token"],
                }
            )

            auth = GitHubTokenAuthenticator.from_stream(stream=stream)

            # Should have only org-agnostic token
            assert None in auth.token_managers
            assert len(auth.token_managers[None]) == 1
            agnostic_token = auth.token_managers[None][0]

            # Switch to an org that has no specific tokens
            with patch("tap_github.authenticator.logger") as mock_logger:
                auth.set_organization("some-org")

                # Should fall back to org-agnostic token
                assert auth.current_organization == "some-org"
                assert auth.active_token == agnostic_token

                # Verify fallback info message was logged
                mock_logger.info.assert_any_call(
                    "No org-specific tokens found for 'some-org', "
                    "using org-agnostic tokens"
                )

    def test_initialization_prefers_org_specific_over_org_agnostic(self, mock_stream):
        """Test that initialization prefers org-specific token over
        org-agnostic when both exist."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "additional_auth_tokens": ["personal_token"],
                    "org_auth_app_keys": {
                        "acme": ["app1;;key1"],
                    },
                }
            )

            with patch(
                "tap_github.authenticator.generate_app_access_token",
                return_value=("org_token", MagicMock()),
            ):
                auth = GitHubTokenAuthenticator.from_stream(stream=stream)

                # Verify both token pools exist
                assert "acme" in auth.token_managers
                assert None in auth.token_managers
                acme_token = auth.token_managers["acme"][0]
                agnostic_token = auth.token_managers[None][0]

                # Mock has_calls_remaining to return True for all tokens
                with (
                    patch.object(acme_token, "has_calls_remaining", return_value=True),
                    patch.object(
                        agnostic_token, "has_calls_remaining", return_value=True
                    ),
                ):
                    # Set organization to acme
                    auth.set_organization("acme")

                    # Should prefer the org-specific token over org-agnostic
                    assert auth.current_organization == "acme"
                    assert auth.active_token == acme_token

    def test_set_organization_falls_back_to_other_org_tokens(self, mock_stream):
        """Test that set_organization falls back to tokens from other orgs
        when no tokens exist for target org."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "org_auth_app_keys": {
                        "acme-corp": ["app1;;key1"],
                        "widget-co": ["app2;;key2"],
                    }
                }
            )

            with patch(
                "tap_github.authenticator.generate_app_access_token",
                side_effect=[
                    ("acme_token", MagicMock()),
                    ("widget_token", MagicMock()),
                ],
            ):
                auth = GitHubTokenAuthenticator.from_stream(stream=stream)

                # Verify token pools exist for both orgs
                assert "acme-corp" in auth.token_managers
                assert "widget-co" in auth.token_managers
                acme_token = auth.token_managers["acme-corp"][0]
                widget_token = auth.token_managers["widget-co"][0]

                # Mock has_calls_remaining to return True for all tokens
                with (
                    patch.object(acme_token, "has_calls_remaining", return_value=True),
                    patch.object(
                        widget_token, "has_calls_remaining", return_value=True
                    ),
                ):
                    # Switch to an org with no tokens (public org like matplotlib)
                    auth.set_organization("matplotlib")

                    # Should fall back to a token from another org
                    assert auth.current_organization == "matplotlib"
                    assert auth.active_token in [acme_token, widget_token]

    def test_get_next_auth_token_falls_back_to_other_org_tokens(self, mock_stream):
        """Test that get_next_auth_token falls back to tokens from other orgs."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "org_auth_app_keys": {
                        "acme-corp": ["app1;;key1"],
                        "widget-co": ["app2;;key2"],
                    }
                }
            )

            with patch(
                "tap_github.authenticator.generate_app_access_token",
                side_effect=[
                    ("acme_token", MagicMock()),
                    ("widget_token", MagicMock()),
                ],
            ):
                auth = GitHubTokenAuthenticator.from_stream(stream=stream)

                # Set current org to public org with no tokens configured
                auth.current_organization = "matplotlib"
                auth.active_token = None

                # Get tokens from other orgs
                acme_token = auth.token_managers["acme-corp"][0]
                widget_token = auth.token_managers["widget-co"][0]

                # Mock widget-co token as available
                with (
                    patch.object(acme_token, "has_calls_remaining", return_value=False),
                    patch.object(
                        widget_token, "has_calls_remaining", return_value=True
                    ),
                ):
                    # Should fall back to token from another org
                    auth.get_next_auth_token()

                    assert auth.active_token == widget_token
                    assert auth.current_organization == "matplotlib"

    def test_both_org_auth_app_keys_and_auth_app_keys_fallback(self, mock_stream):
        """Test org-specific tokens preferred, with fallback to org-agnostic."""
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={},
            ),
            patch.object(TokenManager, "is_valid_token", return_value=True),
        ):
            stream = mock_stream
            stream.config.update(
                {
                    "auth_app_keys": ["fallback_app;;key_fallback"],
                    "org_auth_app_keys": {
                        "acme-corp": ["acme_app;;key_acme"],
                    },
                }
            )

            def mock_generate_token(app_id, private_key, installation_id):
                return (f"token_for_{app_id}", MagicMock())

            with patch(
                "tap_github.authenticator.generate_app_access_token",
                side_effect=mock_generate_token,
            ):
                auth = GitHubTokenAuthenticator.from_stream(stream=stream)

                # Verify both token pools exist
                assert "acme-corp" in auth.token_managers
                assert None in auth.token_managers
                assert len(auth.token_managers["acme-corp"]) == 1
                assert len(auth.token_managers[None]) == 1

                acme_token = auth.token_managers["acme-corp"][0]
                fallback_token = auth.token_managers[None][0]

                # Test 1: Org-specific token should be preferred
                with (
                    patch.object(acme_token, "has_calls_remaining", return_value=True),
                    patch.object(
                        fallback_token, "has_calls_remaining", return_value=True
                    ),
                ):
                    auth.set_organization("acme-corp")
                    assert auth.active_token == acme_token
                    assert auth.current_organization == "acme-corp"

                # Test 2: Should fall back to org-agnostic when org-specific exhausted
                with (
                    patch.object(acme_token, "has_calls_remaining", return_value=False),
                    patch.object(
                        fallback_token, "has_calls_remaining", return_value=True
                    ),
                ):
                    auth.get_next_auth_token()
                    assert auth.active_token == fallback_token
                    assert auth.current_organization == "acme-corp"

                # Test 3: Org without specific tokens should use fallback
                with (
                    patch.object(acme_token, "has_calls_remaining", return_value=True),
                    patch.object(
                        fallback_token, "has_calls_remaining", return_value=True
                    ),
                ):
                    auth.set_organization("other-org")
                    assert auth.active_token == fallback_token
                    assert auth.current_organization == "other-org"
