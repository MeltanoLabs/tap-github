import re
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call, patch

import pytest
import requests
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

    def test_is_valid_token_failure(self):
        with patch("requests.get") as mock_get:
            # Setup for a failed request
            mock_response = mock_get.return_value
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
            mock_response.status_code = 401
            mock_response.content = b"Unauthorized Access"
            mock_response.reason = "Unauthorized"

            token_manager = TokenManager("invalidtoken")
            token_manager.logger = MagicMock()

            assert not token_manager.is_valid_token()
            token_manager.logger.warning.assert_called_once()
            assert "401" in token_manager.logger.warning.call_args[0][0]

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

    def test_has_calls_remaining_regenerates_a_token_if_close_to_expiry(self):
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
            token_manager.logger = MagicMock()
            token_manager.token_expires_at = expired_time
            token_manager.update_rate_limit(mock_response_headers)

            assert token_manager.has_calls_remaining()
            # calling has_calls_remaining() will trigger the token generation function to be called again,  # noqa: E501
            # so token_expires_at should have been reset back to the mocked unexpired_time  # noqa: E501
            assert token_manager.token_expires_at == unexpired_time
            token_manager.logger.info.assert_called_once()
            assert (
                "GitHub app token refresh succeeded."
                in token_manager.logger.info.call_args[0][0]
            )

    def test_has_calls_remaining_logs_warning_if_token_regeneration_fails(self):
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
            token_manager.logger = MagicMock()
            token_manager.token_expires_at = expired_time
            token_manager.update_rate_limit(mock_response_headers)

            mock_is_valid.return_value = False
            assert not token_manager.has_calls_remaining()
            assert isinstance(token_manager.logger.warning, MagicMock)
            token_manager.logger.warning.assert_has_calls(
                [call("GitHub app token refresh failed.")],
                any_order=True,
            )

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
    stream.logger = MagicMock()
    stream.tap_name = "tap_github"
    stream.config = {"rate_limit_buffer": 5}
    return stream


class TestGitHubTokenAuthenticator:
    def test_prepare_tokens_returns_empty_if_none_found(self, mock_stream):
        with (
            patch.object(
                GitHubTokenAuthenticator,
                "get_env",
                return_value={"GITHUB_TLJKJFDS": "gt1"},
            ),
            patch.object(PersonalTokenManager, "is_valid_token", return_value=True),
        ):
            auth = GitHubTokenAuthenticator(stream=mock_stream)
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
            auth = GitHubTokenAuthenticator(stream=stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 1
            assert token_managers[0].token == "gt5"

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
            auth = GitHubTokenAuthenticator(stream=stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 3
            assert sorted({tm.token for tm in token_managers}) == ["gt7", "gt8", "gt9"]

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
            auth = GitHubTokenAuthenticator(stream=mock_stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 2
            assert sorted({tm.token for tm in token_managers}) == ["gt1", "gt2"]

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
            auth = GitHubTokenAuthenticator(stream=stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 7

            app_token_managers = {
                tm for tm in token_managers if isinstance(tm, AppTokenManager)
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
            auth = GitHubTokenAuthenticator(stream=mock_stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 1
            assert token_managers[0].token == "installationtoken12345"

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
            auth = GitHubTokenAuthenticator(stream=stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 5
            assert sorted({tm.token for tm in token_managers}) == [
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
            auth = GitHubTokenAuthenticator(stream=stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 4
            assert sorted({tm.token for tm in token_managers}) == [
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
            auth = GitHubTokenAuthenticator(stream=stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 3
            assert sorted({tm.token for tm in token_managers}) == ["gt1", "gt8", "gt9"]

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
            auth = GitHubTokenAuthenticator(stream=stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 2
            assert sorted({tm.token for tm in token_managers}) == ["gt1", "gt2"]

    def test_handle_error_if_app_key_invalid(self, mock_stream):
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

            auth = GitHubTokenAuthenticator(stream=mock_stream)
            auth.prepare_tokens()

            mock_stream.logger.warning.assert_called_with(
                "An error was thrown while preparing an app token: Invalid key format"
            )

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
            auth = GitHubTokenAuthenticator(stream=mock_stream)
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
            auth = GitHubTokenAuthenticator(stream=stream)
            token_managers = auth.prepare_tokens()

            assert len(token_managers) == 0
