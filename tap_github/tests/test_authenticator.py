from datetime import datetime, timedelta
import pytest
import requests
from unittest.mock import patch, MagicMock
from tap_github.authenticator import TokenManager


class TestTokenManager():

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
            "X-RateLimit-Used": "1"
        }

        token_manager = TokenManager("mytoken")
        token_manager.update_rate_limit(mock_response_headers)

        assert token_manager.rate_limit == 5000
        assert token_manager.rate_limit_remaining == 4999
        assert token_manager.rate_limit_reset == 1372700873
        assert token_manager.rate_limit_used == 1

    def test_is_valid_token_successful(self):
        with patch('requests.get') as mock_get:
            mock_response = mock_get.return_value
            mock_response.raise_for_status.return_value = None

            token_manager = TokenManager("validtoken")

            assert token_manager.is_valid_token()
            mock_get.assert_called_once_with(
                url="https://api.github.com/rate_limit",
                headers={"Authorization": "token validtoken"}
            )

    def test_is_valid_token_failure(self):
        with patch('requests.get') as mock_get:
            # Setup for a failed request
            mock_response = mock_get.return_value
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
            mock_response.status_code = 401
            mock_response.content = b'Unauthorized Access'
            mock_response.reason = 'Unauthorized'

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
            "X-RateLimit-Used": "1"
        }

        token_manager = TokenManager("mytoken")
        token_manager.update_rate_limit(mock_response_headers)

        assert token_manager.has_calls_remaining()

    def test_has_calls_remaining_succeeds_if_reset_time_reached(self):
        mock_response_headers = {
            "X-RateLimit-Limit": "5000",
            "X-RateLimit-Remaining": "1",
            "X-RateLimit-Reset": "1372700873",
            "X-RateLimit-Used": "4999"
        }

        token_manager = TokenManager("mytoken", rate_limit_buffer=1000)
        token_manager.update_rate_limit(mock_response_headers)

        assert token_manager.has_calls_remaining()

    def test_has_calls_remaining_fails_if_few_calls_remaining_and_reset_time_not_reached(self):
        mock_response_headers = {
            "X-RateLimit-Limit": "5000",
            "X-RateLimit-Remaining": "1",
            "X-RateLimit-Reset": str(int((datetime.now() + timedelta(days=100)).timestamp())),
            "X-RateLimit-Used": "4999"
        }

        token_manager = TokenManager("mytoken", rate_limit_buffer=1000)
        token_manager.update_rate_limit(mock_response_headers)

        assert not token_manager.has_calls_remaining()




