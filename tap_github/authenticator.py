"""Classes to assist in authenticating to the GitHub API."""

import logging
import requests
from datetime import datetime
from os import environ
from random import choice, shuffle
from typing import Any, Dict, List, Optional

from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.streams import RESTStream


class TokenRateLimit:
    """A class to store token rate limiting information."""

    DEFAULT_RATE_LIMIT = 5000
    # The DEFAULT_RATE_LIMIT_BUFFER buffer serves two purposes:
    # - keep some leeway and rotate tokens before erroring out on rate limit.
    # - not consume all available calls when we rare using an org or user token.
    DEFAULT_RATE_LIMIT_BUFFER = 1000

    def __init__(self, token: str, rate_limit_buffer: Optional[int] = None):
        """Init TokenRateLimit info."""
        self.token = token
        self.rate_limit = self.DEFAULT_RATE_LIMIT
        self.rate_limit_remaining = self.DEFAULT_RATE_LIMIT
        self.rate_limit_reset: Optional[int] = None
        self.rate_limit_used = 0
        self.rate_limit_buffer = (
            rate_limit_buffer
            if rate_limit_buffer is not None
            else self.DEFAULT_RATE_LIMIT_BUFFER
        )

    def update_rate_limit(self, response_headers: Any) -> None:
        self.rate_limit = int(response_headers["X-RateLimit-Limit"])
        self.rate_limit_remaining = int(response_headers["X-RateLimit-Remaining"])
        self.rate_limit_reset = int(response_headers["X-RateLimit-Reset"])
        self.rate_limit_used = int(response_headers["X-RateLimit-Used"])

    def is_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid and has enough api calls remaining.
        """
        if self.rate_limit_reset is None:
            return True
        if (
            self.rate_limit_used > (self.rate_limit - self.rate_limit_buffer)
            and self.rate_limit_reset > datetime.now().timestamp()
        ):
            return False
        return True


class GitHubTokenAuthenticator(APIAuthenticatorBase):
    """Base class for offloading API auth."""

    def prepare_tokens(self) -> Dict[str, TokenRateLimit]:
        # Save GitHub tokens
        available_tokens: List[str] = []
        if "auth_token" in self._config:
            available_tokens = available_tokens + [self._config["auth_token"]]
        if "additional_auth_tokens" in self._config:
            available_tokens = available_tokens + self._config["additional_auth_tokens"]
        else:
            # Accept multiple tokens using environment variables GITHUB_TOKEN*
            env_tokens = [
                value
                for key, value in environ.items()
                if key.startswith("GITHUB_TOKEN")
            ]
            if len(env_tokens) > 0:
                self.logger.info(
                    "Found {len(env_tokens)} 'GITHUB_TOKEN' environment variables for authentication."
                )
                available_tokens = env_tokens
        self.logger.info(f"Tap will run with {len(available_tokens)} auth tokens")

        # Get rate_limit_buffer
        rate_limit_buffer = self._config.get("rate_limit_buffer", None)

        # Dedup tokens and create a dict of TokenRateLimit
        return {
            token: TokenRateLimit(token, rate_limit_buffer)
            for token in list(set(available_tokens))
        }

    def __init__(self, stream: RESTStream) -> None:
        """Init authenticator.

        Args:
            stream: A stream for a RESTful endpoint.
        """
        super().__init__(stream=stream)
        self.logger: logging.Logger = stream.logger
        self.tap_name: str = stream.tap_name
        self._config: Dict[str, Any] = dict(stream.config)
        self.tokens_map = self.prepare_tokens()
        self.active_token: Optional[TokenRateLimit] = (
            choice(list(self.tokens_map.values())) if len(self.tokens_map) else None
        )

    def get_next_auth_token(self) -> None:
        tokens_list = list(self.tokens_map.items())
        shuffle(tokens_list)
        for _, token_rate_limit in tokens_list:
            if token_rate_limit.is_valid():
                self.active_token = token_rate_limit
                self.logger.info(f"Switching to fresh auth token")
                return

        raise RuntimeError(
            "All GitHub tokens have hit their rate limit. Stopping here."
        )

    def update_rate_limit(
        self, response_headers: requests.models.CaseInsensitiveDict
    ) -> None:
        # If no token or only one token is available, return early.
        if len(self.tokens_map) <= 1:
            return

        self.active_token.update_rate_limit(response_headers)
        if not self.active_token.is_valid():
            self.get_next_auth_token()

    @property
    def auth_headers(self) -> dict[str, str]:
        """Return a dictionary of auth headers to be applied.

        These will be merged with any `http_headers` specified in the stream.

        Returns:
            HTTP headers for authentication.
        """
        result = super().auth_headers
        if self.active_token:
            result["Authorization"] = f"token {self.active_token.token}"
        else:
            self.logger.info(
                "No auth token detected. "
                "For higher rate limits, please specify `auth_token` in config."
            )
        return result
