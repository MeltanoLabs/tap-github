"""Classes to assist in authenticating to the GitHub API."""

import logging
from datetime import datetime
from os import environ
from random import choice, shuffle
from typing import Any, Dict, Optional

from singer_sdk.streams import RESTStream


class TokenRateLimit:
    """A class to store token rate limiting information."""

    DEFAULT_RATE_LIMIT = 5000
    DEFAULT_RATE_LIMIT_BUFFER = 1000

    def __init__(self, token: str, rate_limit_buffer: Optional[int]):
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

    def update_rate_limit(self, response_headers: Any):
        self.rate_limit = int(response_headers["X-RateLimit-Limit"])
        self.rate_limit_remaining = int(response_headers["X-RateLimit-Remaining"])
        self.rate_limit_reset = int(response_headers["X-RateLimit-Reset"])
        self.rate_limit_used = int(response_headers["X-RateLimit-Used"])

    def is_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid (fresh).
        """
        if self.rate_limit_reset is None:
            return True
        if (
            self.rate_limit_used > (self.rate_limit - self.rate_limit_buffer)
            and self.rate_limit_reset > datetime.now().timestamp()
        ):
            return False
        return True


class GitHubTokenAuthenticator:
    """Base class for offloading API auth."""

    def prepare_tokens(self) -> Dict[str, TokenRateLimit]:
        # Save GitHub tokens
        available_tokens = []
        if "auth_token" in self._config:
            available_tokens = [self._config["auth_token"]]
        elif "auth_tokens" in self._config:
            available_tokens = self._config["auth_tokens"]
        else:
            # Accept multiple tokens using environment variables GITHUB_TOKEN*
            env_tokens = [
                value
                for key, value in environ.items()
                if key.startswith("GITHUB_TOKEN")
            ]
            if len(env_tokens) > 0:
                self.logger.info(
                    "Found 'GITHUB_TOKEN' environment variables for authentication."
                )
                available_tokens = env_tokens

        # Get rate_limit_buffer
        rate_limit_buffer = self._config.get("rate_limit_buffer", None)

        return {
            token: TokenRateLimit(token, rate_limit_buffer)
            for token in available_tokens
        }

    def __init__(self, stream: RESTStream) -> None:
        """Init authenticator.

        Args:
            stream: A stream for a RESTful endpoint.
        """
        self.logger: logging.Logger = stream.logger
        self.tap_name: str = stream.tap_name
        self._config: Dict[str, Any] = dict(stream.config)
        self.tokens_map = self.prepare_tokens()
        self.active_token: Optional[TokenRateLimit] = (
            choice(list(self.tokens_map.values())) if len(self.tokens_map) else None
        )

    def get_next_auth_token(self):
        tokens_list = list(self.tokens_map.items())
        shuffle(tokens_list)
        for _, token_rate_limit in tokens_list:
            if token_rate_limit.is_valid():
                self.active_token = token_rate_limit
                return

        raise RuntimeError(
            "All GitHub tokens have hit their rate limit. Stopping here."
        )

    def update_rate_limit(self, response_headers):
        self.active_token.update_rate_limit(response_headers)
        if not self.active_token.is_valid():
            self.get_next_auth_token()
