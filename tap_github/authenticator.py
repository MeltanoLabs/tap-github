"""Classes to assist in authenticating to the GitHub API."""

from __future__ import annotations

import logging
import time
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from os import environ
from random import choice, shuffle
from typing import TYPE_CHECKING, Any

import jwt
import requests
from singer_sdk.authenticators import APIAuthenticatorBase

if TYPE_CHECKING:
    from singer_sdk.streams import RESTStream


class TokenManager:
    """A class to store a token's attributes and state.
    This parent class should not be used directly, use a subclass instead.
    """

    DEFAULT_RATE_LIMIT = 5000
    # The DEFAULT_RATE_LIMIT_BUFFER buffer serves two purposes:
    # - keep some leeway and rotate tokens before erroring out on rate limit.
    # - not consume all available calls when we rare using an org or user token.
    DEFAULT_RATE_LIMIT_BUFFER = 1000

    def __init__(
        self,
        token: str | None,
        rate_limit_buffer: int | None = None,
        logger: Any | None = None,  # noqa: ANN401
    ) -> None:
        """Init TokenManager info."""
        self.token = token
        self.logger = logger
        self.rate_limit = self.DEFAULT_RATE_LIMIT
        self.rate_limit_remaining = self.DEFAULT_RATE_LIMIT
        self.rate_limit_reset: datetime | None = None
        self.rate_limit_used = 0
        self.rate_limit_buffer = (
            rate_limit_buffer
            if rate_limit_buffer is not None
            else self.DEFAULT_RATE_LIMIT_BUFFER
        )

    def update_rate_limit(self, response_headers: Any) -> None:  # noqa: ANN401
        self.rate_limit = int(response_headers["X-RateLimit-Limit"])
        self.rate_limit_remaining = int(response_headers["X-RateLimit-Remaining"])
        self.rate_limit_reset = datetime.fromtimestamp(
            int(response_headers["X-RateLimit-Reset"]),
            tz=timezone.utc,
        )
        self.rate_limit_used = int(response_headers["X-RateLimit-Used"])

    def is_valid_token(self) -> bool:
        """Try making a request with the current token. If the request succeeds return True, else False."""  # noqa: E501
        if not self.token:
            return False

        try:
            response = requests.get(
                url="https://api.github.com/rate_limit",
                headers={
                    "Authorization": f"token {self.token}",
                },
            )
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError:
            msg = (
                f"A token could not be validated. "
                f"{response.status_code} Client Error: "
                f"{response.content!s} (Reason: {response.reason})"
            )
            if self.logger is not None:
                self.logger.warning(msg)
            return False

    def has_calls_remaining(self) -> bool:
        """Check if a token has capacity to make more calls.

        Returns:
            True if the token is valid and has enough api calls remaining.
        """
        if self.rate_limit_reset is None:
            return True
        return self.rate_limit_used <= (
            self.rate_limit - self.rate_limit_buffer
        ) or self.rate_limit_reset <= datetime.now(tz=timezone.utc)


class PersonalTokenManager(TokenManager):
    """A class to store token rate limiting information."""

    def __init__(
        self,
        token: str,
        rate_limit_buffer: int | None = None,
        **kwargs,  # noqa: ANN003
    ) -> None:
        """Init PersonalTokenRateLimit info."""
        super().__init__(token, rate_limit_buffer=rate_limit_buffer, **kwargs)


def generate_jwt_token(
    github_app_id: str,
    github_private_key: str,
    expiration_time: int = 600,
    algorithm: str = "RS256",
) -> str:
    actual_time = int(time.time())

    payload = {
        "iat": actual_time,
        "exp": actual_time + expiration_time,
        "iss": github_app_id,
    }
    token = jwt.encode(payload, github_private_key, algorithm=algorithm)

    if isinstance(token, bytes):
        token = token.decode("utf-8")

    return token


def generate_app_access_token(
    github_app_id: str,
    github_private_key: str,
    github_installation_id: str | None = None,
) -> tuple[str, datetime]:
    produced_at = datetime.now(tz=timezone.utc)
    jwt_token = generate_jwt_token(github_app_id, github_private_key)

    headers = {"Authorization": f"Bearer {jwt_token}"}

    if github_installation_id is None:
        list_installations_resp = requests.get(
            url="https://api.github.com/app/installations", headers=headers
        )
        list_installations_resp.raise_for_status()
        list_installations = list_installations_resp.json()

        if len(list_installations) == 0:
            raise Exception(f"No installations found for app {github_app_id}.")

        github_installation_id = choice(list_installations)["id"]

    url = f"https://api.github.com/app/installations/{github_installation_id}/access_tokens"
    resp = requests.post(url, headers=headers)

    if resp.status_code != 201:
        resp.raise_for_status()

    expires_at = produced_at + timedelta(hours=1)
    return resp.json()["token"], expires_at


class AppTokenManager(TokenManager):
    """A class to store an app token's attributes and state, and handle token refreshing"""  # noqa: E501

    DEFAULT_RATE_LIMIT = 15000
    DEFAULT_EXPIRY_BUFFER_MINS = 10

    def __init__(
        self,
        env_key: str,
        rate_limit_buffer: int | None = None,
        expiry_time_buffer: int | None = None,
        **kwargs,  # noqa: ANN003
    ) -> None:
        if rate_limit_buffer is None:
            rate_limit_buffer = self.DEFAULT_RATE_LIMIT_BUFFER
        super().__init__(None, rate_limit_buffer=rate_limit_buffer, **kwargs)

        parts = env_key.split(";;")
        self.github_app_id = parts[0]
        self.github_private_key = (parts[1:2] or [""])[0].replace("\\n", "\n")
        self.github_installation_id: str | None = parts[2] if len(parts) >= 3 else None

        if expiry_time_buffer is None:
            expiry_time_buffer = self.DEFAULT_EXPIRY_BUFFER_MINS
        self.expiry_time_buffer = expiry_time_buffer

        self.token_expires_at: datetime | None = None
        self.claim_token()

    def claim_token(self) -> None:
        """Updates the TokenManager's token and token_expires_at attributes.

        The outcome will be _either_ that self.token is updated to a newly claimed valid token and
        self.token_expires_at is updated to the anticipated expiry time (erring on the side of an early estimate)
        _or_ self.token and self.token_expires_at are both set to None.
        """  # noqa: E501
        self.token = None
        self.token_expires_at = None

        # Make sure we have the details we need
        if not self.github_app_id or not self.github_private_key:
            raise ValueError(
                "GITHUB_APP_PRIVATE_KEY could not be parsed. The expected format is "
                '":app_id:;;-----BEGIN RSA PRIVATE KEY-----\\n_YOUR_P_KEY_\\n-----END RSA PRIVATE KEY-----"'  # noqa: E501
            )

        self.token, self.token_expires_at = generate_app_access_token(
            self.github_app_id, self.github_private_key, self.github_installation_id
        )

        # Check if the token isn't valid.  If not, overwrite it with None
        if not self.is_valid_token():
            if self.logger:
                self.logger.warning(
                    "An app token was generated but could not be validated."
                )
            self.token = None
            self.token_expires_at = None

    def has_calls_remaining(self) -> bool:
        """Check if a token has capacity to make more calls.

        Returns:
            True if the token is valid and has enough api calls remaining.
        """
        if self.token_expires_at is not None:
            close_to_expiry = datetime.now(
                tz=timezone.utc
            ) > self.token_expires_at - timedelta(minutes=self.expiry_time_buffer)

            if close_to_expiry:
                self.claim_token()
                if self.token is None:
                    if self.logger:
                        self.logger.warning("GitHub app token refresh failed.")
                    return False
                else:
                    if self.logger:
                        self.logger.info("GitHub app token refresh succeeded.")

        return super().has_calls_remaining()


class GitHubTokenAuthenticator(APIAuthenticatorBase):
    """Base class for offloading API auth."""

    @staticmethod
    def get_env():  # noqa: ANN205
        return dict(environ)

    def prepare_tokens(self) -> list[TokenManager]:
        """Prep GitHub tokens"""

        env_dict = self.get_env()
        rate_limit_buffer = self._config.get("rate_limit_buffer", None)
        expiry_time_buffer = self._config.get("expiry_time_buffer", None)

        personal_tokens: set[str] = set()
        if "auth_token" in self._config:
            personal_tokens.add(self._config["auth_token"])
        if "additional_auth_tokens" in self._config:
            personal_tokens = personal_tokens.union(
                self._config["additional_auth_tokens"]
            )
        else:
            # Accept multiple tokens using environment variables GITHUB_TOKEN*
            env_tokens = {
                value
                for key, value in env_dict.items()
                if key.startswith("GITHUB_TOKEN")
            }
            if len(env_tokens) > 0:
                self.logger.info(
                    f"Found {len(env_tokens)} 'GITHUB_TOKEN' environment variables for authentication."  # noqa: E501
                )
                personal_tokens = personal_tokens.union(env_tokens)

        personal_token_managers: list[TokenManager] = []
        for token in personal_tokens:
            token_manager = PersonalTokenManager(
                token, rate_limit_buffer=rate_limit_buffer, logger=self.logger
            )
            if token_manager.is_valid_token():
                personal_token_managers.append(token_manager)
            else:
                logging.warning("A token was dismissed.")

        # Parse App level private keys and generate tokens
        # To simplify settings, we use a single env-key formatted as follows:
        # "{app_id};;{-----BEGIN RSA PRIVATE KEY-----\n_YOUR_PRIVATE_KEY_\n-----END RSA PRIVATE KEY-----}"  # noqa: E501

        app_keys: set[str] = set()
        if "auth_app_keys" in self._config:
            app_keys = app_keys.union(self._config["auth_app_keys"])
            self.logger.info(
                f"Provided {len(app_keys)} app keys via config for authentication."
            )
        elif "GITHUB_APP_PRIVATE_KEY" in env_dict:
            app_keys.add(env_dict["GITHUB_APP_PRIVATE_KEY"])
            self.logger.info(
                "Found 1 app key via environment variable for authentication."
            )

        app_token_managers: list[TokenManager] = []
        for app_key in app_keys:
            try:
                app_token_manager = AppTokenManager(
                    app_key,
                    rate_limit_buffer=rate_limit_buffer,
                    expiry_time_buffer=expiry_time_buffer,
                    logger=self.logger,
                )
                if app_token_manager.is_valid_token():
                    app_token_managers.append(app_token_manager)
            except ValueError as e:  # noqa: PERF203
                self.logger.warning(
                    f"An error was thrown while preparing an app token: {e}"
                )

        self.logger.info(
            f"Tap will run with {len(personal_token_managers)} personal auth tokens "
            f"and {len(app_token_managers)} app keys."
        )
        return personal_token_managers + app_token_managers

    def __init__(self, stream: RESTStream) -> None:
        """Init authenticator.

        Args:
            stream: A stream for a RESTful endpoint.
        """
        super().__init__(stream=stream)
        self.logger: logging.Logger = stream.logger
        self.tap_name: str = stream.tap_name
        self._config: dict[str, Any] = dict(stream.config)
        self.token_managers = self.prepare_tokens()
        self.active_token: TokenManager | None = (
            choice(self.token_managers) if self.token_managers else None
        )

    def get_next_auth_token(self) -> None:
        current_token = self.active_token.token if self.active_token else ""
        token_managers = deepcopy(self.token_managers)
        shuffle(token_managers)
        for token_manager in token_managers:
            if (
                token_manager.has_calls_remaining()
                and current_token != token_manager.token
            ):
                self.active_token = token_manager
                self.logger.info("Switching to fresh auth token")
                return

        raise RuntimeError(
            "All GitHub tokens have hit their rate limit. Stopping here."
        )

    def update_rate_limit(
        self, response_headers: requests.models.CaseInsensitiveDict
    ) -> None:
        # If no token or only one token is available, return early.
        if len(self.token_managers) <= 1 or self.active_token is None:
            return

        self.active_token.update_rate_limit(response_headers)

    def authenticate_request(
        self,
        request: requests.PreparedRequest,
    ) -> requests.PreparedRequest:
        if self.active_token:
            # Make sure that our token is still valid or update it.
            if not self.active_token.has_calls_remaining():
                self.get_next_auth_token()
            request.headers["Authorization"] = f"token {self.active_token.token}"
        else:
            self.logger.info(
                "No auth token detected. "
                "For higher rate limits, please specify `auth_token` in config."
            )
        return request
