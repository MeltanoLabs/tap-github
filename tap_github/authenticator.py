"""Classes to assist in authenticating to the GitHub API."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from os import environ
from random import choice, shuffle
from typing import TYPE_CHECKING, Any

import jwt
import requests
import requests.exceptions
import requests.models
from singer_sdk.authenticators import APIAuthenticatorBase

if TYPE_CHECKING:
    from singer_sdk.streams import RESTStream

logger = logging.getLogger(__name__)


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
            logger.warning(msg)
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
        rate_limit_buffer = rate_limit_buffer or self.DEFAULT_RATE_LIMIT_BUFFER
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
            logger.warning("An app token was generated but could not be validated.")
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
                    logger.warning("GitHub app token refresh failed.")
                    return False
                else:
                    logger.info("GitHub app token refresh succeeded.")

        return super().has_calls_remaining()


class GitHubTokenAuthenticator(APIAuthenticatorBase):
    """Base class for offloading API auth."""

    @staticmethod
    def get_env():  # noqa: ANN205
        return dict(environ)

    def _collect_personal_tokens(self, env_dict: dict) -> set[str]:
        """Collect personal tokens from config and environment variables.

        Args:
            env_dict: Dictionary of environment variables.

        Returns:
            Set of personal access tokens.
        """
        personal_tokens: set[str] = set()
        if self.auth_token:
            personal_tokens.add(self.auth_token)
        if self.additional_auth_tokens:
            personal_tokens = personal_tokens.union(self.additional_auth_tokens)
        else:
            # Accept multiple tokens using environment variables GITHUB_TOKEN*
            env_tokens = {
                value
                for key, value in env_dict.items()
                if key.startswith("GITHUB_TOKEN")
            }
            if len(env_tokens) > 0:
                logger.info(
                    "Found %d 'GITHUB_TOKEN' environment variables for authentication.",
                    len(env_tokens),
                )
                personal_tokens = personal_tokens.union(env_tokens)
        return personal_tokens

    def _create_personal_token_managers(
        self, personal_tokens: set[str]
    ) -> list[TokenManager]:
        """Create validated token managers from personal tokens.

        Args:
            personal_tokens: Set of personal access tokens.

        Returns:
            List of validated PersonalTokenManager instances.
        """
        personal_token_managers: list[TokenManager] = []
        for token in personal_tokens:
            token_manager = PersonalTokenManager(
                token,
                rate_limit_buffer=self.rate_limit_buffer,
            )
            if token_manager.is_valid_token():
                personal_token_managers.append(token_manager)
            else:
                logger.warning("A token was dismissed.")
        return personal_token_managers

    def _collect_app_keys(self, env_dict: dict) -> dict[str | None, list[str]]:
        """Collect org-agnostic app keys from config and environment variables.

        App keys can be provided via:
        1. Config as array: ["app_id;;private_key", ...]
        2. Environment variable GITHUB_APP_PRIVATE_KEY: "app_id;;private_key"

        Args:
            env_dict: Dictionary of environment variables.

        Returns:
            Dictionary mapping None to lists of org-agnostic app keys.
        """
        app_keys: dict[str | None, list[str]] = defaultdict(list)

        if self.auth_app_keys:
            for app_key in self.auth_app_keys:
                app_keys[None].append(app_key)
            logger.info(
                "Provided %d app keys via config for authentication.",
                len(self.auth_app_keys),
            )
        elif "GITHUB_APP_PRIVATE_KEY" in env_dict:
            app_keys[None].append(env_dict["GITHUB_APP_PRIVATE_KEY"])
            logger.info("Found 1 app key via environment variable for authentication.")

        return app_keys

    def _collect_org_auth_app_keys(self) -> dict[str | None, list[str]]:
        """Collect organization-specific app keys from config.

        Returns:
            Dictionary mapping organization names to lists of app keys.
        """
        org_app_keys: dict[str | None, list[str]] = defaultdict(list)

        if self.org_auth_app_keys:
            for org, app_keys in self.org_auth_app_keys.items():
                for app_key in app_keys:
                    org_app_keys[org].append(app_key)
                logger.info(
                    "Provided %d app keys via config for authentication "
                    "for organization: %s",
                    len(app_keys),
                    org,
                )

        return org_app_keys

    def _create_app_token_managers(
        self,
        app_keys: dict[str | None, list[str]],
    ) -> dict[str | None, list[TokenManager]]:
        """Create validated app token managers from app keys.

        Args:
            app_keys: Dictionary mapping organization names to lists of app keys.

        Returns:
            Dictionary mapping organization names to lists of validated
            AppTokenManager instances.
        """
        token_managers: dict[str | None, list[TokenManager]] = defaultdict(list)

        for org in app_keys:
            for app_key in app_keys[org]:
                try:
                    app_token_manager = AppTokenManager(
                        app_key,
                        rate_limit_buffer=self.rate_limit_buffer,
                        expiry_time_buffer=self.expiry_time_buffer,
                    )
                    if app_token_manager.is_valid_token():
                        token_managers[org].append(app_token_manager)
                except ValueError as e:  # noqa: PERF203
                    logger.warning(
                        f"An error was thrown while preparing an app token: {e}"
                    )

        return token_managers

    def prepare_tokens(self) -> dict[str | None, list[TokenManager]]:
        """Prep GitHub tokens"""
        env_dict = self.get_env()

        # Collect and create personal token managers
        personal_tokens = self._collect_personal_tokens(env_dict)
        personal_token_managers = self._create_personal_token_managers(personal_tokens)

        # Collect and create org-agnostic app token managers
        app_keys = self._collect_app_keys(env_dict)
        token_managers = self._create_app_token_managers(app_keys)

        # Collect and create org-specific app token managers
        org_app_keys = self._collect_org_auth_app_keys()
        org_token_managers = self._create_app_token_managers(org_app_keys)

        # Merge org-specific token managers into main dict
        for org, managers in org_token_managers.items():
            token_managers[org].extend(managers)

        # Log summary
        logger.info(
            "Tap will run with %d personal auth tokens and %d app keys.",
            len(personal_token_managers),
            sum(len(token_managers[org]) for org in token_managers),
        )

        # Merge personal tokens into org-agnostic tokens (None key).
        if personal_token_managers:
            token_managers[None].extend(personal_token_managers)

        return token_managers

    def __init__(
        self,
        *,
        rate_limit_buffer: int | None = None,
        expiry_time_buffer: int | None = None,
        auth_token: str | None = None,
        additional_auth_tokens: list[str] | None = None,
        auth_app_keys: list[str] | None = None,
        org_auth_app_keys: dict[str, list[str]] | None = None,
    ) -> None:
        """Init authenticator.

        Args:
            stream: A stream for a RESTful endpoint.
            rate_limit_buffer: A buffer to add to the rate limit.
            expiry_time_buffer: A buffer used when determining when to refresh GitHub
                app tokens. Only relevant when authenticating as a GitHub app.
            auth_token: A personal access token.
            additional_auth_tokens: A list of personal access tokens.
            auth_app_keys: Organization-agnostic GitHub App keys used as fallback
                for all organizations.
            org_auth_app_keys: Organization-specific GitHub App keys. Dict mapping
                organization names to lists of app keys.
        """
        super().__init__()
        self.rate_limit_buffer = rate_limit_buffer
        self.expiry_time_buffer = expiry_time_buffer
        self.auth_token = auth_token
        self.additional_auth_tokens = additional_auth_tokens
        self.auth_app_keys = auth_app_keys
        self.org_auth_app_keys = org_auth_app_keys

        self.token_managers = self.prepare_tokens()
        self.current_organization: str | None = None
        self.active_token: TokenManager | None = None
        if self.token_managers:
            # Prefer org-specific tokens over org-agnostic (None key)
            org_keys = [k for k in self.token_managers if k is not None]
            initial_org = min(org_keys) if org_keys else None
            self.logger.info(
                f"Setting initial organization for authenticator: {initial_org}"
            )
            self.active_token = choice(self.token_managers[initial_org])
        else:
            self.logger.info("Setting initial organization for authenticator: None")

    @classmethod
    def from_stream(cls, stream: RESTStream) -> GitHubTokenAuthenticator:
        return cls(
            rate_limit_buffer=stream.config.get("rate_limit_buffer"),
            expiry_time_buffer=stream.config.get("expiry_time_buffer"),
            auth_token=stream.config.get("auth_token"),
            additional_auth_tokens=stream.config.get("additional_auth_tokens"),
            auth_app_keys=stream.config.get("auth_app_keys"),
            org_auth_app_keys=stream.config.get("org_auth_app_keys"),
        )

    def set_organization(self, org: str) -> None:
        """Set the current organization and switch to an appropriate token.

        Args:
            org: The organization name to switch to.
        """
        # If we're already using this org, no need to switch
        if self.current_organization == org:
            return

        logger.info(f"Switching authentication context to organization: {org}")
        self.current_organization = org

        # Get tokens for this org (check both org-specific and None keys)
        available_tokens = self.token_managers.get(org, [])
        if not available_tokens and None in self.token_managers:
            # Fall back to org-agnostic tokens (personal tokens or env var app keys)
            available_tokens = self.token_managers[None]
            logger.info(
                f"No org-specific tokens found for '{org}', using org-agnostic tokens"
            )

        # If still no tokens, try tokens from other orgs (for public data access)
        if not available_tokens:
            for other_org, tokens in self.token_managers.items():
                if other_org is not None and tokens:
                    available_tokens = tokens
                    logger.info(
                        f"No tokens for '{org}', using tokens from '{other_org}' "
                        f"for public data access"
                    )
                    break
            else:
                logger.warning(
                    f"No authentication tokens available for organization: {org}"
                )
                self.active_token = None
                return

        # Select a token with remaining calls
        for token_manager in available_tokens:
            if token_manager.has_calls_remaining():
                self.active_token = token_manager
                logger.info(f"Selected token for organization: {org}")
                return

        # If no tokens have calls remaining, just pick the first one
        # (it might refresh or we'll rotate later)
        self.active_token = available_tokens[0]
        logger.info(
            f"Selected token for organization: {org} (may need rate limit refresh)"
        )

    def get_next_auth_token(self) -> None:
        current_token = self.active_token.token if self.active_token else ""

        # Build a list of candidate tokens for the current organization
        candidates = []

        # Priority 1: Other tokens for the current organization
        if (
            self.current_organization
            and self.current_organization in self.token_managers
        ):
            org_tokens = list(self.token_managers[self.current_organization])
            shuffle(org_tokens)
            candidates.extend(org_tokens)

        # Priority 2: Org-agnostic tokens (stored under None key)
        if None in self.token_managers:
            agnostic_tokens = list(self.token_managers[None])
            shuffle(agnostic_tokens)
            candidates.extend(agnostic_tokens)

        # Priority 3: Tokens from other organizations (for public data access)
        # GitHub tokens can access public data from any org
        for org, tokens in self.token_managers.items():
            if org != self.current_organization and org is not None:
                other_org_tokens = list(tokens)
                shuffle(other_org_tokens)
                candidates.extend(other_org_tokens)

        # Try to find a token with remaining capacity
        for token_manager in candidates:
            if (
                token_manager.has_calls_remaining()
                and current_token != token_manager.token
            ):
                self.active_token = token_manager
                logger.info("Switching to fresh auth token")
                return

        raise RuntimeError(
            "All GitHub tokens have hit their rate limit. Stopping here."
        )

    def update_rate_limit(
        self,
        response_headers: requests.models.CaseInsensitiveDict,
    ) -> None:
        # If no token or only one token is available, return early.
        # Count total tokens across all organizations
        total_tokens = sum(len(tokens) for tokens in self.token_managers.values())
        if total_tokens <= 1 or self.active_token is None:
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
            logger.info(
                "No auth token detected. "
                "For higher rate limits, please specify `auth_token` in config."
            )
        return request
