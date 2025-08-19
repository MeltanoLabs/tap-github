"""GitHub instance configuration."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class GitHubInstance:
    """Represents a GitHub instance configuration."""
    name: str
    api_url_base: str
    auth_token: str