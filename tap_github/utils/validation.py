"""Validation utilities for GitHub tap."""

from __future__ import annotations

import re
from typing import Any


class GitHubValidationMixin:
    """Mixin providing common validation utilities for GitHub tap."""

    @staticmethod
    def validate_org_names(org_names: list[str]) -> None:
        """Validate GitHub organization names.
        
        Args:
            org_names: List of organization names to validate
            
        Raises:
            ValueError: If any organization name is invalid
        """
        for org in org_names:
            if not org or not isinstance(org, str):
                msg = f"Invalid organization name: {org!r}. Must be non-empty string."
                raise ValueError(msg)

            if not re.match(
                r"^[a-zA-Z0-9]([a-zA-Z0-9-])*[a-zA-Z0-9]$|^[a-zA-Z0-9]$", org
            ):
                msg = (
                    f"Invalid organization name '{org}'. Must contain only alphanumeric "
                    f"characters and hyphens, cannot start or end with hyphen."
                )
                raise ValueError(msg)

    @staticmethod
    def validate_field(value: Any, field_name: str, validator_func: callable) -> None:
        """Unified field validation with consistent error messages.
        
        Args:
            value: Value to validate
            field_name: Name of the field for error messages
            validator_func: Function that validates the value and raises on error
            
        Raises:
            ValueError: If validation fails
        """
        try:
            validator_func(value)
        except Exception as e:
            msg = f"Invalid {field_name}: {e}"
            raise ValueError(msg) from e

    @staticmethod
    def clean_identifier(s: str) -> str:
        """Convert string to URL-safe slug format.
        
        Args:
            s: String to clean
            
        Returns:
            Cleaned identifier string
        """
        return s.replace("/", "_").replace("-", "_").lower()

    @staticmethod
    def clean_month_id(month_id: str) -> str:
        """Clean month ID by removing hyphens.
        
        Args:
            month_id: Month ID string (e.g., '2024-01')
            
        Returns:
            Cleaned month ID string (e.g., '202401')
        """
        return month_id.replace('-', '')