"""Date utilities for GitHub tap."""

from __future__ import annotations

import calendar
import re
from datetime import datetime
from typing import Any


class GitHubDateUtils:
    """Utilities for date validation and processing in GitHub tap."""

    @staticmethod
    def validate_format(date_str: str, field_name: str) -> None:
        """Validate date format is YYYY-MM-DD.
        
        Args:
            date_str: Date string to validate
            field_name: Name of the field for error messages
            
        Raises:
            ValueError: If date format is invalid
        """
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            msg = f"Invalid {field_name} format '{date_str}'. Expected YYYY-MM-DD format."
            raise ValueError(msg)

        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError as e:
            msg = f"Invalid {field_name} '{date_str}': {e}"
            raise ValueError(msg) from e

    @staticmethod
    def get_auto_end_date(start_date: str) -> str:
        """Get auto-detected end date based on start date and current date.
        
        Rules:
        1. End date is last day of the previous complete month
        2. Configurable lookback limit (default: 1 year)
        3. Can enforce or warn based on configuration
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            
        Returns:
            End date string in YYYY-MM-DD format
        """
        today = datetime.now()
        
        # Last complete month is the previous month
        if today.month == 1:
            last_complete_month = today.replace(year=today.year - 1, month=12, day=1)
        else:
            last_complete_month = today.replace(month=today.month - 1, day=1)
        
        # Get last day of that month
        last_day = calendar.monthrange(last_complete_month.year, last_complete_month.month)[1]
        auto_end_date = last_complete_month.replace(day=last_day)
        
        return auto_end_date.strftime("%Y-%m-%d")

    @staticmethod
    def validate_and_adjust_date_range(
        start_date: str, 
        end_date: str | None = None,
        enforce_lookback_limit: bool = False,
        max_lookback_years: int = 1,
        logger: Any = None
    ) -> tuple[str, str]:
        """Validate and potentially adjust date range based on configuration.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (optional)
            enforce_lookback_limit: Whether to enforce lookback limits
            max_lookback_years: Maximum lookback years
            logger: Logger instance for warnings
            
        Returns:
            Tuple of (validated_start_date, validated_end_date)
            
        Raises:
            ValueError: If dates are invalid or exceed limits when enforcement is enabled
        """
        GitHubDateUtils.validate_format(start_date, "start date")
        
        if not end_date:
            end_date = GitHubDateUtils.get_auto_end_date(start_date)
            if logger:
                logger.info(f"Auto-detected end date: {end_date} (last complete month)")
        
        GitHubDateUtils.validate_format(end_date, "end date")
        
        # Parse dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start_dt > end_dt:
            raise ValueError(f"Start date '{start_date}' must be before or equal to end date '{end_date}'")
        
        # Check lookback limit
        max_lookback = end_dt.replace(year=end_dt.year - max_lookback_years)
        
        if start_dt < max_lookback:
            if enforce_lookback_limit:
                adjusted_start = max_lookback.strftime("%Y-%m-%d")
                if logger:
                    logger.warning(
                        f"Start date {start_date} exceeds {max_lookback_years}-year limit. "
                        f"Enforcing limit by adjusting to {adjusted_start}"
                    )
                return adjusted_start, end_date
            else:
                if logger:
                    logger.warning(
                        f"Start date {start_date} exceeds recommended {max_lookback_years}-year lookback. "
                        f"Consider setting enforce_lookback_limit=true to automatically adjust."
                    )
        
        return start_date, end_date

    @staticmethod
    def generate_month_ranges(start_date: str, end_date: str) -> list[tuple[str, str, str]]:
        """Generate monthly date ranges from start and end dates.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of tuples (month_start_date, month_end_date, month_id)
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        ranges = []
        current = start.replace(day=1)

        while current <= end:
            if current.year == end.year and current.month == end.month:
                month_end_date = end_date
            else:
                last_day = calendar.monthrange(current.year, current.month)[1]
                month_end_date = f"{current.year}-{current.month:02d}-{last_day:02d}"

            month_start_date = f"{current.year}-{current.month:02d}-01"
            month_id = f"{current.year}-{current.month:02d}"

            ranges.append((month_start_date, month_end_date, month_id))

            # Move to next month
            current = (
                current.replace(year=current.year + 1, month=1)
                if current.month == 12
                else current.replace(month=current.month + 1)
            )

        return ranges