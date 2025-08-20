"""Tests for search count streams - focused on business logic."""

from __future__ import annotations

import pytest
from unittest.mock import Mock, patch

from tap_github.search_count_streams import (
    IssueSearchCountStream, 
    PRSearchCountStream, 
    BugSearchCountStream
)


class TestQueryGeneration:
    """Test query building logic - core business logic."""

    def setup_method(self):
        """Setup mock tap for testing."""
        self.mock_tap = Mock()
        self.mock_tap.config = {
            "search_scope": {
                "issue_streams": {
                    "instances": [
                        {
                            "api_url_base": "https://api.github.com",
                            "instance": "github.com",
                            "org": ["WordPress"],
                            "repo_breakdown": False
                        }
                    ]
                }
            },
            "backfill_start_month": "2025-01",
            "backfill_end_month": "2025-01",
        }

    def test_issue_query_generation(self):
        """Test issue search query format."""
        stream = IssueSearchCountStream(tap=self.mock_tap)
        query = stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "issue")
        
        assert query == "org:WordPress type:issue is:open created:2025-01-01..2025-01-31"

    def test_pr_query_generation(self):
        """Test PR search query format."""
        stream = PRSearchCountStream(tap=self.mock_tap)
        query = stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "pr")
        
        assert query == "org:WordPress type:pr is:merged created:2025-01-01..2025-01-31"

    def test_bug_query_generation(self):
        """Test bug search query format."""
        stream = BugSearchCountStream(tap=self.mock_tap)
        query = stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "bug")
        
        assert query == 'org:WordPress is:issue is:open label:bug,"[type] bug","type: bug" created:2025-01-01..2025-01-31'

    def test_repo_query_generation(self):
        """Test repo-level query building."""
        stream = IssueSearchCountStream(tap=self.mock_tap)
        query = stream._build_repo_search_query("WordPress/gutenberg", "2025-01-01", "2025-01-31", "issue")
        
        assert query == "repo:WordPress/gutenberg type:issue is:open created:2025-01-01..2025-01-31"

    def test_repo_bug_query_generation(self):
        """Test repo-level bug query building."""
        stream = BugSearchCountStream(tap=self.mock_tap)
        query = stream._build_repo_search_query("WordPress/gutenberg", "2025-01-01", "2025-01-31", "bug")
        
        assert query == 'repo:WordPress/gutenberg is:issue is:open label:bug,"[type] bug","type: bug" created:2025-01-01..2025-01-31'

    def test_query_types_are_different(self):
        """Test that issue, PR, and bug queries are different."""
        issue_stream = IssueSearchCountStream(tap=self.mock_tap)
        pr_stream = PRSearchCountStream(tap=self.mock_tap)
        bug_stream = BugSearchCountStream(tap=self.mock_tap)
        
        issue_query = issue_stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "issue")
        pr_query = pr_stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "pr")
        bug_query = bug_stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "bug")
        
        # All queries should be different
        assert issue_query != pr_query
        assert issue_query != bug_query
        assert pr_query != bug_query
        
        # Check specific differences
        assert "type:issue is:open" in issue_query
        assert "type:pr is:merged" in pr_query
        assert "is:issue is:open label:" in bug_query

    def test_date_range_formatting(self):
        """Test that date ranges are formatted correctly."""
        stream = IssueSearchCountStream(tap=self.mock_tap)
        
        query = stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "issue")
        assert "created:2025-01-01..2025-01-31" in query
        
        query = stream._build_search_query("WordPress", "2024-12-01", "2024-12-31", "issue")
        assert "created:2024-12-01..2024-12-31" in query


class TestMonthGeneration:
    """Test month/date logic - unit tests without stream instantiation."""

    def test_month_range_logic(self):
        """Test month range logic directly."""
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        
        # Test logic for generating month ranges
        start_date = datetime.strptime("2025-01", "%Y-%m")
        end_date = datetime.strptime("2025-03", "%Y-%m")
        
        months = []
        current_date = start_date
        while current_date <= end_date:
            months.append(current_date.strftime("%Y-%m"))
            current_date += relativedelta(months=1)
        
        assert months == ["2025-01", "2025-02", "2025-03"]

    def test_month_to_date_conversion_logic(self):
        """Test month to date conversion logic directly."""
        from datetime import datetime
        from calendar import monthrange
        
        # Test January 2025
        year, month = 2025, 1
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month:02d}-{monthrange(year, month)[1]:02d}"
        
        assert start_date == "2025-01-01"
        assert end_date == "2025-01-31"
        
        # Test February 2024 (leap year)
        year, month = 2024, 2
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month:02d}-{monthrange(year, month)[1]:02d}"
        
        assert start_date == "2024-02-01"
        assert end_date == "2024-02-29"


class TestConfigParsing:
    """Test configuration parsing logic."""

    @patch('tap_github.search_count_streams.SearchCountStreamBase.get_context_state')
    def test_search_scope_org_parsing(self, mock_get_context_state):
        """Test parsing org-level configuration."""
        mock_get_context_state.return_value = {}
        
        mock_tap = Mock()
        mock_tap.config = {
            "search_scope": {
                "issue_streams": {
                    "instances": [
                        {
                            "api_url_base": "https://api.github.com",
                            "instance": "github.com", 
                            "org": ["WordPress", "Automattic"],
                            "repo_breakdown": False
                        }
                    ]
                }
            },
            "backfill_start_month": "2025-01",
            "backfill_end_month": "2025-01",
        }
        mock_tap.state = {"bookmarks": {}}
        
        stream = IssueSearchCountStream(tap=mock_tap)
        partitions = stream.partitions
        
        # Should create partitions for each org
        org_names = [p["org"] for p in partitions]
        assert "WordPress" in org_names
        assert "Automattic" in org_names

    @patch('tap_github.search_count_streams.SearchCountStreamBase.get_context_state')
    def test_multiple_instances_parsing(self, mock_get_context_state):
        """Test parsing multiple GitHub instances."""
        mock_get_context_state.return_value = {}
        
        mock_tap = Mock()
        mock_tap.config = {
            "search_scope": {
                "issue_streams": {
                    "instances": [
                        {
                            "api_url_base": "https://api.github.com",
                            "instance": "github.com",
                            "org": ["WordPress"],
                            "repo_breakdown": False
                        },
                        {
                            "api_url_base": "https://github.example.com/api",
                            "instance": "github.example.com",
                            "org": ["ExampleOrg"],
                            "repo_breakdown": False
                        }
                    ]
                }
            },
            "backfill_start_month": "2025-01",
            "backfill_end_month": "2025-01",
        }
        mock_tap.state = {"bookmarks": {}}
        
        stream = IssueSearchCountStream(tap=mock_tap)
        partitions = stream.partitions
        
        # Should create partitions for each instance
        sources = [p["source"] for p in partitions]
        assert "github.com" in sources
        assert "github.example.com" in sources

    @patch('tap_github.search_count_streams.SearchCountStreamBase.get_context_state')
    def test_empty_config(self, mock_get_context_state):
        """Test behavior with minimal config."""
        mock_get_context_state.return_value = {}
        
        mock_tap = Mock()
        mock_tap.config = {}
        mock_tap.state = {"bookmarks": {}}
        
        stream = IssueSearchCountStream(tap=mock_tap)
        partitions = stream.partitions
        
        # Should return empty partitions with no config
        assert partitions == []

    @patch('tap_github.search_count_streams.SearchCountStreamBase.get_context_state')
    def test_missing_search_scope(self, mock_get_context_state):
        """Test behavior with missing search_scope."""
        mock_get_context_state.return_value = {}
        
        mock_tap = Mock()
        mock_tap.config = {
            "backfill_start_month": "2025-01",
        }
        mock_tap.state = {"bookmarks": {}}
        
        stream = IssueSearchCountStream(tap=mock_tap)
        partitions = stream.partitions
        
        # Should return empty partitions with no search_scope
        assert partitions == []


class TestStreamClassProperties:
    """Test stream class properties without instantiation."""

    def test_stream_names(self):
        """Test stream names are correct."""
        assert IssueSearchCountStream.name == "issue_search_counts"
        assert PRSearchCountStream.name == "pr_search_counts" 
        assert BugSearchCountStream.name == "bug_search_counts"

    def test_stream_types(self):
        """Test stream types are correct."""
        assert IssueSearchCountStream.stream_type == "issue"
        assert PRSearchCountStream.stream_type == "pr"
        assert BugSearchCountStream.stream_type == "bug"

    def test_count_fields(self):
        """Test count fields are correct."""
        assert IssueSearchCountStream.count_field == "issue_count"
        assert PRSearchCountStream.count_field == "pr_count"
        assert BugSearchCountStream.count_field == "bug_count"


class TestConfigurableStreams:
    """Test configurable search count streams."""

    def test_configurable_stream_creation(self):
        """Test creating a configurable stream from config."""
        from tap_github.search_count_streams import ConfigurableSearchCountStream
        
        mock_tap = Mock()
        mock_tap.config = {}
        
        stream_config = {
            "name": "security_issues",
            "query_template": "org:{org} type:issue label:security created:{start}..{end}",
            "count_field": "security_issue_count",
            "description": "Security issues",
            "stream_type": "security"
        }
        
        stream = ConfigurableSearchCountStream(stream_config, mock_tap)
        
        assert stream.name == "security_issues_search_counts"
        assert stream.count_field == "security_issue_count"
        assert stream.stream_type == "security"
        
        # Test query generation
        query = stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "security")
        assert query == "org:WordPress type:issue label:security created:2025-01-01..2025-01-31"

    def test_stream_validation(self):
        """Test stream configuration validation."""
        from tap_github.search_count_streams import validate_stream_config
        
        # Valid config
        valid_config = {
            "name": "test_stream",
            "query_template": "org:{org} type:issue created:{start}..{end}",
            "count_field": "test_count"
        }
        errors = validate_stream_config(valid_config)
        assert errors == []
        
        # Missing required fields
        invalid_config = {"name": "test"}
        errors = validate_stream_config(invalid_config)
        assert len(errors) >= 2  # Missing query_template and count_field
        
        # Missing placeholders
        missing_placeholder_config = {
            "name": "test",
            "query_template": "org:{org} type:issue",  # Missing {start} and {end}
            "count_field": "count"
        }
        errors = validate_stream_config(missing_placeholder_config)
        assert any("placeholder" in error for error in errors)

    def test_factory_function(self):
        """Test the create_configurable_streams factory."""
        from tap_github.search_count_streams import create_configurable_streams
        
        mock_tap = Mock()
        mock_tap.config = {
            "custom_search_streams": [
                {
                    "name": "security",
                    "query_template": "org:{org} type:issue label:security created:{start}..{end}",
                    "count_field": "security_count"
                },
                {
                    "name": "invalid_stream"  # Missing required fields
                }
            ]
        }
        mock_tap.logger = Mock()
        
        streams = create_configurable_streams(mock_tap)
        
        # Should create 1 valid stream and log warning for invalid one
        assert len(streams) == 1
        assert streams[0].name == "security_search_counts"
        mock_tap.logger.warning.assert_called()