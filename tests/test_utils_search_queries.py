"""Tests for search query generation utility."""

from __future__ import annotations

import pytest
from datetime import datetime

from tap_github.utils.search_queries import SearchQueryGenerator, SearchQuery


class TestSearchQueryGenerator:
    """Test suite for SearchQueryGenerator utility."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = SearchQueryGenerator()

    def test_build_search_query_issue_org_scope(self):
        """Test building issue search query for organization scope."""
        query = self.generator.build_search_query(
            query_type="issue",
            scope="org",
            target="test-org",
            start_date="2025-01-01",
            end_date="2025-01-31"
        )
        
        expected = "org:test-org type:issue state:open created:2025-01-01..2025-01-31"
        assert query == expected

    def test_build_search_query_bug_org_scope(self):
        """Test building bug search query for organization scope."""
        query = self.generator.build_search_query(
            query_type="bug",
            scope="org",
            target="test-org",
            start_date="2025-01-01",
            end_date="2025-01-31"
        )
        
        expected = 'org:test-org type:issue state:open label:bug,defect,"[type] bug","type: bug" created:2025-01-01..2025-01-31'
        assert query == expected

    def test_build_search_query_pr_org_scope(self):
        """Test building PR search query for organization scope."""
        query = self.generator.build_search_query(
            query_type="pr",
            scope="org",
            target="test-org",
            start_date="2025-01-01",
            end_date="2025-01-31"
        )
        
        expected = "org:test-org type:pr is:merged merged:2025-01-01..2025-01-31"
        assert query == expected

    def test_build_search_query_issue_repo_scope(self):
        """Test building issue search query for repository scope."""
        query = self.generator.build_search_query(
            query_type="issue",
            scope="repo",
            target="owner/repo",
            start_date="2025-01-01",
            end_date="2025-01-31"
        )
        
        expected = "repo:owner/repo type:issue state:open created:2025-01-01..2025-01-31"
        assert query == expected

    def test_build_search_query_bug_repo_scope(self):
        """Test building bug search query for repository scope."""
        query = self.generator.build_search_query(
            query_type="bug",
            scope="repo",
            target="owner/repo",
            start_date="2025-01-01",
            end_date="2025-01-31"
        )
        
        expected = 'repo:owner/repo type:issue state:open label:bug,defect,"[type] bug","type: bug" created:2025-01-01..2025-01-31'
        assert query == expected

    def test_build_search_query_pr_repo_scope(self):
        """Test building PR search query for repository scope."""
        query = self.generator.build_search_query(
            query_type="pr",
            scope="repo",
            target="owner/repo",
            start_date="2025-01-01",
            end_date="2025-01-31"
        )
        
        expected = "repo:owner/repo type:pr is:merged merged:2025-01-01..2025-01-31"
        assert query == expected

    def test_invalid_query_type(self):
        """Test that invalid query type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid query_type: invalid"):
            self.generator.build_search_query(
                query_type="invalid",
                scope="org",
                target="test-org",
                start_date="2025-01-01",
                end_date="2025-01-31"
            )

    def test_invalid_scope(self):
        """Test that invalid scope raises ValueError."""
        with pytest.raises(ValueError, match="Invalid scope: invalid"):
            self.generator.build_search_query(
                query_type="issue",
                scope="invalid",
                target="test-org",
                start_date="2025-01-01",
                end_date="2025-01-31"
            )

    def test_generate_monthly_queries_single_month(self):
        """Test generating monthly queries for a single month."""
        queries = self.generator.generate_monthly_queries(
            organizations=["test-org"],
            start_date="2025-01-01",
            end_date="2025-01-31",
            query_types=["issue"]
        )
        
        assert len(queries) == 1
        assert queries[0].name == "test-org_issue_2025-01"
        assert queries[0].type == "issue"
        assert queries[0].month == "2025-01"
        assert "org:test-org" in queries[0].query
        assert "created:2025-01-01..2025-01-31" in queries[0].query

    def test_generate_monthly_queries_multiple_months(self):
        """Test generating monthly queries spanning multiple months."""
        queries = self.generator.generate_monthly_queries(
            organizations=["test-org"],
            start_date="2025-01-01",
            end_date="2025-02-15",
            query_types=["issue"]
        )
        
        assert len(queries) == 2
        
        # First month (January)
        assert queries[0].name == "test-org_issue_2025-01"
        assert queries[0].month == "2025-01"
        assert "created:2025-01-01..2025-01-31" in queries[0].query
        
        # Second month (February, partial)
        assert queries[1].name == "test-org_issue_2025-02"
        assert queries[1].month == "2025-02"
        assert "created:2025-02-01..2025-02-15" in queries[1].query

    def test_generate_monthly_queries_multiple_orgs_and_types(self):
        """Test generating queries for multiple organizations and query types."""
        queries = self.generator.generate_monthly_queries(
            organizations=["org1", "org2"],
            start_date="2025-01-01",
            end_date="2025-01-31",
            query_types=["issue", "pr"]
        )
        
        # 2 orgs × 1 month × 2 query types = 4 queries
        assert len(queries) == 4
        
        # Check we have queries for all combinations
        query_names = [q.name for q in queries]
        expected_names = [
            "org1_issue_2025-01",
            "org1_pr_2025-01", 
            "org2_issue_2025-01",
            "org2_pr_2025-01"
        ]
        assert sorted(query_names) == sorted(expected_names)

    def test_generate_monthly_queries_default_types(self):
        """Test that default query types are used when none specified."""
        queries = self.generator.generate_monthly_queries(
            organizations=["test-org"],
            start_date="2025-01-01",
            end_date="2025-01-31"
            # query_types not specified, should default to ["issue", "bug", "pr"]
        )
        
        assert len(queries) == 3  # 1 org × 1 month × 3 default types
        query_types = [q.type for q in queries]
        assert "issue" in query_types
        assert "pr" in query_types
        # Bug queries have type "issue" but different query content
        assert queries[1].name.endswith("_bug_2025-01")

    def test_generate_repo_queries_single_repo(self):
        """Test generating queries for a single repository."""
        queries = self.generator.generate_repo_queries(
            repositories=["owner/repo"],
            start_date="2025-01-01",
            end_date="2025-01-31",
            query_types=["issue"]
        )
        
        assert len(queries) == 1
        assert queries[0].name == "owner_repo_issue_2025-01"
        assert queries[0].type == "issue"
        assert queries[0].month == "2025-01"
        assert "repo:owner/repo" in queries[0].query

    def test_generate_repo_queries_multiple_repos(self):
        """Test generating queries for multiple repositories."""
        queries = self.generator.generate_repo_queries(
            repositories=["owner1/repo1", "owner2/repo2"],
            start_date="2025-01-01",
            end_date="2025-01-31",
            query_types=["issue", "pr"]
        )
        
        # 2 repos × 1 month × 2 query types = 4 queries
        assert len(queries) == 4
        
        # Check repo name cleaning in query names
        query_names = [q.name for q in queries]
        assert "owner1_repo1_issue_2025-01" in query_names
        assert "owner2_repo2_pr_2025-01" in query_names

    def test_generate_repo_queries_with_special_characters(self):
        """Test that repository names with special characters are cleaned properly."""
        queries = self.generator.generate_repo_queries(
            repositories=["owner/repo-with-dashes"],
            start_date="2025-01-01", 
            end_date="2025-01-31",
            query_types=["issue"]
        )
        
        # Check that dashes and slashes are replaced with underscores
        assert queries[0].name == "owner_repo-with-dashes_issue_2025-01"

    def test_monthly_date_ranges_single_month(self):
        """Test monthly date range generation for single month."""
        ranges = self.generator._generate_monthly_date_ranges("2025-01-15", "2025-01-25")
        
        assert len(ranges) == 1
        start, end = ranges[0]
        assert start == datetime(2025, 1, 1)  # First day of month
        assert end == datetime(2025, 1, 25)   # End date (not end of month)

    def test_monthly_date_ranges_full_months(self):
        """Test monthly date range generation for full months."""
        ranges = self.generator._generate_monthly_date_ranges("2025-01-01", "2025-03-31")
        
        assert len(ranges) == 3
        
        # January
        assert ranges[0] == (datetime(2025, 1, 1), datetime(2025, 1, 31))
        
        # February (28 days in 2025)
        assert ranges[1] == (datetime(2025, 2, 1), datetime(2025, 2, 28))
        
        # March
        assert ranges[2] == (datetime(2025, 3, 1), datetime(2025, 3, 31))

    def test_monthly_date_ranges_partial_end_month(self):
        """Test monthly date range generation with partial end month."""
        ranges = self.generator._generate_monthly_date_ranges("2025-01-01", "2025-02-15")
        
        assert len(ranges) == 2
        
        # January (full month)
        assert ranges[0] == (datetime(2025, 1, 1), datetime(2025, 1, 31))
        
        # February (partial month)
        assert ranges[1] == (datetime(2025, 2, 1), datetime(2025, 2, 15))

    def test_monthly_date_ranges_leap_year(self):
        """Test monthly date range generation handles leap years correctly."""
        ranges = self.generator._generate_monthly_date_ranges("2024-02-01", "2024-02-29")
        
        assert len(ranges) == 1
        start, end = ranges[0]
        assert start == datetime(2024, 2, 1)
        assert end == datetime(2024, 2, 29)  # 2024 is a leap year

    def test_query_templates_constants(self):
        """Test that query templates are correctly defined."""
        expected_templates = {
            "issue": "{scope}:{target} type:issue state:open created:{start}..{end}",
            "bug": "{scope}:{target} type:issue state:open label:bug,defect,\"[type] bug\",\"type: bug\" created:{start}..{end}",
            "pr": "{scope}:{target} type:pr is:merged merged:{start}..{end}"
        }
        assert self.generator.QUERY_TEMPLATES == expected_templates

    def test_search_query_dataclass(self):
        """Test SearchQuery dataclass functionality."""
        query = SearchQuery(
            name="test_query",
            query="org:test type:issue",
            type="issue",
            month="2025-01"
        )
        
        assert query.name == "test_query"
        assert query.query == "org:test type:issue"
        assert query.type == "issue"
        assert query.month == "2025-01"

    def test_search_query_dataclass_defaults(self):
        """Test SearchQuery dataclass default values."""
        query = SearchQuery(
            name="test_query",
            query="org:test type:issue"
        )
        
        assert query.type == "issue"  # Default value
        assert query.month is None    # Default value