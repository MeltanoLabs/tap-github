"""Tests for search count streams."""

from __future__ import annotations

from tap_github.search_count_streams import IssueSearchCountStream, PRSearchCountStream
from tap_github.tap import TapGitHub


def test_issue_search_count_stream_schema():
    """Test that IssueSearchCountStream has correct schema."""
    tap = TapGitHub(
        config={
            "auth_token": "test-token",
            "search_count_queries": [
                {"name": "test", "query": "test", "type": "issue"}
            ],
        }
    )
    stream = IssueSearchCountStream(tap=tap)

    assert stream.name == "issue_search_counts"
    assert "search_name" in stream.schema["properties"]
    assert "search_query" in stream.schema["properties"]
    assert "source" in stream.schema["properties"]
    assert "issue_count" in stream.schema["properties"]


def test_pr_search_count_stream_schema():
    """Test that PRSearchCountStream has correct schema."""
    tap = TapGitHub(
        config={
            "auth_token": "test-token",
            "search_count_queries": [{"name": "test", "query": "test", "type": "pr"}],
        }
    )
    stream = PRSearchCountStream(tap=tap)

    assert stream.name == "pr_search_counts"
    assert "search_name" in stream.schema["properties"]
    assert "search_query" in stream.schema["properties"]
    assert "source" in stream.schema["properties"]
    assert "pr_count" in stream.schema["properties"]


def test_partitions_generation():
    """Test that partitions are generated correctly."""
    tap = TapGitHub(
        config={
            "auth_token": "default-token",
            "search_count_queries": [
                {
                    "name": "open_issues",
                    "query": "org:Automattic type:issue state:open",
                    "type": "issue",
                },
                {
                    "name": "merged_prs",
                    "query": "org:Automattic type:pr is:merged",
                    "type": "pr",
                },
            ],
            "github_instances": [
                {
                    "name": "github.com",
                    "api_url_base": "https://api.github.com",
                    "auth_token": "token1",
                },
                {
                    "name": "github.example.com",
                    "api_url_base": "https://github.example.com/api",
                    "auth_token": "token2",
                },
            ],
        }
    )

    issue_stream = IssueSearchCountStream(tap=tap)
    issue_partitions = issue_stream.get_partitions()

    # Should have 1 issue query × 2 instances = 2 partitions
    assert len(issue_partitions) == 2
    assert issue_partitions[0]["search_name"] == "open_issues"
    assert issue_partitions[0]["source"] == "github.com"
    assert issue_partitions[1]["source"] == "github.example.com"

    pr_stream = PRSearchCountStream(tap=tap)
    pr_partitions = pr_stream.get_partitions()

    # Should have 1 PR query × 2 instances = 2 partitions
    assert len(pr_partitions) == 2
    assert pr_partitions[0]["search_name"] == "merged_prs"


def test_graphql_query():
    """Test that the GraphQL query is correct."""
    tap = TapGitHub(
        config={
            "auth_token": "test-token",
            "search_count_queries": [
                {"name": "test", "query": "test", "type": "issue"}
            ],
        }
    )
    stream = IssueSearchCountStream(tap=tap)

    query = stream.query
    assert "search(query: $searchQuery, type: ISSUE, first: 1)" in query
    assert "issueCount" in query
    assert "rateLimit" in query
