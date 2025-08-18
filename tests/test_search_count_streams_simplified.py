"""Tests for simplified search count streams."""

from __future__ import annotations

from tap_github.search_count_streams_simplified import IssueSearchCountStream, PRSearchCountStream
from tap_github.tap import TapGitHub


def test_simplified_issue_stream_schema():
    """Test simplified issue stream schema."""
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
    assert "issue_count" in stream.schema["properties"]


def test_simplified_partition_generation():
    """Test simplified partition generation."""
    tap = TapGitHub(
        config={
            "auth_token": "test-token",
            "search_count_queries": [
                {"name": "test_query", "query": "org:test type:issue", "type": "issue"},
            ],
        }
    )
    
    stream = IssueSearchCountStream(tap=tap)
    partitions = stream.partitions
    
    assert len(partitions) == 1
    assert partitions[0]["search_name"] == "test_query"
    assert partitions[0]["search_query"] == "org:test type:issue"
    assert partitions[0]["source"] == "github.com"


def test_simplified_batch_query_building():
    """Test simplified batch query building."""
    tap = TapGitHub(
        config={
            "auth_token": "test-token",
            "batch_query_size": 2,
            "search_count_queries": [
                {"name": "test", "query": "test", "type": "issue"}
            ],
        }
    )
    
    stream = IssueSearchCountStream(tap=tap)
    batch_query = stream._build_batch_query(["query1", "query2"])
    
    assert "search0:" in batch_query
    assert "search1:" in batch_query
    assert "$q0: String!" in batch_query
    assert "$q1: String!" in batch_query
    assert "type: ISSUE" in batch_query
    assert "issueCount" in batch_query


def test_monthly_partitions():
    """Test monthly partition generation."""
    tap = TapGitHub(
        config={
            "auth_token": "test-token",
            "search_orgs": ["testorg"],
            "date_range": {
                "start": "2025-01-01",
                "end": "2025-01-31"
            }
        }
    )
    
    stream = IssueSearchCountStream(tap=tap)
    partitions = stream.partitions
    
    # Should have 2 partitions (issue + bug) for 1 org, 1 month, 1 instance
    assert len(partitions) == 2
    assert any("issue" in p["search_query"] for p in partitions)
    assert any("bug" in p["search_query"] for p in partitions)
    assert all(p["month"] == "2025-01" for p in partitions)


def test_pr_stream():
    """Test PR stream differences."""
    tap = TapGitHub(
        config={
            "auth_token": "test-token",
            "search_count_queries": [
                {"name": "test_pr", "query": "org:test type:pr", "type": "pr"},
            ],
        }
    )
    
    stream = PRSearchCountStream(tap=tap)
    partitions = stream.partitions
    
    assert len(partitions) == 1
    assert stream.count_field == "pr_count"
    assert stream.stream_type == "pr"
    
    # Test batch query uses correct type
    batch_query = stream._build_batch_query(["test query"])
    assert "type: PULLREQUEST" in batch_query
    assert "prCount" in batch_query