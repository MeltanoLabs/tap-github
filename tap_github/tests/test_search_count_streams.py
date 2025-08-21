from __future__ import annotations

import pytest
from unittest.mock import Mock

from tap_github.search_count_streams import (
    ConfigurableSearchCountStream,
    create_configurable_streams,
    validate_stream_config,
)


class TestConfigurableStreams:

    def setup_method(self):
        self.mock_tap = Mock()
        self.mock_tap.config = {
            "search_streams": [
                {
                    "name": "issue",
                    "query_template": "org:{org} type:issue is:open created:{start}..{end}",
                    "count_field": "issue_count"
                },
                {
                    "name": "bug",
                    "query_template": "org:{org} is:issue is:open label:bug,\"[type] bug\",\"type: bug\" created:{start}..{end}",
                    "count_field": "bug_count"
                }
            ],
            "search_scope": {
                "instances": [
                    {
                        "api_url_base": "https://api.github.com",
                        "instance": "github.com",
                        "streams": ["issue", "bug"],
                        "org": ["WordPress"],
                        "repo_breakdown": False
                    }
                ]
            },
            "backfill_start_month": "2025-01",
            "backfill_end_month": "2025-01",
        }

    def test_issue_query_generation(self):
        stream_config = {
            "name": "issue",
            "query_template": "org:{org} type:issue is:open created:{start}..{end}",
            "count_field": "issue_count"
        }
        stream = ConfigurableSearchCountStream(stream_config, self.mock_tap)
        query = stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "issue")
        
        assert query == "org:WordPress type:issue is:open created:2025-01-01..2025-01-31"

    def test_bug_query_generation(self):
        stream_config = {
            "name": "bug",
            "query_template": "org:{org} is:issue is:open label:bug,\"[type] bug\",\"type: bug\" created:{start}..{end}",
            "count_field": "bug_count"
        }
        stream = ConfigurableSearchCountStream(stream_config, self.mock_tap)
        query = stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "bug")
        
        assert query == 'org:WordPress is:issue is:open label:bug,"[type] bug","type: bug" created:2025-01-01..2025-01-31'

    def test_stream_creation_from_config(self):
        streams = create_configurable_streams(self.mock_tap)
        
        assert len(streams) == 2
        assert streams[0].name == "issue_search_counts"
        assert streams[1].name == "bug_search_counts"

    def test_config_validation(self):
        errors = validate_stream_config({
            "name": "test",
            "query_template": "org:{org} type:issue created:{start}..{end}",
            "count_field": "test_count"
        })
        assert errors == []
        
        errors = validate_stream_config({"name": "test"})
        assert len(errors) >= 2


class TestStreamProperties:

    def test_stream_names_and_types(self):
        mock_tap = Mock()
        mock_tap.config = {}
        
        issue_config = {"name": "issue", "query_template": "org:{org} type:issue created:{start}..{end}", "count_field": "issue_count"}
        issue_stream = ConfigurableSearchCountStream(issue_config, mock_tap)
        assert issue_stream.name == "issue_search_counts"
        
        security_config = {"name": "security", "query_template": "org:{org} type:issue label:security created:{start}..{end}", "count_field": "security_count"}
        security_stream = ConfigurableSearchCountStream(security_config, mock_tap)
        assert security_stream.name == "security_search_counts"
