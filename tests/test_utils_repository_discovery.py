"""Tests for repository discovery utility."""

from __future__ import annotations

import pytest
from unittest.mock import Mock, patch

from tap_github.utils.repository_discovery import RepositoryDiscovery, GitHubInstance


class TestRepositoryDiscovery:
    """Test suite for RepositoryDiscovery utility."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create a mock requester with the methods our utility expects
        self.mock_requester = Mock()
        self.mock_requester._make_batch_graphql_request = Mock()
        self.mock_requester._make_graphql_request_for_instance = Mock()
        
        self.discovery = RepositoryDiscovery(self.mock_requester)
        
        # Sample response data
        self.sample_response = {
            "data": {
                "organization": {
                    "repositories": {
                        "nodes": [
                            {"nameWithOwner": "test/repo1", "issues": {"totalCount": 50}},
                            {"nameWithOwner": "test/repo2", "issues": {"totalCount": 30}},
                            {"nameWithOwner": "test/repo3", "issues": {"totalCount": 80}},
                            {"nameWithOwner": "test/repo4", "issues": {"totalCount": 10}}
                        ]
                    }
                }
            }
        }

    def test_get_top_repos_by_issues_default_instance(self):
        """Test getting top repos by issue count for default github.com instance."""
        self.mock_requester._make_batch_graphql_request.return_value = self.sample_response
        
        repos = self.discovery.get_top_repos("test-org", 3, "issues")
        
        # Should return repos sorted by issue count descending
        expected = ["test/repo3", "test/repo1", "test/repo2"]  # 80, 50, 30 issues
        assert repos == expected
        
        # Verify the correct method was called
        self.mock_requester._make_batch_graphql_request.assert_called_once()
        
        # Check the GraphQL query includes issue count
        call_args = self.mock_requester._make_batch_graphql_request.call_args
        query = call_args[0][0]
        assert "issues" in query
        assert "totalCount" in query

    def test_get_top_repos_by_stars(self):
        """Test getting top repos by star count."""
        response = {
            "data": {
                "organization": {
                    "repositories": {
                        "nodes": [
                            {"nameWithOwner": "test/repo1"},
                            {"nameWithOwner": "test/repo2"},
                            {"nameWithOwner": "test/repo3"}
                        ]
                    }
                }
            }
        }
        self.mock_requester._make_batch_graphql_request.return_value = response
        
        repos = self.discovery.get_top_repos("test-org", 3, "stars")
        
        expected = ["test/repo1", "test/repo2", "test/repo3"]
        assert repos == expected
        
        # Check the GraphQL query sorts by stars
        call_args = self.mock_requester._make_batch_graphql_request.call_args
        query = call_args[0][0]
        assert "STARGAZERS" in query

    def test_get_top_repos_by_forks(self):
        """Test getting top repos by fork count."""
        response = {
            "data": {
                "organization": {
                    "repositories": {
                        "nodes": [
                            {"nameWithOwner": "test/repo1"},
                            {"nameWithOwner": "test/repo2"}
                        ]
                    }
                }
            }
        }
        self.mock_requester._make_batch_graphql_request.return_value = response
        
        repos = self.discovery.get_top_repos("test-org", 2, "forks")
        
        expected = ["test/repo1", "test/repo2"]
        assert repos == expected
        
        # Check the GraphQL query sorts by forks
        call_args = self.mock_requester._make_batch_graphql_request.call_args
        query = call_args[0][0]
        assert "FORKS" in query

    def test_get_top_repos_by_updated(self):
        """Test getting top repos by updated time."""
        response = {
            "data": {
                "organization": {
                    "repositories": {
                        "nodes": [
                            {"nameWithOwner": "test/repo1"},
                            {"nameWithOwner": "test/repo2"}
                        ]
                    }
                }
            }
        }
        self.mock_requester._make_batch_graphql_request.return_value = response
        
        repos = self.discovery.get_top_repos("test-org", 2, "updated")
        
        expected = ["test/repo1", "test/repo2"]
        assert repos == expected
        
        # Check the GraphQL query sorts by updated time
        call_args = self.mock_requester._make_batch_graphql_request.call_args
        query = call_args[0][0]
        assert "UPDATED_AT" in query

    def test_get_top_repos_with_github_enterprise_instance(self):
        """Test getting repos from a GitHub Enterprise instance."""
        instance = GitHubInstance(
            name="enterprise.company.com",
            api_url_base="https://enterprise.company.com/api",
            auth_token="enterprise-token"
        )
        
        self.mock_requester._make_graphql_request_for_instance.return_value = self.sample_response
        
        repos = self.discovery.get_top_repos("test-org", 2, "issues", instance)
        
        expected = ["test/repo3", "test/repo1"]  # Top 2 by issue count
        assert repos == expected
        
        # Verify the instance-specific method was called
        self.mock_requester._make_graphql_request_for_instance.assert_called_once_with(
            instance, 
            pytest.any(str),  # The GraphQL query
            {"org": "test-org", "limit": 2}
        )

    def test_invalid_sort_by_parameter(self):
        """Test that invalid sort_by parameter raises ValueError."""
        with pytest.raises(ValueError, match="Invalid sort_by: invalid"):
            self.discovery.get_top_repos("test-org", 5, "invalid")

    def test_empty_response_handling(self):
        """Test handling of empty or missing response data."""
        # Test completely empty response
        self.mock_requester._make_batch_graphql_request.return_value = {}
        repos = self.discovery.get_top_repos("test-org", 5, "stars")
        assert repos == []
        
        # Test response without organization data
        self.mock_requester._make_batch_graphql_request.return_value = {"data": {}}
        repos = self.discovery.get_top_repos("test-org", 5, "stars")
        assert repos == []
        
        # Test response with empty repositories
        empty_org_response = {
            "data": {
                "organization": {
                    "repositories": {"nodes": []}
                }
            }
        }
        self.mock_requester._make_batch_graphql_request.return_value = empty_org_response
        repos = self.discovery.get_top_repos("test-org", 5, "stars")
        assert repos == []

    def test_graphql_request_exception_handling(self):
        """Test that GraphQL request exceptions are handled gracefully."""
        self.mock_requester._make_batch_graphql_request.side_effect = Exception("API Error")
        
        repos = self.discovery.get_top_repos("test-org", 5, "stars")
        assert repos == []

    def test_limit_parameter_respected(self):
        """Test that the limit parameter is properly respected."""
        self.mock_requester._make_batch_graphql_request.return_value = self.sample_response
        
        repos = self.discovery.get_top_repos("test-org", 2, "issues")
        
        # Should only return top 2 repos by issue count
        expected = ["test/repo3", "test/repo1"]  # 80, 50 issues
        assert repos == expected
        assert len(repos) == 2

    def test_sort_fields_mapping(self):
        """Test that SORT_FIELDS mapping is correct."""
        expected_mapping = {
            "issues": "STARGAZERS",  # Note: issues uses stars sort then local issue sort
            "stars": "STARGAZERS",
            "forks": "FORKS", 
            "updated": "UPDATED_AT"
        }
        assert self.discovery.SORT_FIELDS == expected_mapping

    def test_organization_not_found(self):
        """Test handling when organization is not found."""
        response = {
            "data": {
                "organization": None
            }
        }
        self.mock_requester._make_batch_graphql_request.return_value = response
        
        repos = self.discovery.get_top_repos("nonexistent-org", 5, "stars")
        assert repos == []