"""Tests for the custom REST and GraphQL paginators."""

from __future__ import annotations

import warnings
from typing import ClassVar
from unittest.mock import MagicMock

import pytest

from tap_github.client import (
    GitHubGraphQLPaginator,
    GitHubRestPaginator,
    GitHubRestStream,
)
from tap_github.repository_streams import (
    DiscussionCommentRepliesStream,
    DiscussionCommentsStream,
    DiscussionsStream,
    RepositoryStream,
    StargazersGraphqlStream,
)
from tap_github.tap import TapGitHub


def _response(
    json_data=None,
    *,
    next_url: str | None = None,
    request_url: str = "https://api.github.com/test?page=1&per_page=100",
) -> MagicMock:
    response = MagicMock()
    response.json.return_value = json_data if json_data is not None else []
    response.links = {"next": {"url": next_url}} if next_url else {}
    response.request.url = request_url
    return response


def _rest_paginator(**overrides) -> GitHubRestPaginator:
    kwargs = {
        "max_results_limit": None,
        "max_per_page": 100,
        "use_cursor_pagination": False,
        "replication_key": None,
        "use_fake_since_parameter": False,
        "records_jsonpath": "$[*]",
    }
    kwargs.update(overrides)
    return GitHubRestPaginator(**kwargs)


def _tap(config: dict | None = None) -> TapGitHub:
    return TapGitHub(config=config or {"repositories": ["org/repo"]})


def _nested(keys: tuple[str, ...], value: dict) -> dict:
    result = value
    for key in reversed(keys):
        result = {key: result}
    return result


def _graphql_page(
    collection_path: tuple[str, ...],
    records_key: str,
    records: list[dict],
) -> dict:
    collection = {
        "pageInfo": {"hasNextPage_0": True, "endCursor_0": "next"},
        records_key: records,
    }
    return _nested(("data", "repository", *collection_path), collection)


class _TestRestStream(GitHubRestStream):
    name = "test_rest"
    path = "/test"
    schema: ClassVar[dict] = {
        "type": "object",
        "properties": {"id": {"type": "integer"}},
    }
    replication_key = "updated_at"
    use_fake_since_parameter = True


@pytest.mark.parametrize(
    ("current", "next_url", "expected"),
    [
        (None, "https://api.github.com/test?page=2", 2),
        (3, "https://api.github.com/test?per_page=100", 4),
        (3, "https://api.github.com/test?page=8", 8),
    ],
)
def test_rest_page_number_tokens(current, next_url, expected):
    paginator = _rest_paginator()
    paginator._value = current

    assert paginator.get_next(_response(next_url=next_url)) == expected


@pytest.mark.parametrize(
    ("next_url", "expected"),
    [
        ("https://api.github.com/test?after=cursor-2", "cursor-2"),
        ("https://api.github.com/test?page=2", None),
    ],
)
def test_rest_cursor_tokens(next_url, expected):
    paginator = _rest_paginator(use_cursor_pagination=True)

    assert paginator.get_next(_response(next_url=next_url)) == expected


@pytest.mark.parametrize(
    ("json_data", "next_url", "expected"),
    [
        ([{"id": 1}], "next", True),
        ([], "next", False),
        ([{"id": 1}], None, False),
    ],
)
def test_rest_has_more_requires_records_and_a_next_link(
    json_data,
    next_url,
    expected,
):
    paginator = _rest_paginator()

    assert paginator.has_more(_response(json_data, next_url=next_url)) is expected


@pytest.mark.parametrize(
    ("current", "limit", "cursor_mode", "expected"),
    [
        (2, 200, False, False),
        (2, 300, False, True),
        ("cursor", 100, True, True),
    ],
)
def test_rest_max_results_limit(current, limit, cursor_mode, expected):
    paginator = _rest_paginator(
        max_results_limit=limit,
        use_cursor_pagination=cursor_mode,
    )
    paginator._value = current
    response = _response([{"id": 1}], next_url="next")

    assert paginator.has_more(response) is expected


@pytest.mark.parametrize(
    ("record_date", "since", "expected"),
    [
        ("2024-06-01T00:00:00Z", "2025-01-01T00:00:00Z", False),
        ("2025-06-01T00:00:00Z", "2025-01-01T00:00:00Z", True),
        ("2025-06-01T00:00:00Z", None, True),
    ],
)
def test_rest_fake_since_early_exit(record_date, since, expected):
    paginator = _rest_paginator(
        replication_key="starred_at",
        use_fake_since_parameter=True,
    )
    query = "direction=desc"
    if since:
        query = f"fake_since={since}&{query}"
    response = _response(
        [{"starred_at": record_date}],
        next_url="next",
        request_url=f"https://api.github.com/test?{query}",
    )

    assert paginator.has_more(response) is expected


def test_rest_fake_since_supports_commit_timestamp():
    paginator = _rest_paginator(
        replication_key="commit_timestamp",
        use_fake_since_parameter=True,
    )
    response = _response(
        [{"commit": {"committer": {"date": "2024-01-01T00:00:00Z"}}}],
        next_url="next",
        request_url=(
            "https://api.github.com/test?fake_since=2025-01-01T00:00:00Z&direction=desc"
        ),
    )

    assert paginator.has_more(response) is False


def test_rest_has_more_uses_records_jsonpath():
    paginator = _rest_paginator(records_jsonpath="$.items[*]")

    assert paginator.has_more(_response({"items": [{"id": 1}]}, next_url="next"))


def test_graphql_paginator_advances_deepest_cursor():
    paginator = GitHubGraphQLPaginator()
    paginator._value = {
        "nextPageCursor_0": "outer",
        "nextPageCursor_1": "old-inner",
    }
    response = _response(
        {
            "data": {
                "pageInfo": {
                    "hasNextPage_0": False,
                    "endCursor_0": "unused",
                },
                "nested": {
                    "pageInfo": {
                        "hasNextPage_1": True,
                        "endCursor_1": "new-inner",
                    }
                },
            }
        }
    )

    paginator.advance(response)

    assert paginator.current_value == {
        "nextPageCursor_0": "outer",
        "nextPageCursor_1": "new-inner",
    }
    assert paginator.finished is False


def test_graphql_paginator_finishes_without_next_page():
    paginator = GitHubGraphQLPaginator()
    response = _response({"pageInfo": {"hasNextPage_0": False}})

    paginator.advance(response)

    assert paginator.finished is True


def test_rest_stream_passes_explicit_attributes_to_paginator():
    paginator = _TestRestStream(_tap()).get_new_paginator()

    assert isinstance(paginator, GitHubRestPaginator)
    assert paginator._max_results_limit is None
    assert paginator._max_per_page == 100
    assert paginator._use_cursor_pagination is False
    assert paginator._replication_key == "updated_at"
    assert paginator._use_fake_since_parameter is True
    assert paginator._records_jsonpath == "$[*]"
    assert not hasattr(paginator, "stream")


def test_repository_search_limit_is_available_before_path_access():
    stream = RepositoryStream(
        _tap({"searches": [{"name": "taps", "query": "topic:singer-tap"}]})
    )

    paginator = stream.get_new_paginator()

    assert isinstance(paginator, GitHubRestPaginator)
    assert paginator._max_results_limit == 1000
    assert not hasattr(paginator, "stream")


def test_request_records_uses_new_paginator_without_legacy_warning():
    stream = _TestRestStream(_tap())
    first_response = _response(
        [{"id": 1, "updated_at": "2025-01-01T00:00:00Z"}],
        next_url="https://api.github.com/test?page=2&per_page=100",
    )
    second_response = _response([{"id": 2, "updated_at": "2025-01-02T00:00:00Z"}])
    stream._request = MagicMock(side_effect=[first_response, second_response])

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        records = list(stream.request_records(None))

    assert [record["id"] for record in records] == [1, 2]
    assert stream._request.call_count == 2
    second_request = stream._request.call_args_list[1].args[0]
    assert "page=2" in second_request.url
    assert not any("get_next_page_token" in str(item.message) for item in caught)


@pytest.mark.parametrize(
    ("stream_type", "response_json"),
    [
        (
            StargazersGraphqlStream,
            _graphql_page(
                ("stargazers",),
                "edges",
                [{"starred_at": "2024-01-01T00:00:00Z"}],
            ),
        ),
        (
            DiscussionsStream,
            _graphql_page(
                ("discussions",),
                "nodes",
                [{"updated_at": "2024-01-01T00:00:00Z"}],
            ),
        ),
        (
            DiscussionCommentsStream,
            _graphql_page(
                ("discussion", "comments"),
                "nodes",
                [{"created_at": "2024-01-01T00:00:00Z"}],
            ),
        ),
        (
            DiscussionCommentRepliesStream,
            _graphql_page(
                ("discussion", "comments"),
                "nodes",
                [{"replies": {"nodes": [{"created_at": "2024-01-01T00:00:00Z"}]}}],
            ),
        ),
    ],
)
def test_graphql_stream_paginators_preserve_incremental_early_exit(
    stream_type,
    response_json,
):
    paginator = stream_type(_tap()).get_new_paginator()
    response = _response(
        response_json,
        request_url=(
            "https://api.github.com/graphql?since=2025-01-01T00%3A00%3A00%2B00%3A00"
        ),
    )

    paginator.advance(response)

    assert paginator.finished is True
    assert type(paginator) is GitHubGraphQLPaginator
    assert not hasattr(paginator, "stream")
