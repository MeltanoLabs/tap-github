from enum import Enum
from typing import Mapping, Any, Type

from singer_sdk.streams.core import Stream

from tap_github.organization_streams import OrganizationStream
from tap_github.repository_streams import (
    AnonymousContributorsStream,
    CommitsStream,
    CommunityProfileStream,
    ContributorsStream,
    EventsStream,
    IssueCommentsStream,
    IssueEventsStream,
    IssuesStream,
    LanguagesStream,
    PullRequestsStream,
    ReadmeHtmlStream,
    ReadmeStream,
    RepositoryStream,
    StargazersStream,
    StatsContributorsStream,
    AssigneesStream,
    CollaboratorsStream,
    ReviewsStream,
    ReviewCommentsStream,
    ProjectsStream,
    ProjectColumnsStream,
    ProjectCardsStream,
    PullRequestCommits,
    MilestonesStream,
    CommitCommentsStream,
    ReleasesStream,
)
from tap_github.user_streams import (
    StarredStream,
    UserContributedToStream,
    UserStream,
)


class Streams(Enum):
    valid_queries: set[str]
    streams: list[Type[Stream]]

    def __init__(self, valid_queries: set[str], streams: list[Type[Stream]]):
        self.valid_queries = valid_queries
        self.streams = streams

    REPOSITORY = (
        {"repositories", "organizations", "searches"},
        [
            AnonymousContributorsStream,
            CommitsStream,
            CommitCommentsStream,
            CommunityProfileStream,
            ContributorsStream,
            EventsStream,
            MilestonesStream,
            ReleasesStream,
            CollaboratorsStream,
            AssigneesStream,
            IssuesStream,
            IssueCommentsStream,
            IssueEventsStream,
            LanguagesStream,
            PullRequestsStream,
            PullRequestCommits,
            ReviewsStream,
            ReviewCommentsStream,
            ReadmeHtmlStream,
            ReadmeStream,
            RepositoryStream,
            StargazersStream,
            StatsContributorsStream,
            ProjectsStream,
            ProjectColumnsStream,
            ProjectCardsStream,
        ],
    )
    USERS = (
        {"user_usernames", "user_ids"},
        [
            StarredStream,
            UserContributedToStream,
            UserStream,
        ],
    )
    ORGANIZATIONS = ({"organizations"}, [OrganizationStream])

    @classmethod
    def all_valid_queries(cls):
        return set.union(*[stream.valid_queries for stream in Streams])
