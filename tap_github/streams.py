from enum import Enum
from typing import Type, Set, List

from singer_sdk.streams.core import Stream

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
from tap_github.organization_streams import (
    OrganizationStream,
    TeamsStream,
    TeamMembersStream,
    TeamRolesStream,
)


class Streams(Enum):
    valid_queries: Set[str]
    streams: List[Type[Stream]]

    def __init__(self, valid_queries: Set[str], streams: List[Type[Stream]]):
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
    ORGANIZATIONS = (
        {"organizations"},
        [OrganizationStream, TeamsStream, TeamMembersStream, TeamRolesStream],
    )

    @classmethod
    def all_valid_queries(cls):
        return set.union(*[stream.valid_queries for stream in Streams])
