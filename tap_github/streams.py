from enum import Enum
from typing import Type, Set, List

from singer_sdk.streams.core import Stream

from tap_github.repository_streams import (
    AnonymousContributorsStream,
    AssigneesStream,
    CollaboratorsStream,
    CommitCommentsStream,
    CommitsStream,
    CommunityProfileStream,
    ContributorsStream,
    DependentsStream,
    EventsStream,
    IssueCommentsStream,
    IssueEventsStream,
    IssuesStream,
    LanguagesStream,
    MilestonesStream,
    ProjectCardsStream,
    ProjectColumnsStream,
    ProjectsStream,
    PullRequestCommits,
    PullRequestsStream,
    ReadmeHtmlStream,
    ReadmeStream,
    ReleasesStream,
    RepositoryStream,
    ReviewCommentsStream,
    ReviewsStream,
    StargazersStream,
    StatsContributorsStream,
    WorkflowRunJobsStream,
    WorkflowRunsStream,
    WorkflowsStream,
)
from tap_github.user_streams import (
    StarredStream,
    UserContributedToStream,
    UserStream,
)
from tap_github.organization_streams import (
    OrganizationStream,
    TeamMembersStream,
    TeamRolesStream,
    TeamsStream,
)


class Streams(Enum):
    """
    Represents all streams our tap supports, and which queries (by username, by organization, etc.) you can use.
    """

    valid_queries: Set[str]
    streams: List[Type[Stream]]

    def __init__(self, valid_queries: Set[str], streams: List[Type[Stream]]):
        self.valid_queries = valid_queries
        self.streams = streams

    REPOSITORY = (
        {"repositories", "organizations", "searches"},
        [
            AnonymousContributorsStream,
            AssigneesStream,
            CollaboratorsStream,
            CommitCommentsStream,
            CommitsStream,
            CommunityProfileStream,
            ContributorsStream,
            DependentsStream,
            EventsStream,
            IssueCommentsStream,
            IssueEventsStream,
            IssuesStream,
            LanguagesStream,
            MilestonesStream,
            ProjectCardsStream,
            ProjectColumnsStream,
            ProjectsStream,
            PullRequestCommits,
            PullRequestsStream,
            ReadmeHtmlStream,
            ReadmeStream,
            ReleasesStream,
            RepositoryStream,
            ReviewCommentsStream,
            ReviewsStream,
            StargazersStream,
            StatsContributorsStream,
            WorkflowRunJobsStream,
            WorkflowRunsStream,
            WorkflowsStream,
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
        [OrganizationStream, TeamMembersStream, TeamRolesStream, TeamsStream],
    )

    @classmethod
    def all_valid_queries(cls):
        return set.union(*[stream.valid_queries for stream in Streams])
