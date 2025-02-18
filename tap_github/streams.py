from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from tap_github.organization_streams import (
    OrganizationStream,
    TeamMembersStream,
    TeamRolesStream,
    TeamsStream,
)
from tap_github.repository_streams import (
    AnonymousContributorsStream,
    AssigneesStream,
    BranchesStream,
    CollaboratorsStream,
    CommitCommentsStream,
    CommitsStream,
    CommunityProfileStream,
    ContributorsStream,
    DependenciesStream,
    DependentsStream,
    EventsStream,
    ExtraMetricsStream,
    IssueCommentsStream,
    IssueEventsStream,
    IssuesStream,
    LabelsStream,
    LanguagesStream,
    MilestonesStream,
    ProjectCardsStream,
    ProjectColumnsStream,
    ProjectsStream,
    PullRequestCommits,
    PullRequestDiffsStream,
    PullRequestsStream,
    ReadmeHtmlStream,
    ReadmeStream,
    ReleasesStream,
    RepositoryStream,
    ReviewCommentsStream,
    ReviewsStream,
    StargazersGraphqlStream,
    StargazersStream,
    StatsContributorsStream,
    TagsStream,
    TrafficClonesStream,
    TrafficPageViewsStream,
    TrafficReferralPathsStream,
    TrafficReferrersStream,
    WorkflowRunJobsStream,
    WorkflowRunsStream,
    WorkflowsStream,
)
from tap_github.user_streams import StarredStream, UserContributedToStream, UserStream

if TYPE_CHECKING:
    from singer_sdk.streams.core import Stream


class Streams(Enum):
    """
    Represents all streams our tap supports, and which queries (by username, by organization, etc.) you can use.
    """  # noqa: E501

    valid_queries: set[str]
    streams: list[type[Stream]]

    def __init__(self, valid_queries: set[str], streams: list[type[Stream]]) -> None:
        self.valid_queries = valid_queries
        self.streams = streams

    REPOSITORY = (
        {"repositories", "organizations", "searches"},
        [
            AnonymousContributorsStream,
            AssigneesStream,
            BranchesStream,
            CollaboratorsStream,
            CommitCommentsStream,
            CommitsStream,
            CommunityProfileStream,
            ContributorsStream,
            DependenciesStream,
            DependentsStream,
            EventsStream,
            IssueCommentsStream,
            IssueEventsStream,
            IssuesStream,
            LabelsStream,
            LanguagesStream,
            MilestonesStream,
            ProjectCardsStream,
            ProjectColumnsStream,
            ProjectsStream,
            PullRequestCommits,
            PullRequestDiffsStream,
            PullRequestsStream,
            ReadmeHtmlStream,
            ReadmeStream,
            ReleasesStream,
            ExtraMetricsStream,
            RepositoryStream,
            ReviewCommentsStream,
            ReviewsStream,
            StargazersGraphqlStream,
            StargazersStream,
            StatsContributorsStream,
            TagsStream,
            TrafficClonesStream,
            TrafficPageViewsStream,
            TrafficReferralPathsStream,
            TrafficReferrersStream,
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
    def all_valid_queries(cls) -> set[str]:
        return set.union(*[stream.valid_queries for stream in Streams])
