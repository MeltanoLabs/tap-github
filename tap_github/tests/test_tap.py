import os
import logging
import pytest

from unittest.mock import patch

from singer_sdk.streams.core import Stream

from tap_github.tap import TapGitHub
from tap_github.tests.test_core import ORG_LEVEL_TOKEN

from .fixtures import repo_list_config

repo_list_2 = [
    "MeltanoLabs/tap-github",
    "MeltanoLabs/tap-gitlab",
    "MeltanoLabs/target-athena",
]


@pytest.mark.repo_list(repo_list_2)
def test_validate_repo_list_config(repo_list_config):
    """Verify that the repositories list is parsed correctly"""
    repo_list_context = [
        {"org": repo.split("/")[0], "repo": repo.split("/")[1]} for repo in repo_list_2
    ]
    tap = TapGitHub(config=repo_list_config)
    partitions = tap.streams["repositories"].partitions
    assert partitions == repo_list_context


# TODO - patch mock sync_children to use ORG_LEVEL_TOKEN for the "collaborators stream"
def mock_sync_chidren(self, child_context: dict) -> None:
        for child_stream in self.child_streams:
            # Use org:read access level credentials for collaborators stream
            if child_stream.name in ["collaborators"]:
                ORG_LEVEL_TOKEN = os.environ.get('ORG_LEVEL_TOKEN')
                if not ORG_LEVEL_TOKEN:
                    logging.warning('No "ORG_LEVEL_TOKEN" found. Skipping collaborators stream sync.')
                    continue
                SAVED_GTHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
                os.environ['GITHUB_TOKEN'] = ORG_LEVEL_TOKEN
                child_stream.sync(context=child_context)
                os.environ['GITHUB_TOKEN'] = SAVED_GTHUB_TOKEN or ""
                continue
            
            # default behavior:
            if child_stream.selected or child_stream.has_selected_descendents:
                child_stream.sync(context=child_context)

                
# Mock the update method
# @patch.object(Stream, '_sync_children')
@pytest.mark.repo_list(repo_list_2)
def test_get_a_repository_in_repo_list_mode(capsys, repo_list_config):
    """
    Discover the catalog, and request 2 repository records
    """
    tap1 = TapGitHub(config=repo_list_config)
    tap1.run_discovery()
    catalog = tap1.catalog_dict
    # disable child streams
    # FIXME: this does not work, the child streams are still fetched
    # deselect_all_streams(catalog)
    # set_catalog_stream_selected(
    #     catalog=catalog, stream_name="repositories", selected=True
    # )
    # discard previous output to stdout (potentially from other tests)
    capsys.readouterr()
    tap2 = TapGitHub(config=repo_list_config, catalog=catalog)
    tap2.sync_all()
    captured = capsys.readouterr()
    # Verify we got the right number of records (one per repo in the list)
    assert captured.out.count('{"type": "RECORD", "stream": "repositories"') == len(
        repo_list_2
    )
