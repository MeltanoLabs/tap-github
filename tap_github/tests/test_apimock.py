"""Provides json-server.py interface for mocking client APIs."""


import datetime
import json
import logging
from copy import deepcopy

from dotenv import load_dotenv

from tap_github.tap import TapGitHub

import requests_mock

JSONSERVER_PORT = 8080
JSONSERVER_DB = "resources/json-server-db.json"


SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "searches": [
        {
            "name": "tap_something",
            "query": "target-athena+fork:only",
        }
    ],
}


load_dotenv()  # take environment variables from .env.


class APIMockTest:
    def __init__(self, port, db_file: str, mocker: requests_mock.Mocker) -> None:
        self.port = 3000
        self.db_file = db_file
        self.mocker = mocker

    @property
    def api_base_url(self) -> str:
        return "https://api.github.com"

    def mock(self) -> None:
        self.mocker.get(self.api_base_url, text="resp")


@requests_mock.Mocker(kw="mock", real_http=True)
def test_sync_with_mock(**kwargs):
    recording = True
    mocker = kwargs["mock"]
    api_mock = APIMockTest(JSONSERVER_PORT, JSONSERVER_DB, mocker)
    api_mock.mock()
    config = deepcopy(SAMPLE_CONFIG)
    config["api_url_base"] = api_mock.api_base_url
    tap = TapGitHub(config=config, parse_env_config=True)
    history = api_mock.mocker.request_history
    try:
        tap.sync_all()
    except Exception as ex:
        logging.warning(
            f"Test failed. History of {len(history)} HTTP requests:\n\n"
            + "\n".join([f"{hist.method} {hist.url}" for hist in list(history)])
        )
        raise ex
    finally:
        if recording:
            with open(".output/json-recording.jsonl", "w") as jsonl:
                for hist in list(history):
                    jsonl.write(json.dumps({f"{hist.method} {hist.url}": "none"}))
                    if hasattr(hist, "response"):
                        jsonl.write(json.dumps(hist.response))
                    jsonl.write("\n")
