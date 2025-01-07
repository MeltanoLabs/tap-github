"""Utility functions for scraping https://github.com

Inspired by https://github.com/dogsheep/github-to-sqlite/pull/70
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

import requests

if TYPE_CHECKING:
    from collections.abc import Iterable

    from bs4 import NavigableString, Tag

used_by_regex = re.compile(" {3}Used by ")
contributors_regex = re.compile(" {3}Contributors ")


def scrape_dependents(
    response: requests.Response, logger: logging.Logger | None = None
) -> Iterable[dict[str, Any]]:
    from bs4 import BeautifulSoup

    logger = logger or logging.getLogger("scraping")

    soup = BeautifulSoup(response.content, "html.parser")
    # Navigate through Package toggle if present
    base_url = urlparse(response.url).hostname or "github.com"
    options = soup.find_all("a", class_="select-menu-item")
    links = [link["href"] for link in options] if len(options) > 0 else [response.url]

    logger.debug(links)

    for link in links:
        yield from _scrape_dependents(f"https://{base_url}/{link}", logger)


def _scrape_dependents(url: str, logger: logging.Logger) -> Iterable[dict[str, Any]]:
    # Optional dependency:
    from bs4 import BeautifulSoup

    s = requests.Session()

    while url:
        logger.debug(url)
        response = s.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        repo_names = [
            (a["href"] if not isinstance(a["href"], list) else a["href"][0]).lstrip("/")
            for a in soup.select("a[data-hovercard-type=repository]")
        ]
        stars = [
            int(s.next_sibling.strip())
            for s in soup.find_all("svg", {"class": "octicon octicon-star"})
        ]
        forks = [
            int(s.next_sibling.strip())
            for s in soup.find_all("svg", {"class": "octicon octicon-repo-forked"})
        ]

        if not len(repo_names) == len(stars) == len(forks):
            raise IndexError(
                "Could not find star and fork info. Maybe the GitHub page format has changed?"  # noqa: E501
            )

        repos = [
            {"name_with_owner": name, "stars": s, "forks": f}
            for name, s, f in zip(repo_names, stars, forks)
        ]

        logger.debug(repos)

        yield from repos

        # next page?
        try:
            next_link: Tag = soup.select(".paginate-container")[0].find_all(
                "a", text="Next"
            )[0]
        except IndexError:
            break
        if next_link is not None:
            href = next_link["href"]
            url = str(href if not isinstance(href, list) else href[0])
            time.sleep(1)
        else:
            url = ""


def parse_counter(tag: Tag | NavigableString | None) -> int:
    """
    Extract a count of [issues|PR|contributors...] from an HTML tag.
    For very high numbers, we only get an approximate value as github
    does not provide the actual number.
    """
    if not tag:
        return 0
    try:
        if tag == "\n":
            return 0
        title = tag["title"]  # type: ignore
        if isinstance(title, str):
            title_string = cast(str, title)
        else:
            title_string = cast(str, title[0])
        return int(title_string.strip().replace(",", "").replace("+", ""))
    except (KeyError, ValueError) as e:
        raise IndexError(
            f"Could not parse counter {tag}. Maybe the GitHub page format has changed?"
        ) from e


def scrape_metrics(
    response: requests.Response, logger: logging.Logger | None = None
) -> Iterable[dict[str, Any]]:
    from bs4 import BeautifulSoup

    logger = logger or logging.getLogger("scraping")

    soup = BeautifulSoup(response.content, "html.parser")

    try:
        issues = parse_counter(soup.find("span", id="issues-repo-tab-count"))
        prs = parse_counter(soup.find("span", id="pull-requests-repo-tab-count"))
    except IndexError as e:
        # These two items should exist. We raise an error if we could not find them.
        raise IndexError(
            "Could not find issues or prs info. Maybe the GitHub page format has changed?"  # noqa: E501
        ) from e

    dependents_node = soup.find(string=used_by_regex)
    # verify that we didn't hit some random text in the page.
    # sometimes the dependents section isn't shown on the page either
    dependents_node_parent = getattr(dependents_node, "parent", None)
    dependents: int = 0
    if dependents_node_parent is not None and "href" in dependents_node_parent:  # noqa: SIM102
        if dependents_node_parent["href"].endswith("/network/dependents"):
            dependents = parse_counter(getattr(dependents_node, "next_element", None))

    # likewise, handle edge cases with contributors
    contributors_node = soup.find(string=contributors_regex)
    contributors_node_parent = getattr(contributors_node, "parent", None)
    contributors: int = 0
    if contributors_node_parent is not None and "href" in contributors_node_parent:  # noqa: SIM102
        if contributors_node_parent["href"].endswith("/graphs/contributors"):
            contributors = parse_counter(
                getattr(contributors_node, "next_element", None),
            )

    fetched_at = datetime.now(tz=timezone.utc)

    metrics = [
        {
            "open_issues": issues,
            "open_prs": prs,
            "dependents": dependents,
            "contributors": contributors,
            "fetched_at": fetched_at,
        }
    ]

    logger.debug(metrics)
    return metrics
