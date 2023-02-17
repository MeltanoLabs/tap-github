"""Utility functions for scraping https://github.com

Inspired by https://github.com/dogsheep/github-to-sqlite/pull/70
"""
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Union, cast
from urllib.parse import urlparse

import requests
from bs4 import NavigableString, Tag

used_by_regex = re.compile(" Used by ")
contributors_regex = re.compile(" Contributors ")


def scrape_dependents(
    response: requests.Response, logger: Optional[logging.Logger] = None
) -> Iterable[Dict[str, Any]]:
    from bs4 import BeautifulSoup

    logger = logger or logging.getLogger("scraping")

    soup = BeautifulSoup(response.content, "html.parser")
    # Navigate through Package toggle if present
    base_url = urlparse(response.url).hostname or "github.com"
    options = soup.find_all("a", class_="select-menu-item")
    links = []
    if len(options) > 0:
        for link in options:
            links.append(link["href"])
    else:
        links.append(response.url)

    logger.debug(links)

    for link in links:
        yield from _scrape_dependents(f"https://{base_url}/{link}", logger)


def _scrape_dependents(url: str, logger: logging.Logger) -> Iterable[Dict[str, Any]]:
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
                "Could not find star and fork info. Maybe the GitHub page format has changed?"
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
                "a", string="Next"
            )[0]
        except IndexError:
            break
        if next_link is not None:
            href = next_link["href"]
            url = str(href if not isinstance(href, list) else href[0])
            time.sleep(1)
        else:
            url = ""


def parse_counter(
    tag: Union[Tag, NavigableString, None], logger: logging.Logger
) -> int:
    if not tag:
        return 0
    try:
        title = tag["title"]  # type: ignore
        if isinstance(title, str):
            title_string = cast(str, title)
        else:
            title_string = cast(str, title[0])
        return int(title_string.strip())
    except KeyError:
        raise IndexError(
            f"Could not parse counter {tag}. Maybe the GitHub page format has changed?"
        )


def scrape_metrics(
    response: requests.Response, logger: Optional[logging.Logger] = None
) -> Iterable[Dict[str, Any]]:
    from bs4 import BeautifulSoup

    logger = logger or logging.getLogger("scraping")

    soup = BeautifulSoup(response.content, "html.parser")

    try:
        issues = parse_counter(soup.find("span", id="issues-repo-tab-count"), logger)
        prs = parse_counter(
            soup.find("span", id="pull-requests-repo-tab-count"), logger
        )
    except IndexError:
        # These two items should exist. We raise an error if we could not find them.
        raise IndexError(
            "Could not find issues or prs info. Maybe the GitHub page format has changed?"
        )

    dependents = parse_counter(
        getattr(soup.find(string=used_by_regex), "next_element", None), logger
    )
    contributors = parse_counter(
        getattr(soup.find(string=contributors_regex), "next_element", None),
        logger,
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
