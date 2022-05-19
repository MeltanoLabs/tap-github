"""Utility functions for scraping https://github.com

Inspired by https://github.com/dogsheep/github-to-sqlite/pull/70
"""
import requests
import time


def scrape_dependents(response: requests.Response, verbose=False):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(response.content, "html.parser")
    # Navigate through Package toggle if present
    options = soup.find_all("a", class_="select-menu-item")
    links = []
    if len(options) > 0:
        for link in options:
            links.append(link["href"])
    else:
        links.append(response.url)

    if verbose:
        print(links)

    for link in links:
        yield from _scrape_dependents(f"https://github.com/{link}", verbose=verbose)


def _scrape_dependents(url: str, verbose: bool = False) -> Iterable[dict[str, Any]]:
    # Optional dependency:
    from bs4 import BeautifulSoup

    while url:
        if verbose:
            print(url)
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        repo_names = [
            a["href"].lstrip("/")
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

        if verbose:
            print(repos)

        yield from repos

        # next page?
        try:
            next_link = soup.select(".paginate-container")[0].find("a", text="Next")
        except IndexError:
            break
        if next_link is not None:
            url = next_link["href"]
            time.sleep(1)
        else:
            url = None
