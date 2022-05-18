"""Utility functions for scraping https://github.com

Inspired by https://github.com/dogsheep/github-to-sqlite/pull/70
"""
import requests
import time


def scrape_dependents(repo, verbose=False):
    from bs4 import BeautifulSoup

    url = "https://github.com/{}/network/dependents".format(repo)
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    # Navigate through Package toggle if present
    options = soup.find_all("a", class_="select-menu-item")
    links = []
    if len(options) > 0:
        for link in options:
            links.append(link["href"])
    else:
        links.append(f"{repo}/network/dependents")

    if verbose:
        print(links)

    for link in links:
        yield from _scrape_dependents(f"https://github.com/{link}", verbose=verbose)


def _scrape_dependents(url, verbose=False):
    # Optional dependency:
    from bs4 import BeautifulSoup

    while url:
        if verbose:
            print(url)
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        repos = [
            a["href"].lstrip("/")
            for a in soup.select("a[data-hovercard-type=repository]")
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
