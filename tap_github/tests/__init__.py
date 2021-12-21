"""Test suite for tap-github."""
import requests
import requests_cache

# Setup caching for all api calls done through `requests` in order to limit
# rate limiting problems with github.
# Use the sqlite backend as it's the default option and seems to be best supported.
# To clear the cache, just delete the sqlite db file at api_calls_tests_cache.sqlite
# in the root of this repository
requests_cache.install_cache(
    "api_calls_tests_cache",
    backend="sqlite",
    # make sure that API keys don't end up being cached
    ignored_parameters=["Authorization"],
    # tell requests_cache to check headers for the above parameter
    match_headers=True,
    # expire the cache after 24h (86400 seconds)
    expire_after=24 * 60 * 60,
)
