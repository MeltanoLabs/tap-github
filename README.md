# tap-github

`tap-github` is a Singer tap for GitHub.

Build with the [Singer SDK](https://gitlab.com/meltano/singer-sdk).

## Installation

```bash
pipx install git+https://github.com/MeltanoLabs/tap-github.git
```

Or better yet, please pin to a [release version](https://github.com/MeltanoLabs/tap-github/releases) for a stable experience:

```bash
pipx install git+https://github.com/MeltanoLabs/tap-github.git@vX.Y.Z
```

A list of release versions is available at https://github.com/MeltanoLabs/tap-github/releases

## Configuration

### Accepted Config Options

This tap accepts the following configuration options:

- Required: One and only one of the following modes:
  1. `repositories`: an array of strings specifying the GitHub repositories to be included. Each element of the array should be of the form `<org>/<repository>`, e.g. `MeltanoLabs/tap-github`.
  2. `organizations`: an array of strings containing the github organizations to be included
  3. `searches`: an array of search descriptor objects with the following properties:
     - `name`: a human readable name for the search query
     - `query`: a github search string (generally the same as would come after `?q=` in the URL)
  4. `user_usernames`: a list of github usernames
  5. `user_ids`: a list of github user ids [int]
- Highly recommended:
  - `auth_token` - GitHub token to authenticate with.
  - `additional_auth_tokens` - List of GitHub tokens to authenticate with. Streams will loop through them when hitting rate limits..
  - alternatively, you can input authentication tokens with any environment variables starting with GITHUB_TOKEN.
  - or authenticate as a GitHub app setting a private key in GITHUB_APP_PRIVATE_KEY. Formatted as follows: `:app_id:;;-----BEGIN RSA PRIVATE KEY-----\n_YOUR_P_KEY_\n-----END RSA PRIVATE KEY-----`. You can generate it from the `Private keys` section on https://github.com/organizations/:organization_name/settings/apps/:app_name. Read more about GitHub App quotas [here](https://docs.github.com/en/enterprise-server@3.3/developers/apps/building-github-apps/rate-limits-for-github-apps#server-to-server-requests).
- Optional:
  - `user_agent`
  - `start_date`
  - `metrics_log_level`
  - `stream_maps`
  - `stream_maps_config`
  - `rate_limit_buffer` - A buffer to avoid consuming all query points for the auth_token at hand. Defaults to 1000.",

Note that modes 1-3 are `repository` modes and 4-5 are `user` modes and will not run the same set of streams.

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-github --about
```

### Source Authentication and Authorization

A small number of records may be pulled without an auth token. However, a Github auth token should generally be considered "required" since it gives more realistic rate limits. (See GitHub API docs for more info.)

## Usage

### API Limitation - Pagination

The GitHub API is limited for some resources such as `/events`. For some resources, users might encounter the following error:

```
In order to keep the API fast for everyone, pagination is limited for this resource. Check the rel=last link relation in the Link response header to see how far back you can traverse.
```

To avoid this, the GitHub streams will exit early. I.e. when there are no more `next page` available. If you are fecthing `/events` at the repository level, beware of letting the tap disabled for longer than a few days or you will have gaps in your data.

You can easily run `tap-github` by itself or in a pipeline using [Meltano](www.meltano.com).

### Notes regarding permissions

* For the `traffic_*` streams, [you will need write access to the repository](https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28). You can enable extraction for these streams by [selecting them in the catalog](https://hub.meltano.com/singer/spec/#metadata).

### Executing the Tap Directly

```bash
tap-github --version
tap-github --help
tap-github --config CONFIG --discover > ./catalog.json
```

## Contributing
This project uses parent-child streams. Learn more about them [here.](https://gitlab.com/meltano/sdk/-/blob/main/docs/parent_streams.md)

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_github/tests` subfolder and
then run:

```bash
poetry run pytest
```

You can also test the `tap-github` CLI interface directly using `poetry run`:

```bash
poetry run tap-github --help
```

### Testing with [Meltano](meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-github
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-github --version
# OR run a test `elt` pipeline:
meltano elt tap-github target-jsonl
```

One-liner to recreate output directory, run elt, and write out state file:

```bash
# Update this when you want a fresh state file:
TESTJOB=testjob1

# Run everything in one line
mkdir -p .output && meltano elt tap-github target-jsonl --job_id $TESTJOB && meltano elt tap-github target-jsonl --job_id $TESTJOB --dump=state > .output/state.json
```

### Singer SDK Dev Guide

See the [dev guide](../../docs/dev_guide.md) for more instructions on how to use the Singer SDK to
develop your own taps and targets.
