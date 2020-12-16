<p align="center"><img src="docs/whale_logo.svg" width="600"/></p>

## The simplest way to keep track of your warehouse tables
`whale` is a lightweight **data discovery, documentation, and quality engine for your data warehouse**.

* Automatically index all of the tables in your warehouse as plain markdown files -- so they're easily versionable, searchable, and editable either locally or through a remote git server.
* Search for tables and documentation through the CLI or through a git remote server like Github.
* Define and schedule basic metric calculations (in beta).
* Run quality tests and systematically monitor anomalies (in roadmap).

😁 [**Join the discussion on slack.**](https://join.slack.com/t/df-whale/shared_invite/zt-i2rayu1u-fljCh7reVstTBOtaH1n1xA)

---

![](https://github.com/dataframehq/whale/workflows/CI/badge.svg)
![codecov](https://codecov.io/gh/dataframehq/whale/branch/master/graph/badge.svg)
[![slack](https://badgen.net/badge/icon/slack?icon=slack&color=purple&label)](https://join.slack.com/t/talk-whale/shared_invite/zt-i2rayu1u-fljCh7reVstTBOtaH1n1xA)

For a live demo, check out [**dataframehq/whale-bigquery-public-data**](https://github.com/dataframehq/whale-bigquery-public-data).

![](docs/demo.gif)

# 📔  Documentation

[**Read the docs for a full overview of whale's capabilities.**](https://docs.whale.cx)

## Installation

### Mac OS

```text
brew install dataframehq/tap/whale
```

### All others

Make sure [rust](https://www.rust-lang.org/tools/install) is installed on your local system. Then, clone this directory and run the following in the base directory of the repo:

```text
make && make install
```
If you are running this multiple times, make sure `~/.whale/libexec` does not exist, or your virtual environment may not rebuild. We don't explicitly add an alias for the `whale` binary, so you'll want to add the following alias to your `.bash_profile` or `.zshrc` file.

```text
alias wh=~/.whale/bin/whale
```

## Getting started

### Setup

For individual use, run the following command to go through the onboarding process. It will (a) set up all necessary files in `~/.whale`, (b) walk you through cron job scheduling to periodically scrape metadata, and (c) set up a warehouse:

```text
wh init
```

The cron job will run as you schedule it (by default, every 6 hours). If you're feeling impatient, you can also manually run `wh etl` to pull down the latest data from your warehouse.

For team use, see the [docs](https://docs.whale.cx/setup/getting-started-for-teams) for instructions on how to set up and point your whale installation at a remote git server.

### Seeding some sample data
If you just want to get a feel for how whale works, remove the `~/.whale` directory and follow the instructions at [dataframehq/whale-bigquery-public-data](https://github.com/dataframehq/whale-bigquery-public-data).

### Go go go!

Run:

```text
wh
```

to search over all metadata. Hitting `enter` will open the editable part of the docs in your default text editor, defined by the environmental variable `$EDITOR` (if no value is specified, whale will use the command `open`).
