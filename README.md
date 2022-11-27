<font color="purple"><i>Whale is actively being built and maintained by <a href="https://hyperquery.ai/?utm_source=whale">hyperquery</a>. For our full data workspace for teams, check out <a href="https://hyperquery.ai/?utm_source=whale">hyperquery</a>.</i></font>

<p align="center"><img src="docs/whale_logo.svg" width="600"/></p>

## The simplest way to find tables, write queries, and take notes
`whale` is a lightweight, CLI-first **SQL workspace for your data warehouse**.

* Execute SQL in `.sql` files using `wh run`, or in sql blocks within `.md` files using the `--!wh-run` flag and `wh run`.
* Automatically index all of the tables in your warehouse as plain markdown files -- so they're easily versionable, searchable, and editable either locally or through a remote git server.
* Search for tables and documentation.
* Define and schedule basic metric calculations (in beta).

😁 [**Join the discussion on slack.**](http://slack.dataframe.ai/)

---

![](https://github.com/dataframehq/whale/workflows/CI/badge.svg)
![codecov](https://codecov.io/gh/dataframehq/whale/branch/master/graph/badge.svg)
[![slack](https://badgen.net/badge/icon/slack?icon=slack&color=purple&label)](http://slack.dataframe.ai/)

For a demo of a git-backed workflow, check out [**dataframehq/whale-bigquery-public-data**](https://github.com/dataframehq/whale-bigquery-public-data).

![](docs/demo.gif)

# 📔  Documentation

[**Read the docs for a full overview of whale's capabilities.**](https://rsyi.gitbook.io/whale)

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

For team use, see the [docs](https://rsyi.gitbook.io/whale/setup/getting-started-for-teams) for instructions on how to set up and point your whale installation at a remote git server.

### Seeding some sample data
If you just want to get a feel for how whale works, remove the `~/.whale` directory and follow the instructions at [dataframehq/whale-bigquery-public-data](https://github.com/dataframehq/whale-bigquery-public-data).

### Go go go!

Run:

```text
wh
```

to search over all metadata. Hitting `enter` will open the editable part of the docs in your default text editor, defined by the environmental variable `$EDITOR` (if no value is specified, whale will use the command `open`).

To execute `.sql` files, run:

```
wh run your_query.sql
```

To execute markdown files, you'll need to write the query in a  ```sql block, then place a `--!wh-run` on its own line. Upon execution of the markdown file, any sql blocks with this comment will execute the query and replace the `--!wh-run` line with the result set. To run the markdown file, run:

```
wh run your_markdown_file.md
```

A common pattern is to set up a shortcut in your IDE to execute `wh run %` for a smooth editing + execution workflow. For an example of how to do this in vim, see the docs [here](https://rsyi.gitbook.io/whale/features/running-sql-queries#editor-configuration). This is one of the most powerful features of whale, enabling you to take notes and write executable queries seamlessly side-by-side.
