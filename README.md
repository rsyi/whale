<p align="center"><img src="docs/whale_logo.svg" width="600"/></p>

![](https://github.com/dataframehq/whale/workflows/CD/badge.svg) ![](https://github.com/dataframehq/whale/workflows/CI/badge.svg) ![codecov](https://codecov.io/gh/dataframehq/whale/branch/master/graph/badge.svg)

`whale` is a blazingly fast CLI **data warehouse command-line explorer and documentation tool**. It periodically pulls table and column info from your warehouse and makes it locally searchable and editable.

![](docs/demo.gif)

## Installation

### Mac OS

```text
brew install dataframehq/tap/whale
```

### Building from source

Clone this directory, activate a &gt;= Python 3.6 virtual environment, then run the following in the base directory of the repo :

```text
make && make install
```
If you are running this multiple times, make sure `./dist` does not exist, or pyinstaller won't rebuild. We don't explicitly add an alias for the `whale` binary, so you'll want to add the following alias to your `.bash_profile` or `.zshrc` file.

```text
alias wh=~/.whale/bin/wh
```

## Getting started

### Setup

Run the following command to go through the onboarding process. It will (a) set up all necessary files in `~/.whale`, (b) walk you through cron job scheduling to periodically scrape metadata, and (c) set up a warehouse:

```text
wh init
```

The cron job will run as you schedule it (by default, every 6 hours). If you're feeling impatient, you can also manually run `wh etl` to pull down the latest data from your warehouse.

### Go go go!

Run:

```text
wh
```

to search over all metadata. Hitting `enter` will open the editable part of the docs in your default text editor, defined by the environmental variable `$EDITOR` (if no value is specified, whale will use the command `open`).
