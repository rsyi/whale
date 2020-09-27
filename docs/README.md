# Getting started

## Installation

### Mac OS

```text
brew install rsyi/tap/whale
```

### Compile and install

Clone the whale repository, activate a Python &gt;= 3.6 virtual environment, then run the following in the base directory of the repo _\(if you are updating, make sure to delete `./dist` or `pyinstaller` won't rebuild the binary\)_:

```text
make && make install
```

The Makefile commands don't explicitly add an alias for the `whale` binary, so you'll want to add the following alias to your `.bash_profile` or `.zshrc` file.

```text
alias wh=~/.whale/bin/wh
```

## Quick start

To walk through the setup process, run the following in your terminal:

```text
wh init
```

This will assist you in:

* Setting up the necessary file structure in `~/.whale`.
* Setting up your warehouse connection credentials.
* Registering a cron job in your `crontab`, so whale can periodically scrape metadata.

Once that is complete, either wait for the cron job to run, or run `wh etl &` to manually kick off a job in the background, if you're feeling impatient.

## Usage

To obtain a list of available commands, run:

```text
wh -h
```

To run whale's search engine, run:

```text
wh
```

