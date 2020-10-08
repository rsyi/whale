# Getting started

### Installation

### Mac OS

```text
brew install rsyi/tap/whale
```

### All others

To manually compile and install, you'll have to \(a\) clone the whale repository, \(b\) activate a Python &gt;= 3.6 virtual environment, and \(c\) run **`make && make install`** in the base directory of the repo. These steps are encapsulated in the following commands:

```text
git clone https://github.com/rsyi/whale.git
cd whale/ && python3 -m venv env && source env/bin/activate
make && make install
```

If this does not work, make sure `python3` is aliased to a &gt;=3.6 python installation.

The Makefile commands don't explicitly add an alias for the `whale` binary, so you'll want to add the following alias to your `.bash_profile` or `.zshrc` file.

```text
alias wh=~/.whale/bin/wh
```

{% hint style="info" %}
_**Developer's note:** if you are developing and want to recompile, make sure to delete `./dist` on each rebuild or `pyinstaller` won't rebuild the python binary._
{% endhint %}

### Advanced syntax highlighting

We highly recommend installing [`bat`](https://github.com/sharkdp/bat) to enable advanced syntax highlighting \(once detected, whale will use `bat` over `cat` automatically\).

## Quick start

To walk through the setup process, run the following in your terminal **\(this can be run again in the future if you need to add more warehouses, so feel free to quit part-way\)**:

```text
wh init
```

This will assist you in:

* Setting up the necessary file structure in `~/.whale`.
* Setting up your warehouse connection credentials.
* Registering a cron job in your `crontab`, so whale can periodically scrape metadata.

Once that is complete, either wait for the cron job to run, or run `wh pull &` to manually kick off a job in the background, if you're feeling impatient.

## Usage

To obtain a list of available commands, run:

```text
wh -h
```

To run whale's search engine, run:

```text
wh
```

