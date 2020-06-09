![](docs/.gitbook/assets/image%20%2810%29.png)

![](https://github.com/rsyi/metaframe/workflows/CD/badge.svg) ![](https://github.com/rsyi/metaframe/workflows/CI/badge.svg)

_**Disclaimer:** This project is still in alpha, so there will be bugs. Use at your own risk! But if you find bugs or have feature requests, open an issue :\)_

`metaframe` is a CLI **data documentation tool \(+catalog\)**. It leverages [junegunn/fzf](https://github.com/junegunn/fzf) and [lyft/amundsen](https://github.com/lyft/amundsen) to create a blazingly fast CLI framework to:

* Easily document your tables, using an organizational structure where tables are first-class citizens.
* Run ETL jobs from the command-line \(or manually document your datasets\).
* Search through your tables.

![](https://raw.githubusercontent.com/rsyi/metaframe/master/docs/demo.gif)

## Installation

### Mac OS

```text
brew install rsyi/tap/metaframe
```

### All others

If not on macOS, clone this directory, then run the following in the base directory of the repo \(**make sure `./dist` does not exist, or pyinstaller won't rebuild**\):

```text
make && make install
```

If there are errors, it's often because the specific flavor of python referenced by `pip3` on your machine is incompatible \(metaframe is tested against python 3.7 and 3.8 only\). To troubleshoot this, try using a virtual environment in 3.7 or 3.8 or modifying the makefile `pip3` reference to specific binary paths in your filesystem. **Or open an issue!**

We don't explicitly add an alias for the `mf` binary, so you'll want to either add `~/.metaframe/bin/` to your `PATH`, or add the following alias to your `.bash_profile` or `.zshrc` file.

```text
alias mf=~/.metaframe/bin/mf
```

## Getting started

### Initialize file structure

Start by running:

```text
mf init
```

which will generate a file structure in `~/.metaframe`.

If you want to manually document tables, create a new table stub by running:

```text
mf new <TABLE_NAME>
```

Then run `mf` to search over these docs! See the [Manual usage](docs/manual-usage.md) section for more information.

If you want to run ETL jobs to automatically populate this metadata, keep reading.

### Configure warehouse connections

If you want metadata to be scraped and populated automatically, you'll next need to add an entry to your `connections.yaml` file, which can be accessed by running `mf connections edit`. For example:

```text
- name: presto                # optional
  type: presto
  host: host.mysite.com:8889
  username:                   # optional
  password:                   # optional
  cluster: system             # optional
```

The only necessary arguments are the `host` and the `type`. See [Connection setup](docs/connection-setup/) for more details \(including information on type-specific syntax\).

### Run your ETL job

Once this configuration is complete, you can run your ETL job by running:

```text
mf etl
```

By default this only pulls tables that haven't already been pulled. For more details, see [ETL](docs/running-an-etl-job.md).

### Go go go!

Run:

```text
mf
```

to search over all metadata. Hitting `enter` will open the editable part of the docs in your default text editor, defined by the environmental variable `$EDITOR`.

