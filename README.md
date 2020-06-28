# Metaframe

![](docs/.gitbook/assets/image%20%2810%29%20%281%29%20%281%29.png)

![](https://github.com/rsyi/metaframe/workflows/CD/badge.svg) ![](https://github.com/rsyi/metaframe/workflows/CI/badge.svg) ![codecov](https://codecov.io/gh/rsyi/metaframe/branch/master/graph/badge.svg)

`metaframe` is a blazingly fast CLI **data discovery + documentation tool**, built using [fzf](https://github.com/junegunn/fzf) and [amundsen](https://github.com/lyft/amundsen). It allows you to:

* Collect all table + column information from data warehouses with **`mf etl`**.
* Search for this info with **`mf`**.
* Write documentation that sits next to these table stubs.

Metaframe is built primarily for users who want to write documentation on their tables, but we've found that it can also be useful for: organizations with &lt; 100k tables and want to stand up a git-based data catalog, or hacker data scientists at larger companies who live and run queries in the terminal, to enable more streamlined workflows.

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

If there are errors, it's often because the specific flavor of python referenced by `pip3` on your machine is incompatible \(metaframe requires python &gt;= 3.6\). To troubleshoot this, try using a virtual environment in &gt;=3.6 or modifying the makefile `pip3` reference to specific binary paths in your filesystem. **Or open an issue.**

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

If you want to manually document assets \(tables\), create a new stub by running:

```text
mf new <ASSET_NAME>
```

Then run `mf` to search over these assets. Generally, `<ASSET_NAME>` corresponds to a table name, which we suggest formatting as follows: `database_name/cluster.schema.table` to ensure `mf etl` will correctly associate your documentation with scraped table information, should you choose to automatically extract some table metadata. **But because metaframe is highly flexible in this regard, you also easily document SQL snippets, projects, machine learning models, metrics, etc.** 

See the [Manual usage](docs/manual-usage.md) section for more information.

If you want to run ETL jobs to automatically populate this metadata, keep reading.

### Configure warehouse connections

If you want metadata to be scraped and populated automatically, you'll next need to add an entry to your `connections.yaml` file, which can be accessed by running `mf connections edit`. For example:

```text
- name: presto                # optional
  type: presto
  host: host.mysite.com:8889
  username:                   # optional
  password:                   # optional
  catalog: system             # optional
```

The only necessary arguments is `type` \(and `host`, in general\). See [Connection setup](docs/connection-setup/) for more details + information on `type`-specific syntax.

### Run your ETL job

Once this configuration is complete, you can pull down all table info by running:

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

