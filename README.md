<p align="center"><img src="docs/whale_logo.svg" width="650"/></p>

![](https://github.com/rsyi/metaframe/workflows/CD/badge.svg) ![](https://github.com/rsyi/metaframe/workflows/CI/badge.svg) ![codecov](https://codecov.io/gh/rsyi/metaframe/branch/master/graph/badge.svg)

`whale` is a blazingly fast CLI **data discovery + documentation tool**. It periodically pulls table and column info from your warehouse and makes it locally searchable and documentable.

![](docs/demo.gif)

## Installation

### Mac OS

```text
brew install rsyi/tap/metaframe
```

### Building from source

Clone this directory, activate a &gt;= Python 3.6 virtual environment, then run the following in the base directory of the repo \(**make sure `./dist` does not exist, or pyinstaller won't rebuild**\):

```text
make && make install
```

We don't explicitly add an alias for the `whale` binary, so you'll want to add the following alias to your `.bash_profile` or `.zshrc` file.

```text
alias wh=~/.metaframe/bin/wh
```

## Getting started

### Initialize file structure

Start by running:

```text
wh init
```

which will generate a file structure in `~/.metaframe`.

If you want to manually document assets \(tables\), create a new stub by running:

```text
wh new <ASSET_NAME>
```

Then run `wh` to search over these assets. Generally, `<ASSET_NAME>` corresponds to a table name, which we suggest formatting as follows: `database_name/cluster.schema.table` to ensure `wh etl` will correctly associate your documentation with scraped table information, should you choose to automatically extract some table metadata. **But because metaframe is highly flexible in this regard, you also easily document SQL snippets, projects, machine learning models, metrics, etc.**

See the [Manual usage](docs/manual-usage.md) section for more information.

If you want to run ETL jobs to automatically populate this metadata, keep reading.

### Configure warehouse connections

If you want metadata to be scraped and populated automatically, you'll next need to add an entry to your `connections.yaml` file, which can be accessed by running `wh connections edit`. For example:

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
wh etl
```

By default this only pulls tables that haven't already been pulled. For more details, see [ETL](docs/running-an-etl-job.md).

### Go go go!

Run:

```text
wh
```

to search over all metadata. Hitting `enter` will open the editable part of the docs in your default text editor, defined by the environmental variable `$EDITOR`.

