---
description: >-
  Whale also enables users to run queries from command-line, against both plain
  SQL files as well as within markdown files, enabling notebook-style behavior.
---

# Running SQL queries

{% hint style="info" %}
**Supported connections:** BigQuery, Postgres, Presto, Redshift, Snowflake
{% endhint %}

## Executing SQL files

Whale exposes a direct line into `SQLAlchemy` against connections defined in `~/.whale/config/connections.yaml` through the `wh run` command.

```text
wh run filename.sql -w warehouse_name
```

If the `-w` flag is not given, the first warehouse in your `connections.yaml` file will be used.

If there are multiple warehouses with the same `warehouse_name` the credentials from the _first_ matching `warehouse_name` encountered will be used.

**Note:** this _only_ works for \(a\) direct connections to warehouses \(not the Hive metastore\) and \(b\) connections where permissions allow for query runs.

### Jinja templating

`wh run` also supports Jinja2 templating -- for more information on how to set this up, see [Jinja2 templating](jinja2-templating.md).

### Saving results as comments

If you'd like to store your results automatically as comments within your `.sql` file, you can add `--!wh-run` to any SQL file \(on its own line\) and any `wh run` execution will automatically string replace this line with the results of the query execution.

For example, the following sql file

```text
select * from census.newtable limit 1

--!wh-run
```

would, upon execution using `wh run`, would be overwritten as:

```text
select * from census.newtable limit 1

/* results: 2021-01-12 14:42:24.318297
--------------------------------------
   customer_id       date1 test_field
0            1  2019-10-01       None
*/
```

## Running SQL blocks within markdown files

The `wh run` command can also be used against files with the `.md` or `.markdown` extension, to enable more advanced Jupyter-notebook style behavior within your IDE.

Any block \(\`\`\`sql\) containing **`--!wh-run`** will flag said block will:

1. Flag this block for execution when `wh run` is called on this file.
2. Upon execution, replace the line containing `--!wh-run` with a block of results.

This behavior is very similar to the execution within SQL files as shown above, but enables execution of SQL query blocks alongside context written in Markdown.

For instance, the following Markdown file

```text
# My project

I've found that we've had some problems with this census table!

```sql
select * from census.newtable limit 1

--!wh-run
```
```

would be overwritten upon execution of `wh run` as:

```text
# My project

I've found that we've had some problems with this census table!

```sql
select * from census.newtable limit 1

/* results: 2021-01-12 14:42:24.318297
--------------------------------------
   customer_id       date1 test_field
0            1  2019-10-01       None
*/
```
```

For those familiar with Jupyter or R markdown notebooks, this pattern might feel somewhat foreign. Unlike with full-fledged programming languages, there is no need for logic to be transferred between cells in SQL, so we've made the conscious decision to simplify the usage pattern and supporting code by not building out support for execution of individual code blocks except for through explicit inclusion of the `--!wh-run` statement. This has the added benefit of being minimally destructive, only replacing the `--!wh-run` character sequence.

### Editor configuration

#### Vim

Add the following to your `.vimrc` file, and you can run `<Leader>wh` to execute whale files. `<Leader>` is generally `\` or `,`.

```text
nmap <Leader>wh :w<CR>:exec '!~/.whale/bin/whale run %'<CR>:e<CR>
```

#### Vscode

TODO

