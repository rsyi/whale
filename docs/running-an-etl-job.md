# ETL

Once your connection is set up, you can trigger a full ETL job by running:

```text
mf etl
```

For most databases, this can take quite some time \(often several hours\). 

By default, `mf etl` only updates metadata for tables that have not already been pulled \(i.e. we use the `~/.metaframe/metadata` directory as a cache\). To disable this and re-run a full ETL job, add the `--no-cache` flag:

```text
mf etl --no-cache
```



If you're using a [Custom ETL](custom-etl.md) job, you simply need to run that job in your local environment.

