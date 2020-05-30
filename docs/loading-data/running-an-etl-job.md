# Running an ETL job

Once your connection is set up, you can trigger a full ETL job by running:

```text
mf etl
```

For most databases, this can take quite some time \(~1 hour\). We set this as a `nohup` job, so no need to wait and/or keep the terminal window open. If the job is taking exceedingly long, check the logs in `~/.metaframe/logs`.

If you're using a [Custom ETL](custom-etl.md) job, you simply need to run that job in your local environment.

