# Running SQL queries

Whale exposes a direct line into `SQLAlchemy` against connections defined in `~/.whale/config/connections.yaml` through the `wh run` command.

```text
wh run filename.sql -w warehouse_name
```

If the `-w` flag is not given, the first warehouse in your `connections.yaml` file will be used.

If there are multiple warehouses with the same `warehouse_name` the credentials from the _first_ matching `warehouse_name` encountered will be used.

**Note:** this _only_ works for \(a\) direct connections to warehouses \(not the Hive metastore\) and \(b\) connections where permissions allow for query runs.


