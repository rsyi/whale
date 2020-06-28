# Snowflake

## Supported flags

We support the following flags to be added to `connections.yaml` for snowflake connections.

* **`name`** Name to be used in naming the sub-folder for this connection. Files will be searchable as `name/cluster.schema.table`.
* **`host`** Host name, without the snowflakecomputing.com address. For example, if your full address is `jku301.snowflakecomputing.com`, you should set `host` to `jku301`.
* **`username`** \[Optional\] Username to connect.
* **`password`** \[Optional\] Password to connect.
* **`catalog`** \[Optional\] Specify the catalog that you want to source metadata from.

