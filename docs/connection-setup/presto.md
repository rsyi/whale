# Presto

## Supported flags

We support the following flags to be added to `connections.yaml` for presto connections.

* **`name`** Name to be used in naming the sub-folder for this connection. Files will be searchable as `name/cluster.schema.table`.
* **`host`** Host URI, in `uri:port` format, e.g. `ec2.us-west-2.1.23.4.5:8889`.
* **`username`** \[Optional\] Username to connect.
* **`password`** \[Optional\] Password to connect.
* **`catalog`** \[Optional\] Specify the catalog that you want to source metadata from. Only necessary/useful if your presto connection is connected to more than one database, or if you want to extract metadata from other system-level catalogs.
* **`include_schemas`** \[Optional\] When this field is specified, `mf etl` will _only_ extract data from the schemas in this list.
* **`exclude_schemas`** \[Optional\] When this field is specified, `mf etl` will not extract any schemas in this list.

