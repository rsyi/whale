# Presto

## Presto-specific flags

We support the following flags to be added to `connections.yaml` for presto connections.

* **`cluster`** Specify the cluster that you want to source metadata from. Only necessary/useful if your presto connection is connected to more than one database, or if you want to extract metadata from other system-level catalogs.
* **`include_schemas`** When this field is specified, `mf etl` will _only_ extract data from the schemas in this list.
* **`exclude_schemas`** When this field is specified, `mf etl` will not extract any schemas in this list.

