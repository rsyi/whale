# Connection configuration

We provide access to warehouse configuration through the `~/.whale/config/connections.yaml` file. The accepted key/value pairs, however, are warehouse-specific and, as such, should preferably be added through the `wh init` workflow. However, in the case where this needs to be done manually, refer to the following warehouse-specific documentation below.

## Universal connection parameters

```text
---
name: bq-1
 

```

* **name** Unique warehouse name. This will be used to name the subdirectory within `~/.whale/metadata` that stores metadata and UGC for each table.
* **metadata\_source** The type of connection that this yaml section describes. See warehouse-specific implementation details for valid values for this field.

## Bigquery

```text
---
name: bq1
metadata_source: bigquery
key_path: /Users/robert/gcp-credentials.json
project_credentials: ~
project_id: bigquery-db-10394103
```

Only one of `key_path` and `project_credentials` are required.



