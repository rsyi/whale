# Connection configuration

We provide access to warehouse configuration through the `~/.whale/config/connections.yaml` file. The accepted key/value pairs, however, are warehouse-specific and, as such, are most easily added through the `wh init` workflow. However, in the case where this needs to be done manually, refer to the following warehouse-specific documentation below.

## Universal connection parameters

```text
---
name: bq-1
metadata_source: ~

```

* **name** Unique warehouse name. This will be used to name the subdirectory within `~/.whale/metadata` that stores metadata and UGC for each table.
* **metadata\_source** The type of connection that this yaml section describes. These are case sensitive and can be one of the following:
  * Bigquery
  * Neo4j
  * Presto
  * Snowflake

## Bigquery

```text
---
name: 
metadata_source: Bigquery
key_path: /Users/robert/gcp-credentials.json
project_credentials:  # Only one of key_path or project_credentials needed
project_id:
```

Only one of `key_path` and `project_credentials` are required.

## Neo4j

We provide support to scrape metadata from Amundsen's neo4j backend.

```text
---
name: 
metadata_source: Neo4j
uri:
port:
username:  # Optional
password:  # Optional
```

## Presto

```text
---
name: 
metadata_source: Presto
uri:
port:
username:  # Optional
password:  # Optional
```

## Snowflake

```text
---
name:
metadata_source: Snowflake
uri:
port:
username:  # Optional
password:  # Optional
```

## Build script

We also support use of custom scripts that handle the metadata scraping and dumping of this data into local files \(in the `metadata` subdirectory\) and manifests \(in the `manifests` subdirectory\). For more information, see [Custom extraction](custom-extraction.md).

```text
---
build_script_path: /path/to/build_script.py
venv_path: /path/to/venv
python_binary_path: /path/to/binary  # Optional

```

