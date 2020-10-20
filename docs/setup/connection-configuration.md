# Connection configuration

We provide access to warehouse configuration through the `~/.whale/config/connections.yaml` file. The accepted key/value pairs, however, are warehouse-specific and, as such, are most easily added through the `wh init` workflow. However, in the case where this needs to be done manually, refer to the following warehouse-specific documentation below.

## Universal connection parameters

```text
---
name: ~
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

## Hive metastore

```text
---
name: 
metadata_source: HiveMetastore
uri:
port:
username:  # Optional
password:  # Optional
dialect: postgres  # postgres/mysql/etc. This is the dialect used in the SQLAlchemy conn string.
database: hive  # The database within this connection where the metastore lives. This is usually "hive".  
```

For more information the `dialect` field, see the [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/13/core/engines.html).

## Neo4j

We provide support to scrape metadata from Amundsen's neo4j backend. However, by default we do not install the neo4j drivers within our installation virtual environment. To use this, you must install using `make && make install`, then `pip install neo4j-driver` within the virtual environment located at `~/.whale/libexec/env`.

```text
---
name: 
metadata_source: Neo4j
uri:
port:
username:  # Optional
password:  # Optional
```

## Postgres

```text
---
name: 
metadata_source: Postgres
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

## Redshift

```text
---
name: 
metadata_source: Redshift
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

We also support use of custom scripts that handle the metadata scraping and dumping of this data into local files \(in the `metadata` subdirectory\) and manifests \(in the `manifests` subdirectory\). For more information, see [Custom extraction](../for-developers/custom-extraction.md).

```text
---
build_script_path: /path/to/build_script.py
venv_path: /path/to/venv
python_binary_path: /path/to/binary  # Optional

```

