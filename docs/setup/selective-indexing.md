# Selective indexing

It's often useful to only pull down metadata about certain tables or schemas. This will improve search speed, reduce scraping time, and reduce load on your warehouse.

To this end, we support regex and/or where clause appending to our metadata extractors \(see warehouse-specific details below\). ****The listed keys can be simply added to the appropriate warehouse-specific section in your connections.yaml file, accessible through:

```text
wh connections
```

**Note:** For the `where_clause_suffix`-enabled warehouses, we follow the amundsen-databuilder pattern, for those familiar with it. For simplicity, we provide a list of metadata tables \(e.g. `information_schema`\) and their aliases, as well as a list of a few notable column names that are commonly used for filtering.

{% hint style="info" %}
Once you run a scraping job \(`wh pull`, manually or through cron\), we will never _delete_ any table stubs, to avoid removing any personal documentation, so if you want to remove filtered tables scraped before adding a filter, they'll have to be deleted manually.
{% endhint %}

## Bigquery: included\_tables\_regex

Because we access Bigquery through the google client API, we don't supply a `where_clause` like with the other warehouses. You can instead add the `included_tables_regex` with an associated regex expression. If the regex expression is matched, the table will be indexed.

```text
---
name: bq-1
metadata_source: Bigquery
key_path: /path/to/key.json
project_credentials: ~
project_id: your-project-name
included_tables_regex: ".*(?<!button_clicked)$"  # tables that don't end in "button_clicked" 
```

The table to be matched by this regex will follow the format: `project_id.dataset.table_name`, so dataset- level restrictions can also occur here.

## Hive Metastore: where\_clause\_suffix

```text
---
name: hm-1
metadata_source: HiveMetastore
...
where_clause_suffix: |
  where d.NAME = "schema1"
  and t.TABLE_NAME = "table1"
```

Available metadata tables:

* `TBLS` \(alias: `t`\)
* `DBS` \(alias: `d`\)
* `PARTITION_KEYS` \(alias: p\)
* `TABLE_PARAM` \(alias: `tp`\)

Notable fields:

* `d.NAME` \(table schema\)
* `t.TABLE_NAME`

## Postgres: where\_clause\_suffix

```text
---
name: pg1-1
metadata_source: Postgres
...
where_clause_suffix: |
  where c.table_schema = "schema1"
  and c.table_name = "table1"
```

Available metadata tables:

* `INFORMATION_SCHEMA.COLUMNS` \(alias: `c`\)
* `PG_CATALOG.PG_STATIO_ALL_TABLES` \(alias: `st`\)
* `PG_CATALOG.PG_DESCRIPTION` \(alias: `pgtd`\)

Notable fields:

* `c.table_schema`
* `c.table_name`

## Presto: where\_clause\_suffix

```text
---
name: pr-1
metadata_source: Presto
...
where_clause_suffix: | 
  where a.table_name = ''
  and a.table_schema = ''
```

Available metadata tables:

* `information_schema.columns` \(alias: `a`\)
* `information_schema.views` \(alias: `b`\)

Notable fields:

* `a.table_catalog`
* `a.table_schema`
* `a.table_name`

## Redshift

```text
---
name: rs-1
metadata_source: Redshift
...
where_clause_suffix: | 
  where schema = ''
  and name = ''
```

Available metadata tables: None \(all the constituent tables are combined into a larger table\)

Notable fields:

* `cluster`
* `schema`
* `name` \(table name\)

## Snowflake

```text
---
name: sf-1
metadata_source: Snowflake
...
where_clause_suffix: | 
  where c.TABLE_NAME = ''
  and c.TABLE_SCHEMA = ''
```

Available metadata tables:

* `TABLES` \(alias: `t`\)
* `COLUMNS` \(alias: `c`\)

Notable fields:

* `c.TABLE_NAME`
* `c.TABLE_SCHEMA`



