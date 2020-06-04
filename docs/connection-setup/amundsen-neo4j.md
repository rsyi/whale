---
description: Amundsen neo4j-specific instructions
---

# Amundsen neo4j

We provide support for users who have already set up a neo4j instance using amundsen. Because neo4j is exceedingly fast, this can provide users with a quick way to get started with metaframe, while also giving users access within metaframe to all documentation that has been upserted from the front-end.

## Setup

Setup is largely the same as other data warehouses. The `host` value should be the neo4j graph endpoint \(e.g. `localhost:7687`\), without `bolt://`.

If you are using amundsen with kubernetes, you can port-forward the neo4j instance with the following command, then run `mf etl` to extract data:

```text
k port-forward pod-name-@@@@@-@@@@@ 7474:7474 7687:7687
```

## Neo4j-specific flags

We provide several neo4j-specific configuration flags that you can add to your `connections.yaml` entry to customize your experience. 

* **`included_keys`** This field must be provided as a list \(e.g. \[`presto://default.default`\]\). When this field is provided,  it specifies the only keys that are to be included in metaframe. For each table, metaframe requires that at least one of either the database, schema, cluster, or table keys match the keys in this list. All other tables will not be indexed.
* **`excluded_keys`** Like `included_keys`, `excluded_keys` is a list of keys, but these will be excluded. For each table, if any of the database, schema, cluster, or table keys matches any key in this list, it will not be indexed.
* **`included_key_regex`**

  A regular expression that specifies keys that you want to include. For each table, at least one of either the database, schema, cluster or table keys must match this regex expression, or it will not be included.

* **`excluded_key_regex`**

  A regular expression that specifies keys that you want to exclude. For each table, if any of the database, schema, cluster or table keys matches this regular expression, it will not be indexed.

The inclusion/exclusion yaml keys are intended to restrict which datasets are being indexed to ensure that metaframe always returns salient results. A common use case, therefore, is to restrict tmp tables or artifact tables \(such as those generated by Looker PDT jobs\) from being indexed.

### Example

Below is an example of how to prevent looker PDT artifact tables from being indexed. They generally follow the form `hive.looker_lc1349019hu0eo9h`, so we use a regular expression to exclude these. 

```text
- type: neo4j 
  excluded_key_regex: 'presto://hive.looker_l.*'
```
