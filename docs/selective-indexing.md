# Selective indexing

When working in a small team \(or if you have a ton of cruft tables in your warehouse\), it's often prudent to only pull down metadata about certain tables or schemas -- this will not only make your searches easier, but also reduce scraping time, reducing load on your warehouse.

To this end, we support regex and/or where clause appending to our metadata extractors. See the warehouse-specific sections below for detailed instructions. In general, all off the keys detailed can be simply added to the appropriate warehouse-specific section in your connections.yaml file, accessible through:

{% hint style="info" %}
Once you run a scraping job \(`wh pull`, manually or through cron\), we will never _delete_ any table stubs, to avoid removing any personal documentation, so if you want to remove these files, they'll have to be deleted manually.
{% endhint %}

```text
wh connections
```

## Bigquery

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

