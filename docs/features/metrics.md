# Metrics

{% hint style="info" %}
**Supported connections:** BigQuery, Postgres, Presto, Redshift, Snowflake
{% endhint %}

Whale supports automatic barebones metric definition and scheduled calculation. Metrics are defined by creating a ```````metrics```` block, as explained below. Any metric defined in this way will automatically be scheduled alongside the metadata scraping job. Metric definitions support Jinja2 templating -- for more information on how to set this up, see [Jinja2 templating](jinja2-templating.md).

## Basic usage

A metric is simply a named SQL statement that **returns a single value**, defined in plain yaml in a table stub, as shown below:

```text
```metrics
metric-name:
  sql: |
    select statement
```

```text
For example, below two metrics, `null-registrations` and `distinct-registrations` are defined:

```text
```metrics
null-registrations:
  sql: |
    select
      count(distinct user_id)
    from mart.user_signups
    where user_id is null
distinct-registrations:
  sql: |
    select
      count(distinct user_id)
    from mart.user_signups
    where user_id is not null
```

```text
The same block is shown within the context of a full table stub, below:

```text
schema.table

## Column details

## Partition info

------------------------------------------------------
*Do not make edits above this line.*

```metrics
null-registrations:
  sql: |
    select
      count(distinct user_id)
    from mart.user_signups
    where user_id is null
distinct-registrations:
  sql: |
    select
      count(distinct user_id)
    from mart.user_signups
    where user_id is not null
```

```text
These metrics will be scheduled, with the latest calculations injected into the programmatic portion of the table stub. An example is shown below:

```text
schema.table

## Column details

## Partition info

## Metrics
null-registrations: 103 @ 2020-04-01 05:12:15
distinct-registrations: 30104 @ 2020-04-01 05:12:18

------------------------------------------------------
*Do not make edits above this line.*

```metrics
null-registrations:
  sql: |
    select
      count(distinct user_id)
    from mart.user_signups
    where user_id is null
distinct-registrations:
  sql: |
    select
      count(distinct user_id)
    from mart.user_signups
    where user_id is not null
```

\`\`\`

A full list of all historical values are saved in `~/.whale/metrics`.

