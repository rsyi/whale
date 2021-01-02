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
      count(*)
    from mart.user_signups
    where user_id is null
distinct-registrations:
  sql: |
    select
      count(distinct user_id)
    from mart.user_signups
    where user_id is not null
```

A full list of all historical values are saved in `~/.whale/metrics`.

## Slack alerts

Metrics can be enhanced with Slack alerts. These will send a message to you or your channel if a certain condition is met.

### Setup

To enable Slack alerts for your Slack workspace first add the 🐳 Slack app by clicking ****[**this link**](https://slack.com/oauth/v2/authorize?client_id=1407551924673.1505585912487&scope=chat:write,im:write&user_scope=).

Once you hit "Allow", you will be redirected back here. This time, however, an access token will be added to the URL parameters like so: https://docs.whale.cx/features/metrics\#setup?token=123. Store this token, 123 in the URL given before, as an environment variable called `WHALE_SLACK_TOKEN`. That's all!

### Syntax

The syntax is as follows:

```text
```metrics
metric-name:
  sql: |
    select statement
  alerts:
    - condition: "condition"
      message: "message"
      slack: 
        - "channel"
```

Using the earlier example we could set an alert every time we find a null in column `user_id` like this:

```text
```metrics
null-registrations:
  sql: |
    select
      count(*)
    from mart.user_signups
    where user_id is null
  alerts:
    - condition: "> 0"
      message: "Nulls found in column 'user_id' of mart.user_signups."
      slack:
        - "#data-monitoring"
        - "@bob"
```

As you can see, you can send a message on Slack to _individuals_ as well as Slack _channels_. In case you are interested, it's also possible to attach several conditions and messages to one metric.

All in all your `table.md` file with metrics and corresponding alerts could look like this:

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
      count(*)
    from mart.user_signups
    where user_id is null
  alerts:
    - condition: ">0"
      message: "Nulls found in column 'id' of mart.user_signups."
      slack:
        - "#data-monitoring"
        - "@bob"
    - condition: "> 100"
      message: "More than 100 nulls found in column 'id' of mart.user_signups."
      slack:
        - "#incident-room"
        - "@joseph"

distinct-registrations:
  sql: |
    select
      count(distinct user_id)
    from mart.user_signups
    where user_id is not null
  alerts:
    - condition: "<10"
      message: "Less than 10 users in mart.user_signups."
      slack:
        - "#data-monitoring"
        - "@bob"
```

