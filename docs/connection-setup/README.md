# Connection setup

**To use metaframe, you need to configure your server connection to enable ETL jobs to be run. Metaframe automatically creates some** [**amundsen databuilder**](https://github.com/lyft/amundsendatabuilder) **pipelines to enable this.** 

Currently, metaframe supports the following connections out-of-the-box:

* presto
* neo4j \(amundsen's format\)
* bigquery

If you need to connect any other sort of database, it's not difficult -- you just have to make your own build script. See [Custom ETL](custom-etl.md) for detailed instructions.

If you'd like us to build native support for your connection, drop an issue, and I'd be happy to oblige. \(Or feel free to make a contribution! It's not difficult!\)

## Setup

**To set up your connections, run `mf connections edit` to manually add entries.**

`mf connections edit` will give you direct access to the list of connections enabled for your metaframe installation, saved in `~/.metaframe/config/connections.yaml` \(you can therefore just edit this file directly, if you wish\).

This file accepts entries as yaml blocks. For example:

```text
- name: presto
  type: presto
  host: ec2-something.yourhost.com:8889
  username:
  password:
  catalog:
```

All fields are optional except for `host` and `type`. 

\*\*\*\*

## Other considerations

**If you have too much data, try using an `exclude` or `include` rule to explicitly exclude or include only certain schemas.**

For companies with very large numbers of tables \(&gt;100k\), it can take a prohibitively long time to run the ETL job and/or parse through so many files from the command line. However, it's generally unnecessary for consumers to index all of these tables. We therefore provide support to explicitly _exclude_ or _only include_ certain schemas for all of our connections. Moreover, for neo4j, we extend this behavior to databases, clusters, and also allow for key-based inclusion and exclusion using regular expressions.

See the sub-pages in this section for more details.

