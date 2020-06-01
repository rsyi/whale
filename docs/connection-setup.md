# Connection setup

To use metaframe, you need to configure your server connection to enable ETL jobs to be run. Metaframe automatically creates some [amundsen databuilder](https://github.com/lyft/amundsendatabuilder) pipelines to enable this. 

Currently, metaframe supports the following connections out-of-the-box:

* presto
* neo4j \(amundsen's format\) _\[WIP\]_

If you need to connect any other sort of database, it's not difficult -- you just have to make your own amundsen build script. See [Custom ETL](custom-etl.md) for detailed instructions.

If you'd like us to build native support for your connection, drop an issue, and I'd be happy to oblige. \(Or feel free to make a contribution! It's not difficult!\)

## Setup

There are **two** ways to add a new connection:

**Option 1: Run `mf connections edit` to manually add entries**

`mf connections edit` will give you direct access to the list of connections enabled for your metaframe installation, saved in `~/.metaframe/config/connections.yaml` \(you can therefore just edit this file directly, if you wish\).

This file accepts entries as yaml blocks. For example:

```text
- name: presto
  type: presto
  host: ec2-something.yourhost.com:8889
  username:
  password:
  cluster:
```

All fields are optional except for `host` and `type`.

#### **Option 1: Run `mf connections add`, with keyword arguments.**

If you prefer, you can use `mf connections add` with flags to write to the `connections.yaml` file instead. Use the following flags to specify database properties.

```text
--name      # A unique name to identify your connection.
--type      # The "type" of connection, e.g. presto.
--host      # The host:port address.
--username  # [Optional]
--password  # [Optional]
--cluster   # [Optional] 
```

For example:

```text
mf connections add \
    --name presto \
    --type presto \
    --host ec2-1-2-3-4-5.us-west-2.compute.amazonaws.com:8889 \
    --username admin \
    --password bungabungabunga
```

## Type-specific setup

### Neo4j

The `host` value should be the neo4j graph endpoint \(e.g. `localhost:7687`\), without `bolt://`.

If you are using amundsen with kubernetes, you can port-forward the neo4j instance with the following command, then run `mf etl` to extract data:

```text
k port-forward pod-name-@@@@@-@@@@@ 7474:7474 7687:7687
```

