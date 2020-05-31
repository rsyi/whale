# Connection setup

To use metaframe, you need to configure your server connection to enable ETL jobs to be run. Metaframe automatically creates some [amundsen databuilder](https://github.com/lyft/amundsendatabuilder) pipelines to enable this. 

There are **two** ways to add a new connection:

#### **Option 1: Run `mf connections add`, with keyword arguments.**

Use the following flags to specify database properties.

```text
--name      # A unique name to identify your connection.
--type      # The "type" of connection, e.g. presto.
--host      # The host:port address.
--username  # [Optional]
--password  # [Optional]
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

**Option 2: Manually edit `~/.metaframe/config/connections.yaml`**

The above commands simply edit a yaml file located at `~/.metaframe/config/connections.yaml` referenced by the markdown loader. You can, therefore, alternatively manually edit this connection file yourself. Simply add the following block:

```text
- name: presto
  type: presto
  host: ec2-something.yourhost.com:8889
  username:
  password:
  cluster:
```

All fields are optional except for `host` and `type`.

