`metaframe` is a CLI tool that allows you to run ETL jobs and view metadata straight from the command-line. It leverages [junegunn/fzf](https://github.com/junegunn/fzf) and [lyft/amundsen](https://github.com/lyft/amundsen) to create a blazingly fast CLI framework to search through your metadata.

## Installation

```
brew install rsyi/tap/metaframe
```

If not on macOS, clone this directory, then run:

```
make && make install
```

*Metaframe is still in alpha, so if you encounter a problem with installation, please post an issue, and I'll look it into it immediately.*

## Quick start
To quickly get started, run `mf init` (and follow the prompts) to add a db connection and `mf etl` to run an ETL job against that connection. Once these are complete, run `mf` to view your metadata.

```
mf
```

-----

## Connection setup (verbose)
To use metaframe, you need to configure your server connection to enable ETL jobs to be run. If you (a) already aggregate your data using a tool like amundsen or (b) are installing metaframe outside of a walled garden and need special access, see the "Running a custom ETL job" section.

There are **three** ways to add a new connection:

**Run `mf init`.**
This will manually walk you through a step-by-step set of prompts that will help you register your connection with metaframe.

**Run `mf connections add`, with keyword arguments.**
Use the following flags to specify database properties.

```
--name      # A unique name to identify your connection.
--type      # The "type" of connection, e.g. presto.
--host      # The host:port address.
--username  # [Optional]
--password  # [Optional]
```

For example:

```
mf connections add \
    --name presto \
    --type presto \
    --host ec2-1-2-3-4-5.us-west-2.compute.amazonaws.com:8889 \
    --username admin \
    --password bungabungabunga
```

**Manually edit `~/.metaframe/config/connections.yaml`**
The above commands simply edit a yaml file located at `~/.metaframe/config/connections.yaml` referenced by the markdown loader. You can, therefore, alternatively manually edit this connection file yourself. Simply add the following block:

```
- name: presto
  type: presto
  host: ec2-something.yourhost.com:8889
  username:
  password:
```

### Pulling data
Once your connection is set up, you can trigger a full ETL job by running:
```
mf etl
```
For most databases, this can take quite some time (~1 hour). We set this as a `nohup` job, so no need to wait and/or keep the terminal window open. If the job is taking exceedingly long, check the logs in `~/.metaframe/logs`.

## Customization

### Syntax highlighting for previews
Syntax highlighting is actually enabled by default, but you need a library called [bat](https://github.com/sharkdp/bat). Once this is installed, metaframe will attempt to use this to render your previews, instead of `cat`.

### Version-tracking and collaboration
If you're working in a team of more than 1 person, you may want to consider tracking your `./metaframe/metadata` directory through git rather than locally through your own computer.
To set this up, navigate to `~/.metaframe/metadata`, run `git init`, and proceed as usual with any other repo. Voila, you now have version-tracked metadata!

You can now set up a CI/CD pipeline or airflow to manage your ETL job for you.

### Running a custom ETL job
At its core, metaframe manages three components:

1. A custom `fzf` installation
2. A markdown-based metadata repository
3. A catch-all ETL job configured by `~/.metaframe/config/connections.yaml`.

The third component, the ETL job, may not suit all use cases. For example, companies may use some sort of proxy service to access the database rather than providing users with the access credentials directly. In this case, you can write your own `databuilder.extractor.Extractor` object (following amundsen's design patterns) and manage your own ETL process. See `metaframe/__init__.py` for an example of how to write such a script.

