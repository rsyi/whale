# ~/.whale file structure

## Overview

This section provides an in-depth overview of the `~/.whale` file structure. Unlike most command-line tools, whale requires data storage: warehouse credentials, scraped metadata, metadata caches for speedier access, user-generated content, logs, etc. We therefore use the `~/.whale` directory to store all of this information.

While whale will happily manage these files automatically without any user intervention, understanding how it does so can be helpful in building more advanced functionality on top of whale.

Whale installs all files within the `~/.whale` path. This path contains the following directory structure:

```text
~/.whale
├── bin  
├── config  
│   └── connections.yaml
├── libexec
├── logs
├── manifests
├── metadata
├── metrics
└── templates
```

## Subdirectories

### bin

This folder contains the whale binary, `whale`, if whale is built from source \(homebrew manages all binaries within its own file structure\). The `whale` binary within can be executed directly, though we recommend setting an alias in your shell's `rc` file so `wh` can be executed without having to remember this path.

The `whale` binary within is not portable without the rest of the directory, as it relies on the other directories for metadata scraping, caching, and storage.

### config

This folder contains all whale configuration files. Most notably, `connections.yaml` is stored here, which contains a list of all registered warehouse connections.

#### connections.yaml

This file contains all warehouse connections. This file can be directly accessed by running `wh connections edit`, and new connections are configured through the `wh init` workflow.

Each connection is stored as a separated yaml document within the `connections.yaml` file, meaning they are separated by `---`. Unlike most warehouse interfaces, we chose to use configuration keys that differ for each warehouse for easier end-user comprehension.

See the [Connection configuration](../setup/connection-configuration.md) section for warehouse-specific instructions. For most use cases, however, we recommend simply running through the `wh init` workflow to add new warehouses \(you can repeat this command multiple times without worrying about clearing your existing connections\).

### libexec

`libexec` houses the virtual environment containing the `whale-pipelines` installation, as well as some scripts that are accessed by the rust-side of the program \(`build_script.py` and `run_script.py`\).

The name `libexec` comes from the unix `/usr/libexec` directory, which houses binaries that are not intended to be accessed by the end-user. However, because these are written entirely in python and stored as a virtual environment, it is completely acceptable to modify these directly.

### logs

Contains all system logs. In particular, `cron.logs` within contains logs from any registered cron job, which can be useful for debugging broken ETL processes.

### manifests

In order to speed up search execution and avoid indexing stale \(deleted\) tables, we store a cache of all table names in `manifest.txt` within this directory that only contains tables from the most recent metadata scrape. During a scraping job, these tables are appended to a temporary manifest named `tmp_manifest_<NUMBER>.txt` file \(where `NUMBER` is appended to prevent simultaneous scraping jobs from appending to the same temporary manifest\). Upon completion of the ETL job, this manifest is copied to `manifest.txt`.

This logic and the manifests within generally need not be understood by the end-user \(or most developers\). However, this design pattern should be leveraged when adding the ability to search over other non-table assets.

### metadata

The metadata directory stores all warehouse metadata. When typing `enter` on a selected `wh`-searched file, documents within this directory are opened.

### metrics

The metrics directory stores all calculated metrics \(along with a timestamp of when the metrics were calculated\). The folder structure follows the same structure as the metadata folder, except the table name is used as a folder to house \(and prevent collisions over\) metric names: `warehouse_name/catalog.schema.table/metric-name.md`.

### templates

The templates directory is where users can add their own Jinja2 templates. When named in the form `warehouse-connection-name.sql`, these templates are pre-pended to any queries run against the warehouse with connection name `warehouse-connection-name`. See the [Jinja2 templating](../features/jinja2-templating.md) section for more details. Connection names can be found by running `wh connections`, in the `name` field of each yaml block. 

