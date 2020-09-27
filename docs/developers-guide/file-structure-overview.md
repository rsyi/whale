# ~/.whale file structure

## Overview

This section provides an in-depth overview of the ~~/.whale file structure. Unlike most command-line tools, whale requires data storage: warehouse credentials, scraped metadata, metadata caches for speedier access, custom notes, logs, etc. We therefore use the \`~~/.

While whale will generally manage the lifetimes of all build artifacts, ETL artifacts, caches, local warehouse metadata, and user-generated documentation automatically, understanding how it does so can be helpful in building more advanced functionality on top of whale.

Whale installs all files within the `~/.whale` path. This path contains the following directory structure:

```text
~/.whale
├── bin  
├── config
│   └── connections.yaml
├── libexec
├── logs
├── manifests
└── metadata
```

## Subdirectories

### bin

This folder contains the whale binary, `whale`, if whale is built from source \(homebrew manages all binaries within its own file structure\). The `whale` binary within can be executed directly, though we recommend setting an alias in your shell's `rc` file so `wh` can be executed without having to remember this path.

The `whale` binary within is not portable without the rest of the directory, as it relies on the other directories for metadata scraping, caching, and storage.

### config

This folder contains all whale configuration files. Most notably, `connections.yaml` is stored here, which contains a list of all registered warehouse connections.

#### connections.yaml

This file contains all 

