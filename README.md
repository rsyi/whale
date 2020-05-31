# Metaframe

![](docs/.gitbook/assets/image%20%281%29.png)

_**Disclaimer: This project is still in alpha testing, so there will be bugs. Use at your own risk! But if you find bugs or have feature requests, open an issue :\)**_ 

`metaframe` is a CLI tool that allows you to run ETL jobs and view metadata straight from the command-line. It leverages [junegunn/fzf](https://github.com/junegunn/fzf) and [lyft/amundsen](https://github.com/lyft/amundsen) to create a blazingly fast CLI framework to search through your metadata.

## Installation

### Mac OS

```text
brew install rsyi/tap/metaframe
```

### All others

If not on macOS, clone this directory, then run:

```text
make && make install
```

We don't explicitly add an alias for the `mf` binary, so you'll want to either add `~/.metaframe/bin/` to your `PATH`, or add the following alias to your `.bash_profile` or `.zshrc` file.

```text
alias mf=~/.metaframe/bin/mf
```

## Getting started

### Initialize file structure

Start by running:

```text
mf init
```

which will generate a file structure in `~/.metaframe`.

### Configure warehouse connections

You'll next need to **manually configure your warehouse connections**, which are defined as yaml in `~/.metaframe/config/connections.yaml`. Add an entry like the one below, replacing the credentials with your own.

```text
- name: presto                # optional
  type: presto
  host: host.mysite.com:8889
  username:                   # optional
  password:                   # optional
  cluster: system             # optional 
```

The only **necessary** arguments are the `host` and the `type`. At the moment, we only support `type: presto`, but this will change very soon.

### Run your ETL job

Once this configuration is complete, you can run your ETL job by running:

```text
mf etl
```

### Go go go!

Run:

```text
mf
```

to search over all metadata. Hitting `enter` will open the editable part of the docs in your default text editor, defined by the environmental variable `$EDITOR`.

