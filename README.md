# Metaframe

![](docs/.gitbook/assets/image%20%281%29.png)

_**Disclaimer: This project is still in alpha testing, so there will be bugs. Use at your own risk! But if you find bugs or have feature requests, open an issue :\)**_ 

`metaframe` is a CLI tool that allows you to run ETL jobs and view metadata straight from the command-line. It leverages [junegunn/fzf](https://github.com/junegunn/fzf) and [lyft/amundsen](https://github.com/lyft/amundsen) to create a blazingly fast CLI framework to search through your metadata.

### Installation

```text
brew install rsyi/tap/metaframe
```

If not on macOS, clone this directory, then run:

```text
make && make install
```

_Metaframe is still in alpha, so if you encounter a problem with installation, please post an issue, and I'll look it into it immediately._

### Quick start

To quickly get started, run `mf init` \(and follow the prompts\) to add a db connection and `mf etl` to run an ETL job against that connection. Once these are complete, run `mf` to view your metadata.

```text
mf
```

