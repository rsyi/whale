# Git setup

## Overview

Whale supports a free hosted backend + lightweight GUI through Github and Github actions \(though any git remote server + CI/CD system will work - you'll simply have to [write your own config](getting-started-for-teams.md#manual-setup)\). This is possible because the metadata and user-generated content accessed by whale are stored as markdown in the `~/.whale` subdirectory.

We have provided a series of simple commands and instructions to get started easily, but these can be [executed manually quite easily](getting-started-for-teams.md#advanced-usage) as well.

## Getting started

### Set up ~/.whale/ as a git repository

Run the following command to set up and push your `~/.whale` directory to the provided git remote \(replace `<YOUR_GIT_REMOTE>` with your git address\). This will add a `.gitignore` file, add all files, and push to your git remote server.

{% hint style="warning" %}
This will also push your `connections.yaml` file to your repo. If you'd like to avoid doing this, `mv` your `~/.whale/config/connections.yaml` file elsewhere first and see [**Advanced usage**](getting-started-for-teams.md#advanced-usage).
{% endhint %}

```text
wh git-setup <YOUR_GIT_REMOTE>
```

### Set up a CI/CD pipeline to scrape metadata

Below, we illustrate how to set up **github actions** to scrape metadata for you, but the steps can be [easily adapted to any CI/CD platform](getting-started-for-teams.md#manual-setup). We chose github actions because github supplies 2000 free minutes/month, even for private repos and organizations, which is generally more than enough to cover these simple scraping jobs.

First, create a local directory for your github actions workflows:

```text
mkdir -p ~/.whale/.github/workflows
```

Then, within this directory, create a new file \(e.g. `metadata.yml`\), paste in the following file, then `git add`, `commit`, & `push` to master.

```text
name: Whale Runner
on:
  schedule:
   - cron: "0 */12 * * *"

jobs:
  run-etl:
    runs-on: ubuntu-latest
    steps:

      # Setup python + clone repos
      - uses: actions/setup-python@v2
        with:
          python-version: '3.8'
      - uses: actions/checkout@v2
      - name: Copy to ~/.whale
        run: |
          cp -r . ~/.whale/
      - uses: actions/checkout@v2
        with:
          repository: dataframehq/whale
          path: whale

      # Scrape from warehouse
      - name: etl
        working-directory: ./whale
        run: |
          make python
          source ~/.whale/libexec/env/bin/activate
          python3 ~/.whale/libexec/build_script.py

      # Push to git
      - name: push-to-git
        working-directory: /home/runner/.whale
        run: |
          git config user.name 'GHA Runner'
          git config user.email '<your_username>@users.noreply.github.com'
          git add .
          git commit -m "Automated push." || echo "No changes to commit"
          git push

```

### Update your local whale instance

Now that you have a remote git server pulling metadata, you'll want to avoid scraping metadata independently from your warehouse, and instead periodically rebase your table stubs over your git remote. If you desire, you can set a `git pull --autostash --rebase` to occur programmatically. To do this, run the following command:

```text
wh git-enable
```

This will add a `is_git_etl_enabled: "true"` flag to the file located at `~/.whale/config/config.yaml`. This file can be accessed by running `wh config` and manually edited at any point to turn the flag off.

{% hint style="info" %}
While programmatic `git` commands can be a bit dangerous, whale's file formatting ensures that this is done in debuggable and easily resolvable manner. Because the only local command run is the `git pull --autostash --rebase` command, your personal edits will be saved as merge conflicts, still viewable in the respective files \(and therefore, through `wh`\). If such conflicts arise, we will surface this to you through a warning when running `wh`, and they should be simple to address.
{% endhint %}

### Team setup

Now that you've set up a git as your SSOT, have others [Install whale](../), then run the following series of commands to clone your central repo and set up a cron job to periodically rebase onto your remote:

```text
git clone <YOUR_GIT_REMOTE> ~/.whale
wh schedule
wh git-enable
```

Make sure to not have an existing `~/.whale` directory or the clone will fail.

## Advanced usage

### Manual setup

Though we have enabled convenient install hooks to make this git setup process easy, if you're familiar with git and a CI/CD platform, it is quite simple to implement all of this manually. In short, `wh git-setup` is doing the following:

* Creating a [`.gitignore` file](https://github.com/dataframehq/whale-bigquery-public-data/blob/master/.gitignore).
* `git add . && git commit -m "Whale on our way" && git push`

If you would rather not install the command-line tool, you can therefore simply create a repo, manually create a `credentials.yaml` file in `config/credentials.yaml`, and create a CI/CD pipeline that does the following \(or use our github action above\):

* Checkout your repo, and copy it to `~/.whale` on your CI/CD runner.
* Install python.
* `pip install whalebuilder`
* Run `python -c 'import whalebuilder as wh; wh.run()'`.
* Push the results back to git.

If you want improved logging, see [here](https://github.com/dataframehq/whale/blob/master/databuilder/build_script.py) for an example \(in short, simply `import logging` and adjust the logging level\).

### Storing credentials

If storing credentials as plaintext is a concern, a workaround is to simply save the full `connections.yaml` file as a Github secret \(named `CONNECTIONS` in the example below\), then echo this into the `~/.whale/config/connections.yaml` file. For example, with Github actions:

```text
run: |
  echo '${{ secrets.CONNECTIONS }}' > ~/.whale/config/connections.yaml
```

Then remove the file before the push step.

```text
run: |
  rm ~/.whale/config/connections.yaml
```

The full file would then be:

```text
name: Whale Runner
on:
  schedule:
   - cron: "0 */12 * * *"

jobs:
  run-etl:
    runs-on: ubuntu-latest
    steps:

      # Setup python + clone repos
      - uses: actions/setup-python@v2
        with:
          python-version: '3.8'
      - uses: actions/checkout@v2
      - name: Copy to ~/.whale
        run: |
          cp -r . ~/.whale/
      - uses: actions/checkout@v2
        with:
          repository: dataframehq/whale
          path: whale

      # Scrape from warehouse
      - name: etl
        working-directory: ./whale
        run: |
          make python
          source ~/.whale/libexec/env/bin/activate
          echo '${{ secrets.CONNECTIONS }}' > ~/.whale/config/connections.yaml
          python3 ~/.whale/libexec/build_script.py
          rm ~/.whale/config/connections.yaml

      # Push to git
      - name: push-to-git
        working-directory: /home/runner/.whale
        run: |
          git config user.name 'GHA Runner'
          git config user.email '<your_username>@users.noreply.github.com'
          git add metadata manifests metrics
          git commit -m "Automated push." || echo "No changes to commit"
          git push
```

For Bigquery, specifically, the credentials file alone could alternatively be echoed at runtime into the correct path, as follows:

```text
run: |
  echo '${{ secrets.BIGQUERY_JSON }}' > ~/.whale/credentials.json
```



