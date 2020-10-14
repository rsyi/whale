# Git setup

## Overview

Outside of personal use, whale can also be used collaboratively by setting up a git repository \(e.g. github\) to function as a centralized source of truth for your organization, with metadata being periodically updated through CI/CD pipelines. This is possible because the metadata and user-generated content accessed by whale are stored as markdown in the `~/.whale` subdirectory.

## Getting started

### Setting up .whale/ as a git repository

If you haven't already, start by installing whale locally, following the [Getting started](../#installation) guide.

{% hint style="warning" %}
While you're going through this process, it may be prudent to **disable any local etl runs** by running `crontab -e` and commenting out \(with `#`\) the appropriate `wh pull` line.
{% endhint %}

Then run the following command to set up and push your `~/.whale` directory to the provided git remote \(replace `<YOUR_GIT_REMOTE>` with your git address\).

```text
wh git-setup <YOUR_GIT_REMOTE>
```

### Creating a CI/CD pipeline using github actions

Next, you'll need to set up a CI/CD pipeline to handle the scraping of metadata for you. Below, we illustrate how to do this through **github actions**, but the steps can be easily adapted to any CI/CD platform. In short, we: \(a\) clone the repo into `~/.whale`, \(b\) install the python library, \(c\) update the metadata, and \(c\) push these changes back to the repo.

Begin by creating a local directory for your github actions workflows:

```text
cd ~/.whale
mkdir -p .github/workflows
```

Then, within `.github/workflows`, create a new file \(e.g. `metadata.yml`\), paste in the following file, then `git add`, `commit`, & `push` to master.

```text
name: Whale ETL
on:
  schedule:
   - cron: "* */6 * * *"

jobs:
  run-etl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-python@v2
        with:
          python-version: '3.7'
      - uses: actions/checkout@v2
      - name: install
        run: |
          cp -r . ~/.whale/
      - uses: actions/checkout@v2
        with:
          repository: dataframehq/whale
          path: whale
      - name: etl
        working-directory: ./whale
        run: |
          make python
          mkdir ~/.whale/logs
          source ./build/env/bin/activate
          python ./build/build_script.py
          cd ~/.whale
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

{% hint style="warning" %}
If you previously commented the `wh pull` job from `crontab -e`, you should uncomment this \(or add scheduling in by running `wh schedule`\). Your cron job will now ignore any warehouse connections and instead run rebase over your remote.
{% endhint %}

While programmatic `git` actions in other situations can be a bit dangerous, whale's file formatting ensures that this will be done in debuggable and easily resolvable manner. Because the only local command run is the `git pull --autostash --rebase` command, your personal edits will be saved as merge conflicts, still viewable in the respective files \(and therefore, through `wh`\). If such conflicts arise, we will surface this to you through a warning when running `wh`, and they should be simple to address.

### Team setup

Now that you've set up a git remote as your SSOT, have others [Install whale](../), then run the following series of commands to clone your central repo and set up a cron job to periodically rebase onto your remote:

```text
git clone <YOUR_GIT_REMOTE> ~/.whale
wh schedule
wh git-enable
```

## Advanced usage

### Storing credentials

If storing credentials as plaintext is a concern, a temporary workaround is to simply save the full `connections.yaml` file as a github secret, then echo this into the `~/.whale/config/connections.yaml` file.

```text
run: |
  echo ${CONNECTIONS} > ~/.whale/config/connections.yaml
env:
  CONNECTIONS: ${{ secrets.CONNECTIONS }}
```

For Bigquery, specifically, the credentials file alone could alternatively be echoed at schedule-time into the correct path, as follows:

```text
run: |
  echo ${BIGQUERY_JSON} > /keypath/specified/in/connections
env:
  BIGQUERY_JSON: ${{ secrets.BIGQUERY_JSON }}
```

