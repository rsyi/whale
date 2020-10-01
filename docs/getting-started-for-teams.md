# Getting started for teams

## Overview

Outside of personal use, whale can also be used collaboratively by setting up a git repository \(e.g. github\) + CI/CD pipelines to function as a centralized source of truth for your organization. This is possible because the metadata and user-generated content accessed by whale are stored as markdown in the `~/.whale` subdirectory.

Using a hosted git solution has the distinct advantage of enabling:

* **Automatic schema/change tracking** Any changes will be change-tracked through git.
* **Collaboration using a centralized hosted git repository** Rather than working on locally-available \(and thus, isolated\) table stubs, you can push and pull metadata from your remote server, enabling quicker, richer data discovery and documentation.
* **Automated non-cron scheduling through CI/CD pipelines** While the cron-based metadata-scraping functionality of whale is generally pretty seamless and low-cost, it may be helpful to centralize this process to reduce warehouse load at scale.

## Getting started

### Setting up .whale/ as a git repository

If you haven't already, start by installing whale locally, following the [Getting started](./#installation) guide. If you have a cron job scheduled, remove it by running `crontab -e` and deleting the appropriate line to avoid scraping the metadata in multiple places. Then run the following series of commands to push your whale directory to your remote branch, taking care to replace `<YOUR_GIT_ADDRESS>` with your git address.

```text
cd ~/.whale
git init && git remote add origin <YOUR_GIT_ADDRESS>
echo "bin/" > .gitignore
git add .
mkdir -p .github/workfloms
git commit -am "first commit"
git push origin master
```

### Creating a CI/CD pipeline using github actions

Next, you'll need to set up a CI/CD pipeline to handle the scraping of metadata for you. Below, we illustrate how to do this through **github actions** \(which simply requires that you add the following file to the `.github/workflows` directory\), but the steps can be easily adapted to any CI/CD platform. In short, we run the following steps:

* Checkout the repository.
* Copy the repository to `~/.whale` on the runner machine.
* Run the etl process by directly executing the compiled `build_script` binary in `libexec`.
* Push these changes back to the repo.

```text
name: Whale ETL
on:
  schedule:
   - cron: "* */6 * * *"

jobs:
  run-etl:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v2
        with:
          path: main
      - name: wh etl
        run: |
          mkdir -p ~/.whale/metadata
          mkdir -p ~/.whale/manifests
          mkdir -p ~/.whale/logs
          cp -r main/ ~/.whale

          echo ${BIGQUERY_JSON} > ~/.whale/config.json
          ~/.whale/libexec/dist/build_script/build_script
          cd ~/.whale
          git config --global user.name 'GHA Runner'
          git config --global user.email 'rsyi@users.noreply.github.com'
          git add metadata manifests
          git commit -am "Automated push." || echo "No changes to commit"
          git push
        env:
          BIGQUERY_JSON: ${{ secrets.BIGQUERY_JSON }}


```

For illustration purposes, we've used github secrets to store our Bigquery credentials.json file, then pipe this into the path `~/.whale/config.json` file, which we reference in `config/connections.yaml`. Sensitive data \(even the entire `connections.yaml` file, if you so desire\) can be stored as a github secret, read into a file during the CI/CD process, then safely referenced by `whale` binaries for warehouse access.

Take care to \(a\) change the `git config` specifications, and \(b\) choose a runner that coincides with the machine type that you used to create the python binaries \(below, we use `macos-latest`\). That said, though our instructions thus far have involved the storage of the compiled python binaries \(`libexec/`\) on github as well \(simply to reduce compute time\), it is alternatively possible to simply install `whalebuilder` and run `build_script.py` manually with each CI/CD run. In this case, specification of the machine type is irrelevant, as no `pyinstaller` compilation would need to take place.

### Schedule a local cron job to keep your local table stubs up-to-date

TODO

