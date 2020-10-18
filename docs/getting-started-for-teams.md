# Getting started for teams

## Overview

Outside of personal use, whale can also be used collaboratively by setting up a git repository \(e.g. github\) to function as a centralized source of truth for your organization, with metadata being periodically updated through CI/CD pipelines. This is possible because the metadata and user-generated content accessed by whale are stored as markdown in the `~/.whale` subdirectory.

Using a hosted git solution has the distinct advantage of enabling:

* **Automatic schema/change tracking** Any changes will be change-tracked through git.
* **Collaboration using a centralized hosted git repository** Rather than working on local-only table stubs, you can push and pull documentation from your remote server, enabling collaboration.
* **Automated non-cron scheduling through CI/CD pipelines** While the cron-based metadata-scraping functionality of whale is generally pretty seamless and low-cost, it may be helpful to centralize this process to reduce warehouse load at scale.
* **A centralized GUI with linkable table details pages.** Because servers like github naturally render markdown files, documentation can be shared with others by passing around plain links.

## Getting started

### Setting up .whale/ as a git repository

If you haven't already, start by installing whale locally, following the [Getting started](./#installation) guide.

{% hint style="warning" %}
While you're going through this process, it may be prudent to disable any local etl runs by running `crontab -e` and removing the appropriate `wh etl` line. Either save this somewhere and add it back in later to enable scheduled `git pull`ing, or run `wh schedule` again.
{% endhint %}

Then navigate to `~/.whale` and run the following series of commands to push your whale directory to your remote branch, taking care to replace `<YOUR_GIT_ADDRESS>` with your git address.

```text
git init && git remote add origin <YOUR_GIT_ADDRESS>
echo "bin/" > .gitignore
git add . && git commit -m "Whale on our way!"
git push -u origin master
```

### Creating a CI/CD pipeline using github actions

Next, you'll need to set up a CI/CD pipeline to handle the scraping of metadata for you. Below, we illustrate how to do this through **github actions** \(which simply requires that you add the following file to the `.github/workflows` directory\), but the steps can be easily adapted to any CI/CD platform. In short, we run the following steps:

* Checkout the repository.
* Copy the repository to `~/.whale` on the runner machine.
* Run the etl process by directly executing the compiled `build_script` binary in `libexec`.
* Push these changes back to the repo.

Begin by creating a local directory for your github actions workflows:

```text
cd ~/.whale
mkdir -p .github/workflows
```

Then, within `.github/workflows`, create a new file \(e.g. `metadata.yml`\), and paste in the following file.

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
      - name: etl
        run: |
          mkdir -p ~/.whale/metadata
          mkdir -p ~/.whale/manifests
          mkdir -p ~/.whale/logs
          cp -r main/ ~/.whale

          echo ${BIGQUERY_JSON} > ~/.whale/config.json  # ONLY FOR BIGQUERY 
          
          ~/.whale/libexec/dist/build_script/build_script
          cd ~/.whale
          git config --global user.name 'GHA Runner'
          git config --global user.email '<YOUR_GIT_USERNAME>@users.noreply.github.com'
          git add metadata manifests
          git commit -am "Automated push." || echo "No changes to commit"
          git push
        env:  # ONLY FOR BIGQUERY
          BIGQUERY_JSON: ${{ secrets.BIGQUERY_JSON }}


```

Take care to \(a\) change the `git config` specifications in the file above to reflect your own information, and \(b\) choose a runner that coincides with the machine type that you used to create the python binaries \(below, we use `macos-latest`, because my local machine is a mac\).

That said, though our instructions thus far have involved the storage of the compiled python binaries \(`libexec/`\) on github as well \(simply to reduce compute time\), it is alternatively possible to simply install `whale-pipelines` and run `build_script.py` manually with each CI/CD run. In this case, specification of the machine type is irrelevant, as no `pyinstaller` compilation would need to take place.

{% hint style="info" %}
For illustration purposes, we've used github secrets to store our Bigquery credentials.json file, then pipe this into the path `~/.whale/config.json` file, which we reference in `config/connections.yaml`. Sensitive data \(even the entire `connections.yaml` file, if you so desire\) can be stored as a github secret, read into a file during the CI/CD process, then safely referenced by `whale` binaries for warehouse access. These lines are explicitly tagged with `# ONLY FOR BIGQUERY` in the config file above. If you are not on bigquery, simply remove these sections.
{% endhint %}

### Update your local whale instance

Now that you have a remote git server pulling metadata, you'll want to avoid scraping metadata independently from your warehouse, and instead periodically rebase your table stubs over your git remote. If you desire, you can set a `git pull --autostash --rebase` to occur programmatically. To do this, run the following command:

```text
wh git-enable
```

This will add a `is_git_etl_enabled: "true"` flag to the file located at `~/.whale/config/config.yaml`. This file can be accessed by running `wh config` and manually edited at any point to turn the flag off.

{% hint style="warning" %}
At this point, if you previously removed the scheduled cron job from `crontab -e`, you should add this back in by either copying back in the deleted line, or re-running `wh schedule`. While the `is_git_etl_enabled` flag is `true`, `wh pull` \(and your cron job\) will ignore all user-defined connections and instead run `git pull --autostash --rebase`.
{% endhint %}

While programmatic `git` actions in other situations can be a bit dangerous, whale's file formatting ensures that this will be done in debuggable and easily resolvable manner. Because the only local command run is the `git pull --autostash --rebase` command, your personal edits will be saved as merge conflicts, still viewable in the respective files \(and therefore, through `wh`\). If such conflicts arise, we will surface this to you through a warning when running `wh`, and they should be simple to address.

