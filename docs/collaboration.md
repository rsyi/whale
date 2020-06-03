# Collaboration

## Version tracking

If you're working in a team of more than 1 person, you may want to consider tracking your `./metaframe/metadata` directory through git rather than locally through your own computer. A `git pull` is much cheaper than a full-fledged ETL job.

To set this up, navigate to `~/.metaframe/metadata`, run `git init`, and proceed as usual with any other repo. Voila -- you now have version-tracked metadata!

{% hint style="danger" %}
Because syncing your custom metadata upstream with neo4j is not on our roadmap \(and because it is also very difficult to implement seamlessly\), we strongly suggest you do not push to master -- back up your personal docs to a branch instead.
{% endhint %}

## Scheduling

If you are using git to version track your data, a reasonable next step is to schedule a job so users can simply sync their `~/.metaframe/metadata` repo with a central repository, rather than having to run an ETL job locally. An easy way to do this could be through github actions or a CI/CD pipeline, which is free, up to a point. 

_**TODO: Post an example github action script to demonstrate how this could work.**_

