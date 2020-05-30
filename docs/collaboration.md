# Collaboration

## Version tracking

If you're working in a team of more than 1 person, you may want to consider tracking your `./metaframe/metadata` directory through git rather than locally through your own computer.

To set this up, navigate to `~/.metaframe/metadata`, run `git init`, and proceed as usual with any other repo. Voila -- you now have version-tracked metadata!

## Scheduling

If you are using git to version track your data, a reasonable next step is to schedule a job so users can simply sync their `~/.metaframe/metadata` repo with a central repository, rather than having to run an ETL job locally. An easy way to do this could be through github actions or a CI/CD pipeline, which is free, up to a point. 

_**TODO: Post an example github action script to demonstrate how this could work.**_

