# Custom extraction

Whale supports custom metadata scrapers through plain python scripts, as long as these satisfy two criteria:

* A list of all tables should be generated in `~/.whale/manifests/manifest.txt`.
* Table "stubs" should be stored as markdown files in `~/.whale/metadata/`.

To register a build script, simply run `wh init` and follow the `build-script` workflow, or manually add details \(see [Connection configuration](../setup/connection-configuration.md)\). 

This can be particularly useful for organizations where metadata is already collected and stored in an intermediary repository \(e.g. Amundsen\), or if you want to get metadata from a source we do not support. For an example, see the `databuilder/build_script.py` file in the whale github repo.





