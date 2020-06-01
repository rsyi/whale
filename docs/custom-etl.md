# Custom ETL

In some situations \(e.g. if your company uses some sort of proxy service to access the database rather than providing users with the access credentials directly\), you may want to manage your own ETL job. In this case, you can **write your own `build_script.py` using `databuilder.extractor.Extractor` objects \(following amundsen's design patterns\) and manage your own ETL process**. See `build_script.py` and `metaframe/__init__.py` for an example of how to write such a script.

Once this is written, you can bake this script into metaframe by navigating to the repo base directory, deleting the `/dist` directory, and running the following command:

```text
make && make install
```

We don't explicitly add an alias for the `mf` binary, so you'll want to either add `~/.metaframe/bin/` to your `PATH`, or add the following alias to your `.bash_profile` or `.zshrc` file.

```text
alias mf=~/.metaframe/bin/mf
```

This will allow `mf etl` to run your custom script.

