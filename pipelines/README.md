# whale-pipelines

whale-pipelines is a library based on amundsen's databuilder that enables easy extraction of metadata into whale's markdown format. The library references static config files in `~/.whale/` to establish connections and customize the scraping process. Whale also provides hooks into SQLAlchemy for easy execution of SQL queries against these locally defined connections, without having to specify connection strings at every request.

For information on the full CLI platform, visit [whale](https://github.com/dataframehq/whale).

There are two main functions: `pull`, which handles metadata extraction, and `run`, which is enables execution of SQL queries.

## `pull`
While [whale](https://github.com/dataframehq/whale) invokes a `build_script.py` function to run `pull`, it does nothing else than call `pull()`, with some logging set up around it. If, therefore, you'd like to pare down/write a custom CI/CD pipeline, all you need to do is:

```
pip install whale-pipelines
```

then run:
```
import whale as wh
wh.pull()
```

## `run`
While libraries like pydobc, sqlalchemy, pyhive, etc. provide easy-to-use interfaces against a warehouse, the stateless nature of these libraries can make it a bit repetitive -- whenever you need to write a query, you generally need to open a cursor, specifying your warehouse URI and credentials. While somewhat trivial, `run` simply wraps SQLAlchemy, enabling you to open a connection automatically against connections defined in `~/.whale/config/connections.yaml`.

To use this, simply run:
```
import whale as wh
wh.run()
```

A `warehouse_name` kwarg can be specified, which will force `run` to establish a connection with the first warehouse with the corresponding `name` field matching the argument passed. If not given, the first warehouse in the list will be used.
