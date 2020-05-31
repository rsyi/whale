# Engines

All code within supports the use of the `Engine` class, which is an adaptation of amundsen's `Extractor` class. While `Extractor`s tie a single query to a single `Extractor`, `Engine`s allow instantiated connections to be re-used for multiple queries, making for cleaner code when running repeated queries over the same connection, while still providing a pattern that abstracts the nitty-gritty of connecting to different kinds of databases. This also means you can put a ton of different methods (with different queries) into one engine, have an extractor call all of them, then load them with a single loader.

While `SQLAlchemy` does this to an extent, the `Engine` framework allows us to be slightly less modular and so more intuitive for quick use. The `Engine` class follows roughly the same architecture as other amundsen classes:

* All engines should inherit an abstract `Engine` base class, which has abstract methods `init`  and `execute`. Unlike `extract`, `execute` takes a single argument -- the query to be executed.
* `Engine`s use `pyhocon` for configuration, in the same way as other amundsen classes.
* `Engine`s are lazily initialized, so `init` much be run with a `pyhocon.ConfigTree` to instantiate the connection and parse the configuration.
* The `Engine` base class inherits the `Scoped` class, enabling the `get_scoped_conf` method to pull a `pyhocon.ConfigTree` out of a `pyhocon.ConfigTree`.

