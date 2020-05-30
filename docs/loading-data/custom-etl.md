# Custom ETL

In some situations \(e.g. if your company uses some sort of proxy service to access the database rather than providing users with the access credentials directly\), you may want to manage your own ETL job. In this case, you can **write your own `databuilder.extractor.Extractor` object \(following amundsen's design patterns\) and manage your own ETL process**. See `metaframe/__init__.py` for an example of how to write such a script.

