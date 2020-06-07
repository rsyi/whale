# Manual usage

While metaframe supports the ability to run ETL jobs and automatically document tables, you can also use it fully manually if you so desire to document whatever you'd like -- tables, ML models, dashboards. The actual base idea behind `mf` is pretty flexible.

## Creating a new table stub

If you want to start documenting a new dataset, run the following

```text
mf new <TABLE_NAME>
```

replacing `<TABLE_NAME>` with your table's name. This will create a new metaframe table stub and open up a `<TABLE_NAME>.docs.md` file, which will allow you to start writing documentation for this table immediately. The metadata will then be searchable using `mf`, as usual.

We follow the naming convention `database/cluster.schema.table` \(and drop the `cluster` if there is no cluster or catalog\), which, if you want to maintain compatibility with future ETL jobs, we recommend you follow as well. 

## Overview of the file structure

The `mf` command searches over your `~/.metaframe/metadata` directory for two kinds of files:

* **`table_name.md` files** These contain the more or less unchanging properties about your dataset. Because these are not by default visible or editable by `mf`, you probably don't want to store any frequently changing data here, as this will be overwritten by any future ETL jobs. If you do, however, you can run `mf --all` to show and edit all raw files.
* **`table_name.docs.md`** **files** These contain any custom docs you want to add regarding the dataset. This is the file that you enter when you press `enter` in the `mf` window, so this is a great place to store quick notes or anything you need to be easily editable. 

