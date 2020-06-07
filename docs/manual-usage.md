# Manual usage

While metaframe supports the ability to run ETL jobs and automatically document tables, you can also use it fully manually if you so desire to document whatever you'd like -- tables, ML models, dashboards. The actual base idea behind `mf` is pretty flexible.

The `mf` command searches over your `~/.metaframe/metadata` directory for two kinds of files:

* **`table_name.md` files** These contain the more or less unchanging properties about your dataset. Because these are not by default visible or editable by `mf`, you probably don't want to store any frequently changing data here. For now, you'll have to go in and edit these manually in `~/.metaframe/metadata` \(though we plan on providing an `mf --all` command to add the ability to show and edit all raw files soon\).
* **`table_name.docs.md`** **files** These contain any custom docs you want to add regarding the dataset. This is the file that you enter when you press `enter` in the `mf` window, so this is a great place to store quick notes or anything you need to be easily editable. 

