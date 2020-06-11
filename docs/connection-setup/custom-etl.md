# Other / Custom ETL

In some situations \(e.g. if your company uses some sort of proxy service to access the database rather than providing users with the access credentials directly; or if you simply want to use a connection that isn't natively supported by metaframe\), you may want to manage your own ETL job. We provide instructions here on how to do this.

## Writing your own "build script"

Because metaframe's search and metadata file structure components operate entirely independently of the ETL job \(they just rely on the text files that are produced\), you can easily write your own ETL job in a python script and run that instead of `mf etl`. We even provide bindings to effectively re-bind `mf etl` to this script. We call this python script a **build script**.

{% hint style="info" %}
See [`./metaframe/example/sample_build_script.py`](https://github.com/rsyi/metaframe/blob/master/metaframe/example/sample_build_script.py) for an example of such a script. 
{% endhint %}

Writing your own build script, in short, simply involves invoking a databuilder `Task` using:

* **An `Extractor`** See amundsen's [databuilder](https://github.com/lyft/amundsendatabuilder) library or `metaframe.extractor` for a number of compatible extractors.
* **The `MarkdownLoader` class** This generates markdown strings from your extracted data.
* **And the `MetaframeTransformer` class**  This dumps the markdown strings into files\).

This pattern mostly follows the architecture of amundsen-databuilder's `Task`s \(see [this doc](https://lyft.github.io/amundsen/databuilder/) for a more detailed overview\). We simplify the process considerably by removing `Publishers` and `Jobs`, in order to \(a\) sequentially pull data, which is enabled by the `Task` class, and \(b\) avoid having to redundantly dump all metadata to comma-separated files before copying these to our markdown format, which is the typical pattern for amundsen `Job`s.

Some small additional details to be mindful of:

* `pyhocon.ConfigFactory` objects are used to configure the task, and are passed at lazy initialization time of the task.
* `Task`s are lazily initialized, and both the `init` and `run` method need to be called for it to run.

## Adding the build script to the connections.yaml manifest

Once you've written an executable python build script, you can either run it manually when you want to refresh your local metadata or you can **register it within the `connections.yaml` file to link it with `mf etl`.** To accomplish the latter, run `mf connections edit` to open your `connections.yaml` file, and add the following lines:

```text
- type: build_script
  build_script_path: /path/to/build/script
  python_binary: python3
  venv_path: ~/envs/default
```

There are a few key arguments of note, above:

* **`type`** The `build_script` type signals to metaframe that you'll be using a custom build script which will simply be executed.
* **`build_script_path`** This is the path to the build\_script. We like to keep these in `~/.metaframe/build_scripts/`, but these can be stored anywhere.
* **`python_binary`** The path to \(or alias of\) the python binary that you want to use for execution of the build script. Typically, `python3` works fine, but if you aren't using python &gt;= 3.6, you may have to install a newer version of python or find a &gt;= 3.6 binary.
* **`venv_path`** The path to a virtual environment that will be used for the installation. We assume there is a `/bin/activate` file, which will be sourced at runtime.

