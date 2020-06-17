# Bigquery

## Supported flags

* **`name`** Name to be used in naming the sub-folder for this connection. Files will be searchable as `name/cluster.schema.table`.
* **`key_path`** Path to json key.
* **`project_id`** Google cloud project ID with the database you want to scrape.
* **`project_credentials`** If you don't have a key, you can alternatively explicitly pass in project credentials.
* **`page_size`** Passed to the `maxResults` keyword argument when running `.datasets().list()` on a `googleapiclient` `Resource`. This simply truncates the list of datasets to be returned. See the [GCP documentation](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list) for more information.
* **`filter`** As with `page_size`, passed to `.datasets().list()` to filter the results. See the [GCP documentation](https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list) for more information.

