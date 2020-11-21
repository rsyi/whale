import logging
import json
from collections import namedtuple

from pyhocon import ConfigTree  # noqa: F401
from typing import List, Any  # noqa: F401

from whale.extractor.base_bigquery_extractor import BaseBigQueryExtractor
from whale.models.table_metadata import TableMetadata, ColumnMetadata


DatasetRef = namedtuple("DatasetRef", ["datasetId", "projectId"])
TableKey = namedtuple("TableKey", ["schema", "table_name"])

LOGGER = logging.getLogger(__name__)


class BigQueryMetadataExtractor(BaseBigQueryExtractor):

    """A metadata extractor for bigquery tables, taking the schema metadata
    from the google cloud bigquery APIs. This extractor goes through all
    visible datasets in the project identified by project_id and iterates over
    all tables it finds. A separate account is configurable through the
    key_path parameter, which should point to a valid json file corresponding
    to a service account.

    This extractor supports nested columns, which are delimited by a dot (.) in
    the column name.
    """

    def init(self, conf: ConfigTree) -> None:
        BaseBigQueryExtractor.init(self, conf)
        self.grouped_tables = set([])

    def _retrieve_tables(self, dataset) -> Any:
        for page in self._page_table_list_results(dataset):
            if "tables" not in page:
                continue

            for table in page["tables"]:
                tableRef = table["tableReference"]
                table_id = tableRef["tableId"]

                # BigQuery tables that have 8 digits as last characters are
                # considered date range tables and are grouped together in the
                # UI. (e.g. ga_sessions_20190101, ga_sessions_20190102, etc.)

                if self._is_sharded_table(table_id):
                    # If the last eight characters are digits, we assume the
                    # table is of a table date range type and then we only need
                    # one schema definition
                    table_prefix = table_id[: -BigQueryMetadataExtractor.DATE_LENGTH]
                    if table_prefix in self.grouped_tables:
                        # If one table in the date range is processed, then
                        # ignore other ones (it adds too much metadata)
                        continue

                    table_id = table_prefix
                    self.grouped_tables.add(table_prefix)

                table = (
                    self.bigquery_service.tables()
                    .get(
                        projectId=tableRef["projectId"],
                        datasetId=tableRef["datasetId"],
                        tableId=tableRef["tableId"],
                    )
                    .execute(num_retries=BigQueryMetadataExtractor.NUM_RETRIES)
                )

                tags_dict = None
                try:
                    # Fetch entry for given linked_resource
                    entry = self.datacatalog_service.lookup_entry(
                        request={
                            "linked_resource": f"//bigquery.googleapis.com/projects/{tableRef['projectId']}/datasets/{tableRef['datasetId']}/tables/{tableRef['tableId']}"
                        }
                    )
                    if not isinstance(entry, dict):
                        entry_json = entry.__class__.to_json(entry)
                        entry = json.loads(entry_json)

                    # Fetch tags for given entry
                    tags = self.datacatalog_service.list_tags(
                        request={"parent": entry["name"]}
                    )
                    tags_dict = dict(tags)
                except Exception as e:
                    LOGGER.warning(f"Error fetching tags from Data Catalog: {e}")

                cols = []
                if self._is_table_match_regex(tableRef):
                    # BigQuery tables also have interesting metadata about
                    # partitioning data location (EU/US), mod/create time, etc...
                    # Extract that some other time?
                    # Not all tables have schemas
                    if "schema" in table:
                        schema = table["schema"]
                        if "fields" in schema:
                            total_cols = 0
                            for column in schema["fields"]:
                                total_cols = self._iterate_over_cols(
                                    tags_dict, "", column, cols, total_cols + 1
                                )

                table_tag = None
                if tags_dict and "tags" in tags_dict:
                    for tag in tags_dict["tags"]:
                        if "column" not in tag:
                            table_tag = tag

                table_meta = TableMetadata(
                    database=self._database,
                    cluster=tableRef["projectId"],
                    schema=tableRef["datasetId"],
                    name=table_id,
                    description=table.get("description", ""),
                    columns=cols,
                    is_view=table["type"] == "VIEW",
                    tags=table_tag,
                    labels=table.get("labels", ""),
                )

                yield (table_meta)

    def _iterate_over_cols(
        self,
        tags_dict: dict,
        parent: str,
        column: str,
        cols: List[ColumnMetadata],
        total_cols: int,
    ) -> int:
        if len(parent) > 0:
            col_name = "{parent}.{field}".format(parent=parent, field=column["name"])
        else:
            col_name = column["name"]

        tags = None
        if tags_dict and "tags" in tags_dict:
            for tag in tags_dict["tags"]:
                if "column" in tag:
                    if tag["column"] == col_name:
                        tags = tag

        if column["type"] == "RECORD":
            col = ColumnMetadata(
                name=col_name,
                description=column.get("description", ""),
                col_type=column["type"],
                sort_order=total_cols,
                tags=tags,
            )
            cols.append(col)
            total_cols += 1
            for field in column["fields"]:
                total_cols = self._iterate_over_cols(
                    tags_dict, col_name, field, cols, total_cols
                )
            return total_cols
        else:
            col = ColumnMetadata(
                name=col_name,
                description=column.get("description", ""),
                col_type=column["type"],
                sort_order=total_cols,
                tags=tags,
            )
            cols.append(col)
            return total_cols + 1

    def get_scope(self):
        # type: () -> str
        return "extractor.bigquery_table_metadata"
