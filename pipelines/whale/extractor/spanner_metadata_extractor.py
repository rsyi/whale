import json
import logging

from collections import namedtuple, OrderedDict

from pyhocon import ConfigTree  # noqa: F401
from typing import List, Any  # noqa: F401

from whale.extractor.base_spanner_extractor import BaseSpannerExtractor
from whale.models.table_metadata import TableMetadata, ColumnMetadata

from ddlparse import DdlParse


LOGGER = logging.getLogger(__name__)


def format_spanner_fields(column_name, column_contents):

    if column_contents.array_dimensional <= 1:
        type = column_contents._data_type

    else:
        # multiple dimensional array data type
        type = "RECORD"

        fields = OrderedDict()
        fields_cur = fields

        for i in range(1, column_contents.array_dimensional):
            is_last = True if i == column_contents.array_dimensional - 1 else False

            fields_cur['fields'] = [OrderedDict()]
            fields_cur = fields_cur['fields'][0]

            fields_cur['name'] = "dimension_{}".format(i)
            fields_cur['type'] = column_contents.bigquery_legacy_data_type if is_last else "RECORD"
            fields_cur['mode'] = column_contents.bigquery_mode if is_last else "REPEATED"

    col = OrderedDict()
    col['name'] = column_name
    col['type'] = type
    col['mode'] = column_contents.bigquery_mode
    if column_contents.description is not None:
        col['description'] = column_contents.description
    if column_contents.array_dimensional > 1:
        col['fields'] = fields['fields']

    return json.dumps(col, ensure_ascii=False)


class SpannerMetadataExtractor(BaseSpannerExtractor):

    """A metadata extractor for spanner databases, taking the schema metadata
    from the google cloud spanner APIs. This extractor goes through all
    visible instances in the project identified by project_id and iterates over
    all databases it finds. A separate account is configurable through the
    key_path parameter, which should point to a valid json file corresponding
    to a service account.
    """

    def init(self, conf: ConfigTree) -> None:
        BaseSpannerExtractor.init(self, conf)

    def _retrieve_databases(self, instance) -> Any:
        for page in self._page_database_list_results(instance):
            if "databases" not in page:
                continue

            for database in page["databases"]:
                database_name = database["name"]

                database = (
                    self.spanner_service.projects()
                    .instances()
                    .databases()
                    .getDdl(
                        database=database_name,
                    )
                    .execute(num_retries=SpannerMetadataExtractor.NUM_RETRIES)
                )

                if "statements" not in database:
                    continue

                for statement in database["statements"]:
                    # DdlParse currently cannot handle MAX
                    ddl = statement.replace("(MAX)", "(10485760)")

                    # DdlParse currently cannot handle ARRAY
                    if "ARRAY" in ddl:
                        continue

                    if "CREATE TABLE" not in ddl:
                        continue

                    # Using DdlParse to convert Spanner DDL to BQ JSON schema
                    try:
                        parsed_ddl = json.loads(DdlParse().parse(ddl))
                    except:
                        continue

                    cols = []
                    total_cols = 0
                    for column_name, column_contents in parsed_ddl.items():
                        column = format_spanner_fields(column_name, column_contents)
                        total_cols = self._iterate_over_cols(
                            "", column, cols, total_cols + 1
                        )

                    schema = "{}.{}".format(
                        database_name.split("/")[3], database_name.split("/")[5]
                    )

                    table_meta = TableMetadata(
                        database="spanner",
                        cluster=database_name.split("/")[1],
                        schema=schema,
                        name=ddl.split()[2],
                        description=None,
                        columns=cols,
                    )

                    yield (table_meta)

    def _iterate_over_cols(
        self, parent: str, column: str, cols: List[ColumnMetadata], total_cols: int
    ) -> int:
        if len(parent) > 0:
            col_name = "{parent}.{field}".format(parent=parent, field=column["name"])
        else:
            col_name = column["name"]

        if column["type"] == "RECORD":
            col = ColumnMetadata(
                name=col_name,
                description=column.get("description", ""),
                col_type=column["type"],
                sort_order=total_cols,
            )
            cols.append(col)
            total_cols += 1
            for field in column["fields"]:
                total_cols = self._iterate_over_cols(col_name, field, cols, total_cols)
            return total_cols
        else:
            col = ColumnMetadata(
                name=col_name,
                description=column.get("description", ""),
                col_type=column["type"],
                sort_order=total_cols,
            )
            cols.append(col)
            return total_cols + 1

    def get_scope(self):
        # type: () -> str
        return "extractor.spanner_table_metadata"
