import boto3
from databuilder.extractor.base_extractor import Extractor
from pyhocon import ConfigFactory, ConfigTree
from typing import Any, Dict, Iterator, List, Union
from whale.models.table_metadata import TableMetadata, ColumnMetadata


class GlueExtractor(Extractor):
    """
    Extracts metadata from AWS glue. Adapted from Amundsen's glue extractor.
    """

    CONNECTION_NAME_KEY = "connection_name"
    FILTER_KEY = "filters"
    IS_LOCATION_PARSING_ENABLED_KEY = "is_location_parsing_enabled"
    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            FILTER_KEY: None,
            IS_LOCATION_PARSING_ENABLED_KEY: False,
            CONNECTION_NAME_KEY: None,
        }
    )

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(GlueExtractor.DEFAULT_CONFIG)
        self._filters = conf.get(GlueExtractor.FILTER_KEY)
        self._connection_name = conf.get(GlueExtractor.CONNECTION_NAME_KEY) or ""
        self._is_location_parsing_enabled = conf.get(
            GlueExtractor.IS_LOCATION_PARSING_ENABLED_KEY
        )
        self._glue = boto3.client("glue")
        self._extract_iter: Union[None, Iterator] = None

    def extract(self) -> Union[TableMetadata, None]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return "extractor.glue"

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        for row in self._get_raw_extract_iter():
            columns, i = [], 0

            for column in row["StorageDescriptor"]["Columns"] + row.get(
                "PartitionKeys", []
            ):
                columns.append(
                    ColumnMetadata(
                        column["Name"],
                        column["Comment"] if "Comment" in column else None,
                        column["Type"],
                        i,
                    )
                )
                i += 1

            if self._is_location_parsing_enabled:
                catalog, schema, table = self._parse_location(
                    location=row["StorageDescriptor"]["Location"], name=row["Name"]
                )
            else:
                catalog = None
                schema = None
                table = row["Name"]

            if self._connection_name:
                database = self._connection_name + "/" + row["DatabaseName"]
            else:
                database = row["DatabaseName"]

            yield TableMetadata(
                database,
                catalog,
                schema,
                table,
                row.get("Description") or row.get("Parameters", {}).get("comment"),
                columns,
                row.get("TableType") == "VIRTUAL_VIEW",
            )

    def _parse_location(self, location, name):

        """
        Location is formatted in glue as `catalog.schema.table`, while name
        is formatted as `catalog_schema_table`. To determine what the catalog,
        schema, and table are, then, (particularly in the case where catalogs,
        schemas, and tables can have underscores and/or periods), we need to
        find points where location has a `.`, while name has a `_`."""

        start_index = 0
        splits = []
        for end_index, (location_character, name_character) in enumerate(
            zip(location, name)
        ):
            if location_character == "." and name_character == "_":
                splits.append(location[start_index:end_index])
                start_index = end_index + 1
            elif end_index == len(location) - 1:
                splits.append(location[start_index:])

        table = splits[-1]
        schema = splits[-2]
        if len(splits) == 3:
            catalog = splits[-3]
        else:
            catalog = None

        return catalog, schema, table

    def _get_raw_extract_iter(self) -> Iterator[Dict[str, Any]]:
        tables = self._search_tables()
        return iter(tables)

    def _search_tables(self) -> List[Dict[str, Any]]:
        tables = []
        kwargs = {}
        if self._filters is not None:
            kwargs["Filters"] = self._filters
        data = self._glue.search_tables(**kwargs)
        tables += data["TableList"]
        while "NextToken" in data:
            token = data["NextToken"]
            kwargs["NextToken"] = token
            data = self._glue.search_tables(**kwargs)
            tables += data["TableList"]
        return tables
