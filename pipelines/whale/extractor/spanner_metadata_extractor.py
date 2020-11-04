import logging
from collections import namedtuple

from google.cloud import spanner
from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from typing import Iterator, Union, Dict, Any  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from whale.models.table_metadata import TableMetadata, ColumnMetadata
from itertools import groupby


TableKey = namedtuple("TableKey", ["schema", "table_name"])

LOGGER = logging.getLogger(__name__)


class SpannerMetadataExtractor(Extractor):
    """
    Extracts metadata from Google Cloud Spanner using the Spanner Python Client.
    """

    INSTANCE_ID_KEY = "instance_id"
    CONNECTION_NAME_KEY = "connection_name"
    DATABASE_ID_KEY = "database_id"
    KEY_PATH_KEY = "key_path"
    PROJECT_CREDENTIALS_KEY = "project_credentials"
    PROJECT_ID_KEY = "project_id"
    WHERE_CLAUSE_SUFFIX_KEY = "where_clause_suffix"

    SQL_STATEMENT = """
    SELECT
      lower(c.column_name) AS col_name,
      lower(c.spanner_type) AS col_type,
      c.ordinal_position AS col_sort_order,
      lower(c.table_schema) AS `schema`,
      lower(c.table_name) AS name
    FROM
      INFORMATION_SCHEMA.COLUMNS AS c
    LEFT JOIN
      INFORMATION_SCHEMA.TABLES t
          ON c.TABLE_NAME = t.TABLE_NAME
          AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
    {where_clause_suffix}
    ORDER by `schema`, name, col_sort_order ;
    """

    HEADER = ["col_name", "col_type", "col_sort_order", "schema", "name"]

    # Config keys.
    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            CONNECTION_NAME_KEY: "spanner",
            WHERE_CLAUSE_SUFFIX_KEY: "",
        }
    )

    def init(self, conf):
        self.conf = conf.with_fallback(SpannerMetadataExtractor.DEFAULT_CONFIG)
        self._project_id = self.conf.get_string(SpannerMetadataExtractor.PROJECT_ID_KEY)
        self._connection_name = self.conf.get_string(
            SpannerMetadataExtractor.CONNECTION_NAME_KEY
        )
        self._instance_id = self.conf.get_string(
            SpannerMetadataExtractor.INSTANCE_ID_KEY
        )
        self._database_id = self.conf.get(
            SpannerMetadataExtractor.DATABASE_ID_KEY, None
        )
        self._key_path = self.conf.get(SpannerMetadataExtractor.KEY_PATH_KEY, None)

        client_kwargs = {"project": self._project_id}
        if self._key_path is not None:
            spanner_client = spanner.Client.from_service_account_json(
                self._key_path, **client_kwargs
            )
        else:
            spanner_client = spanner.Client(**client_kwargs)

        self.instance = spanner_client.instance(self._instance_id)
        self.database = self.instance.database(self._database_id)

        self.sql_stmt = SpannerMetadataExtractor.SQL_STATEMENT.format(
            where_clause_suffix=self.conf.get_string(
                SpannerMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY
            )
        )

        LOGGER.info("SQL for Spanner: {}".format(self.sql_stmt))

        self._extract_iter = None  # type: Union[None, Iterator]

    def extract(self):
        # type: () -> Union[TableMetadata, None]
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        # type: () -> str
        return "extractor.spanner_table_metadata"

    def _get_extract_iter(self):
        # type: () -> Iterator[TableMetadata]
        """
        Using itertools.groupby and raw level iterator, it groups to table and yields TableMetadata
        :return:
        """

        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(self.sql_stmt)
            header = SpannerMetadataExtractor.HEADER
            headered_results = [dict(zip(header, result)) for result in results]
            schema = "{}.{}".format(self._instance_id, self._database_id)

            for _, group in groupby(headered_results, self._get_table_key):
                columns = []

                for row in group:
                    last_row = row
                    columns.append(
                        ColumnMetadata(
                            row["col_name"],
                            None,
                            row["col_type"],
                            row["col_sort_order"],
                        )
                    )

                yield TableMetadata(
                    database=self._connection_name or "spanner",
                    cluster=self._project_id,
                    schema=schema,
                    name=last_row["name"],
                    description=None,
                    columns=columns,
                )

    def _get_table_key(self, row):
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            return TableKey(schema=row["schema"], table_name=row["name"])
        return None
