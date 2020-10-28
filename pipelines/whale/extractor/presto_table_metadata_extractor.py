import logging
from collections import namedtuple

from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from typing import Iterator, Union, Dict, Any  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata
from itertools import groupby


TableKey = namedtuple("TableKey", ["schema", "table_name"])

LOGGER = logging.getLogger(__name__)


class PrestoTableMetadataExtractor(Extractor):
    """
    Extracts Hive table and column metadata from underlying meta store database using SQLAlchemyExtractor
    """

    WHERE_CLAUSE_SUFFIX_KEY = "where_clause_suffix"
    CLUSTER_KEY = "cluster"
    DATABASE_KEY = "database"

    SQL_STATEMENT = """
    SELECT
      a.table_catalog AS catalog
      , a.table_schema AS schema
      , a.table_name AS name
      , NULL AS description
      , a.column_name AS col_name
      , a.ordinal_position as col_sort_order
      , IF(a.extra_info = 'partition key', 1, 0) AS is_partition_col
      , a.comment AS col_description
      , a.data_type AS col_type
      , IF(b.table_name is not null, 1, 0) AS is_view
    FROM {cluster_prefix}information_schema.columns a
    LEFT JOIN {cluster_prefix}information_schema.views b ON a.table_catalog = b.table_catalog
        and a.table_schema = b.table_schema
        and a.table_name = b.table_name
    {where_clause_suffix}
    """

    # Config keys.
    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            WHERE_CLAUSE_SUFFIX_KEY: "WHERE a.table_schema NOT IN ('pg_catalog', 'information_schema')",
            DATABASE_KEY: "presto",
        }
    )

    def init(self, conf):
        # type: (ConfigTree) -> None
        self.conf = conf.with_fallback(PrestoTableMetadataExtractor.DEFAULT_CONFIG)
        self._database = "{}".format(
            self.conf.get_string(PrestoTableMetadataExtractor.DATABASE_KEY)
        )
        self._cluster = self.conf.get(PrestoTableMetadataExtractor.CLUSTER_KEY, None)
        LOGGER.info("Cluster name: {}".format(self._cluster))

        if self._cluster is not None:
            cluster_prefix = self._cluster + "."
        else:
            cluster_prefix = ""

        self.sql_stmt = PrestoTableMetadataExtractor.SQL_STATEMENT.format(
            cluster_prefix=cluster_prefix,
            where_clause_suffix=self.conf.get_string(
                PrestoTableMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY
            )
            or "",
        )

        LOGGER.info("SQL for presto: {}".format(self.sql_stmt))

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(
            self.conf, self._alchemy_extractor.get_scope()
        ).with_fallback(
            ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt})
        )

        self._alchemy_extractor.init(sql_alch_conf)
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
        return "extractor.presto_table_metadata"

    def _get_extract_iter(self):
        # type: () -> Iterator[TableMetadata]
        """
        Using itertools.groupby and raw level iterator, it groups to table and yields TableMetadata
        :return:
        """
        for _, group in groupby(self._get_raw_extract_iter(), self._get_table_key):
            columns = []

            for row in group:
                last_row = row
                columns.append(
                    ColumnMetadata(
                        row["col_name"],
                        row["col_description"],
                        row["col_type"],
                        row["col_sort_order"],
                    )
                )

            yield TableMetadata(
                self._database,
                self._cluster,
                last_row["schema"],
                last_row["name"],
                last_row["description"],
                columns,
                is_view=bool(last_row["is_view"]),
            )

    def _get_raw_extract_iter(self):
        # type: () -> Iterator[Dict[str, Any]]
        """
        Provides iterator of result row from SQLAlchemy extractor
        :return:
        """
        row = self._alchemy_extractor.extract()
        while row:
            yield row
            row = self._alchemy_extractor.extract()

    def _get_table_key(self, row):
        # type: (Dict[str, Any]) -> Union[TableKey, None]
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            return TableKey(schema=row["schema"], table_name=row["name"])
        return None
