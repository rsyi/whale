import logging
from collections import namedtuple

from pyhocon import ConfigFactory, ConfigTree
from typing import Any, Dict, Iterator, Optional
from unidecode import unidecode

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from whale.models.table_metadata import TableMetadata
from whale.models.column_metadata import ColumnMetadata
from itertools import groupby


TableKey = namedtuple("TableKey", ["schema", "table_name"])

LOGGER = logging.getLogger(__name__)


class SnowflakeMetadataExtractor(Extractor):
    """
    Extracts Snowflake table and column metadata from underlying meta store
    database using SQLAlchemyExtractor.
    Requirements:
        snowflake-connector-python
        snowflake-sqlalchemy
    """

    SQL_STATEMENT = """
    SELECT
        lower(c.column_name) AS col_name,
        c.comment AS col_description,
        lower(c.data_type) AS data_type,
        lower(c.ordinal_position) AS col_sort_order,
        lower('{database}') AS database,
        lower(c.table_catalog) AS cluster,
        lower(c.table_schema) AS schema,
        lower(c.table_name) AS name,
        t.comment AS description,
        decode(lower(t.table_type), 'view', 'true', 'false') AS is_view
    FROM
        {cluster}.INFORMATION_SCHEMA.COLUMNS AS c
    LEFT JOIN
        {cluster}.INFORMATION_SCHEMA.TABLES t
            ON c.TABLE_NAME = t.TABLE_NAME
            AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
    {where_clause_suffix};
    """

    WHERE_CLAUSE_SUFFIX_KEY = "where_clause_suffix"
    DATABASE_KEY = "database"
    CLUSTER_KEY = "cluster"

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            WHERE_CLAUSE_SUFFIX_KEY: "",
            DATABASE_KEY: "snowflake",
            CLUSTER_KEY: "master",
        }
    )

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf.with_fallback(SnowflakeMetadataExtractor.DEFAULT_CONFIG)
        self._database = self.conf.get_string(SnowflakeMetadataExtractor.DATABASE_KEY)
        self._cluster = "{}".format(
            self.conf.get_string(SnowflakeMetadataExtractor.CLUSTER_KEY)
        )

        self.sql_stmt = SnowflakeMetadataExtractor.SQL_STATEMENT.format(
            where_clause_suffix=self.conf.get_string("where_clause_suffix"),
            cluster=self._cluster,
            database=self._database,
        )

        LOGGER.info("SQL for snowflake: {}".format(self.sql_stmt))

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alchemy_scope = self._alchemy_extractor.get_scope()
        sql_alchemy_conf = Scoped.get_scoped_conf(conf, sql_alchemy_scope)
        sql_alchemy_conf.put(SQLAlchemyExtractor.EXTRACT_SQL, self.sql_stmt)

        self._alchemy_extractor.init(sql_alchemy_conf)
        self._extract_iter = None

    def extract(self) -> Optional[TableMetadata]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return "extractor.snowflake"

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        """
        Using itertools.groupby and raw level iterator, it groups to table and
        yields TableMetadata
        :return:
        """
        for _, group in groupby(self._get_raw_extract_iter(), self._get_table_key):
            columns = []
            for row in group:
                column_description = (
                    unidecode(row["col_description"])
                    if row["col_description"]
                    else None
                )
                last_row = row
                columns.append(
                    ColumnMetadata(
                        name=row["col_name"],
                        description=column_description,
                        data_type=row["data_type"],
                        sort_order=row["col_sort_order"],
                    )
                )

            description = (
                unidecode(last_row["description"]) if last_row["description"] else None
            )

            yield TableMetadata(
                database=self._database,
                cluster=last_row["cluster"],
                schema=last_row["schema"],
                name=last_row["name"],
                description=description,
                columns=columns,
                is_view=last_row["is_view"] == "true",
            )

    def _get_raw_extract_iter(self) -> Iterator[Dict[str, Any]]:
        """
        Provides iterator of result row from SQLAlchemy extractor
        :return:
        """
        row = self._alchemy_extractor.extract()
        while row:
            yield row
            row = self._alchemy_extractor.extract()

    def _get_table_key(self, row: Dict[str, Any]) -> Optional[TableKey]:
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            return TableKey(schema=row["schema"], table_name=row["name"])

        return None
