import logging
import os
from collections import namedtuple
from jinja2 import Environment, FileSystemLoader

from pyhocon import ConfigFactory, ConfigTree
from typing import Any, Dict, Iterator, Optional

from databuilder.extractor.base_extractor import Extractor
from whale.models.table_metadata import TableMetadata
from whale.models.column_metadata import ColumnMetadata
from itertools import groupby
from splicemachinesa.pyodbc import splice_connect


TableKey = namedtuple("TableKey", ["schema_name", "table_name"])

LOGGER = logging.getLogger(__name__)


class SpliceMachineMetadataExtractor(Extractor):
    """
    Extracts SpliceMachine table and column metadata from underlying meta store
    database using SQLAlchemyExtractor.
    Requirements:
        snowflake-connector-python
        snowflake-sqlalchemy
    """

    WHERE_CLAUSE_SUFFIX_KEY = "where_clause_suffix"
    DATABASE_KEY = "database"
    CLUSTER_KEY = "cluster"
    USERNAME_KEY = "username"
    PASSWORD_KEY = "password"
    HOST_KEY = "host"

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            WHERE_CLAUSE_SUFFIX_KEY: "",
            DATABASE_KEY: "sm",
            CLUSTER_KEY: "master",
        }
    )

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf.with_fallback(SpliceMachineMetadataExtractor.DEFAULT_CONFIG)
        self._database = self.conf.get_string(
            SpliceMachineMetadataExtractor.DATABASE_KEY
        )
        self._cluster = self.conf.get_string(SpliceMachineMetadataExtractor.CLUSTER_KEY)
        self._where_clause_suffix = self.conf.get_string(
            SpliceMachineMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY
        )
        self._username = self.conf.get_string(
            SpliceMachineMetadataExtractor.USERNAME_KEY
        )
        self._password = self.conf.get_string(
            SpliceMachineMetadataExtractor.PASSWORD_KEY
        )
        self._host = self.conf.get_string(SpliceMachineMetadataExtractor.HOST_KEY)

        context = {
            "where_clause_suffix": self._where_clause_suffix,
        }

        j2_env = Environment(
            loader=FileSystemLoader(os.path.dirname(os.path.abspath(__file__))),
            trim_blocks=True,
        )
        self.sql_statement = j2_env.get_template(
            "splice_machine_metadata_extractor.sql"
        ).render(context)

        LOGGER.info("SQL for splicemachine: {}".format(self.sql_statement))
        self._extract_iter = None
        self.connection = splice_connect(self._username, self._password, self._host)
        self.cursor = self.connection.cursor()

    def extract(self) -> Optional[TableMetadata]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return "extractor.splicemachine"

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        """
        Using itertools.groupby and raw level iterator, it groups to table and
        yields TableMetadata
        :return:
        """

        for _, group in groupby(self._get_raw_extract_iter(), self._get_table_key):
            columns = []
            for row in group:
                last_row = row
                columns.append(
                    ColumnMetadata(
                        name=row["column_name"],
                        description=None,
                        data_type=row["column_type"],
                        sort_order=row["column_sort_order"],
                    )
                )

            yield TableMetadata(
                database=self._database,
                cluster=None,
                schema=last_row["schema_name"],
                name=last_row["table_name"],
                description=None,
                columns=columns,
                is_view=last_row["table_type"] == "V",
            )

    def _get_raw_extract_iter(self) -> Iterator[Dict[str, Any]]:
        """
        Provides iterator of result row from SQLAlchemy extractor
        :return:
        """
        self.cursor.execute(self.sql_statement)
        keys = [
            "schema_name",
            "table_name",
            "table_type",
            "column_name",
            "column_sort_order",
            "column_type",
        ]

        for row in self.cursor.fetchall():
            yield dict(zip(keys, row))

    def _get_table_key(self, row: Dict[str, Any]) -> Optional[TableKey]:
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            return TableKey(
                schema_name=row["schema_name"], table_name=row["table_name"]
            )

        return None
