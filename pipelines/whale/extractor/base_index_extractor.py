import logging
import os
from datetime import datetime
from pyhocon import ConfigFactory
import subprocess
import yaml
from databuilder import Scoped
from typing import Iterator, Union, Dict, Any
from itertools import groupby

from whale.utils.paths import METADATA_PATH
from whale.utils import get_table_info_from_path
from whale.utils.sql import template_query
from whale.utils.parsers import (
    parse_ugc,
    sections_from_markdown,
    DEFINED_METRICS_SECTION,
)
from whale.engine.sql_alchemy_engine import SQLAlchemyEngine
from whale.models.index_metadata import IndexMetadata

from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.extractor.base_extractor import Extractor

SQLALCHEMY_ENGINE_SCOPE = SQLAlchemyEngine().get_scope()
SQLALCHEMY_CONN_STRING_KEY = (
    f"{SQLALCHEMY_ENGINE_SCOPE}.{SQLAlchemyEngine.CONN_STRING_KEY}"
)


LOGGER = logging.getLogger(__name__)


class IndexExtractor(Extractor):
    """Base Index Extractor
    SQL language-specific classes should inherit from this class, but
    this class should not be called by itself.
    """

    # CONFIG KEYS
    WHERE_CLAUSE_SUFFIX_KEY = "where_clause_suffix"
    CLUSTER_KEY = "cluster_key"
    USE_CATALOG_AS_CLUSTER_NAME = "use_catalog_as_cluster_name"
    DATABASE_KEY = "database_key"
    CONN_STRING_KEY = "conn_string"

    # Default values
    DEFAULT_CLUSTER_NAME = "master"

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            WHERE_CLAUSE_SUFFIX_KEY: "",
            CLUSTER_KEY: DEFAULT_CLUSTER_NAME,
            USE_CATALOG_AS_CLUSTER_NAME: True,
            SQLALCHEMY_CONN_STRING_KEY: None,
        }
    )

    def init(self, conf):
        conf = conf.with_fallback(self.DEFAULT_CONFIG)

        self._cluster = "{}".format(conf.get_string(self.CLUSTER_KEY))

        self._database = conf.get_string(self.DATABASE_KEY)

        self.sql_stmt = self._get_sql_statement(
            use_catalog_as_cluster_name=conf.get_bool(self.USE_CATALOG_AS_CLUSTER_NAME),
            where_clause_suffix=conf.get_string(self.WHERE_CLAUSE_SUFFIX_KEY),
        )

        self._alchemy_extractor = SQLAlchemyExtractor()

        sql_alch_conf = Scoped.get_scoped_conf(
            conf, SQLALCHEMY_ENGINE_SCOPE
        ).with_fallback(
            ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt})
        )

        self.sql_stmt = sql_alch_conf.get_string(SQLAlchemyExtractor.EXTRACT_SQL)

        LOGGER.info("SQL for postgres metadata: %s", self.sql_stmt)

        self._alchemy_extractor.init(sql_alch_conf)
        self._extract_iter: Union[None, iterator] = None

    def extract(self):
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            extraction = next(self._extract_iter)
            return extraction
        except StopIteration:
            return None

    def _get_raw_extract_iter(self) -> Iterator[Dict[str, Any]]:
        """
        Provides iterator of result row from SQLAlchemy extractor
        :return:
        """
        row = self._alchemy_extractor.extract()
        while row:
            yield row
            row = self._alchemy_extractor.extract()

    def get_scope(self):
        return "extractor.markdown_index"
