import logging
from pyhocon import ConfigFactory
from typing import Dict, Iterable, Iterator, List, Optional  # noqa: F401

from whale.engine.sql_alchemy_engine import SQLAlchemyEngine
from whale.engine.mixins.presto_commands_mixin import PrestoCommandsMixin

LOGGER = logging.getLogger(__name__)


class PrestoAlchemyEngine(PrestoCommandsMixin, SQLAlchemyEngine):
    """
    Create a Presto-specific engine with methods that run custom queries.
    """

    CONN_STRING_KEY = "conn_string"
    DEFAULT_CLUSTER_NAME_KEY = "default_cluster_name"
    DATABASE_KEY = "database"

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            CONN_STRING_KEY: None,
            # Give a non-catalog-like default if none is given
            DEFAULT_CLUSTER_NAME_KEY: "<default>",
            DATABASE_KEY: "presto",
        }
    )

    def init(self, conf):
        sql_alchemy_conf = ConfigFactory.from_dict(
            {
                SQLAlchemyEngine.CONN_STRING_KEY: conf.get_string(
                    PrestoAlchemyEngine.CONN_STRING_KEY
                )
            }
        )
        super().init(sql_alchemy_conf)
        self.conf = conf.with_fallback(PrestoAlchemyEngine.DEFAULT_CONFIG)
        self._default_cluster_name = self.conf.get_string(
            PrestoAlchemyEngine.DEFAULT_CLUSTER_NAME_KEY
        )
        self._database = self.conf.get_string(PrestoAlchemyEngine.DATABASE_KEY)
        self._extract_iter = None

    def get_scope(self) -> str:
        return "engine.presto"


class PrestoEngine(PrestoCommandsMixin, SQLAlchemyEngine):
    """
    Create a Presto-specific engine with methods that run custom queries.
    """

    CONN_STRING_KEY = "conn_string"
    DEFAULT_CLUSTER_NAME_KEY = "default_cluster_name"
    DATABASE_KEY = "database"

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            CONN_STRING_KEY: None,
            DEFAULT_CLUSTER_NAME_KEY: "<default>",
            DATABASE_KEY: "presto",
        }
    )

    def init(self, conf):
        sql_alchemy_conf = ConfigFactory.from_dict(
            {
                SQLAlchemyEngine.CONN_STRING_KEY: conf.get_string(
                    PrestoEngine.CONN_STRING_KEY
                )
            }
        )
        super().init(sql_alchemy_conf)
        self.conf = conf.with_fallback(PrestoEngine.DEFAULT_CONFIG)
        self._default_cluster_name = self.conf.get_string(
            PrestoEngine.DEFAULT_CLUSTER_NAME_KEY
        )
        self._database = self.conf.get_string(PrestoEngine.DATABASE_KEY)
        self._extract_iter = None

    def get_scope(self) -> str:
        return "engine.presto"
