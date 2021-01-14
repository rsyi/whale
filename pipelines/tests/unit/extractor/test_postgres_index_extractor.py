import logging
import unittest
from typing import Any, Dict
from mock import MagicMock, patch
from pyhocon import ConfigFactory

from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor

from whale.extractor.postgres_index_extractor import PostgresIndexExtractor
from whale.models.index_metadata import IndexMetadata


class TestPostgresIndexExtractor(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)

        config_dict = {
            f"extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}": "TEST_CONNECTION",
            PostgresIndexExtractor.CLUSTER_KEY: "MY_CLUSTER",
            PostgresIndexExtractor.USE_CATALOG_AS_CLUSTER_NAME: False,
            PostgresIndexExtractor.DATABASE_KEY: "postgres",
        }

        self.conf = ConfigFactory.from_dict(config_dict)

    def test_extraction_with_empty_query_result(self) -> None:
        with patch.object(SQLAlchemyExtractor, "_get_connection"):
            extractor = PostgresIndexExtractor()
            extractor.init(self.conf)

            results = extractor.extract()
            self.assertEqual(results, None)

    def test_extraction_with_single_result(self) -> None:
        with patch.object(SQLAlchemyExtractor, "_get_connection") as mock_connection:
            connection = MagicMock()
            mock_connection.return_value = connection
            sql_execute = MagicMock()
            connection.execute = sql_execute

            table = {
                "cluster": self.conf[PostgresIndexExtractor.CLUSTER_KEY],
                "schema": "test_schema",
                "table": "test_table",
            }

            sql_execute.return_value = [
                self._union(
                    {
                        "index_name": "idx_1",
                        "is_primary": False,
                        "is_clustered": False,
                        "is_unique": True,
                        "column_name": "idx_column_1",
                    },
                    table,
                ),
                self._union(
                    {
                        "index_name": "idx_1",
                        "is_primary": False,
                        "is_clustered": False,
                        "is_unique": True,
                        "column_name": "idx_column_2",
                    },
                    table,
                ),
            ]

            extractor = PostgresIndexExtractor()
            extractor.init(self.conf)
            actual = extractor.extract()

            expected = IndexMetadata(
                database="postgres",
                cluster="MY_CLUSTER",
                schema="test_schema",
                table="test_table",
                name="idx_1",
                description=None,
                index_type=None,
                architecture=None,
                constraint="unique",
                columns=["idx_column_1", "idx_column_2"],
                tags=None,
            )

            self.assertEqual(expected.__repr__(), actual.__repr__())
            self.assertIsNone(extractor.extract())

    def test_extraction_with_multiple_result(self) -> None:
        with patch.object(SQLAlchemyExtractor, "_get_connection") as mock_connection:
            connection = MagicMock()
            mock_connection.return_value = connection
            sql_execute = MagicMock()
            connection.execute = sql_execute

            table_1 = {
                "cluster": self.conf[PostgresIndexExtractor.CLUSTER_KEY],
                "schema": "test_schema_1",
                "table": "test_table_1",
            }

            table_2 = {
                "cluster": self.conf[PostgresIndexExtractor.CLUSTER_KEY],
                "schema": "test_schema_2",
                "table": "test_table_2",
            }

            sql_execute.return_value = [
                self._union(
                    {
                        "index_name": "idx_1",
                        "is_primary": False,
                        "is_clustered": False,
                        "is_unique": True,
                        "column_name": "idx_1_column_1",
                    },
                    table_1,
                ),
                self._union(
                    {
                        "index_name": "idx_1",
                        "is_primary": False,
                        "is_clustered": False,
                        "is_unique": True,
                        "column_name": "idx_1_column_2",
                    },
                    table_1,
                ),
                self._union(
                    {
                        "index_name": "idx_2",
                        "is_primary": False,
                        "is_clustered": False,
                        "is_unique": True,
                        "column_name": "idx_2_column_1",
                    },
                    table_1,
                ),
                self._union(
                    {
                        "index_name": "idx_3",
                        "is_primary": False,
                        "is_clustered": False,
                        "is_unique": True,
                        "column_name": "idx_3_column_1",
                    },
                    table_2,
                ),
                self._union(
                    {
                        "index_name": "idx_3",
                        "is_primary": False,
                        "is_clustered": False,
                        "is_unique": True,
                        "column_name": "idx_3_column_2",
                    },
                    table_2,
                ),
            ]

            extractor = PostgresIndexExtractor()
            extractor.init(self.conf)

            expected = IndexMetadata(
                database="postgres",
                cluster="MY_CLUSTER",
                schema="test_schema_1",
                table="test_table_1",
                name="idx_1",
                description=None,
                index_type=None,
                architecture=None,
                constraint="unique",
                columns=["idx_1_column_1", "idx_1_column_2"],
                tags=None,
            )

            self.assertEqual(expected.__repr__(), extractor.extract().__repr__())

            expected = IndexMetadata(
                database="postgres",
                cluster="MY_CLUSTER",
                schema="test_schema_1",
                table="test_table_1",
                name="idx_2",
                description=None,
                index_type=None,
                architecture=None,
                constraint="unique",
                columns=["idx_2_column_1"],
                tags=None,
            )

            self.assertEqual(expected.__repr__(), extractor.extract().__repr__())

            expected = IndexMetadata(
                database="postgres",
                cluster="MY_CLUSTER",
                schema="test_schema_2",
                table="test_table_2",
                name="idx_3",
                description=None,
                index_type=None,
                architecture=None,
                constraint="unique",
                columns=["idx_3_column_1", "idx_3_column_2"],
                tags=None,
            )

            self.assertEqual(expected.__repr__(), extractor.extract().__repr__())
            self.assertIsNone(extractor.extract())

    def _union(self, target: Dict[Any, Any], extra: Dict[Any, Any]) -> Dict[Any, Any]:
        target.update(extra)
        return target


class TestPostgresIndexExtractorWithWhereClause(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)
        self.where_clause_suffix = """
        where table_schema in ('public') and table_name = 'movies'
        """

        config_dict = {
            PostgresIndexExtractor.WHERE_CLAUSE_SUFFIX_KEY: self.where_clause_suffix,
            PostgresIndexExtractor.DATABASE_KEY: "postgres",
            f"extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}": "TEST_CONNECTION",
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_sql_statement(self) -> None:
        """
        Test Extraction with empty result from query
        """
        with patch.object(SQLAlchemyExtractor, "_get_connection"):
            extractor = PostgresIndexExtractor()
            extractor.init(self.conf)
            self.assertTrue(self.where_clause_suffix in extractor.sql_stmt)


class TestPostgresIndexExtractorClusterKeyNoTableCatalog(unittest.TestCase):
    # test when USE_CATALOG_AS_CLUSTER_NAME is false and CLUSTER_KEY is specified
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)
        self.cluster_key = "not_master"

        config_dict = {
            PostgresIndexExtractor.CLUSTER_KEY: self.cluster_key,
            PostgresIndexExtractor.DATABASE_KEY: "postgres",
            f"extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}": "TEST_CONNECTION",
            PostgresIndexExtractor.USE_CATALOG_AS_CLUSTER_NAME: False,
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_sql_statement(self) -> None:
        """
        Test Extraction with empty result from query
        """
        with patch.object(SQLAlchemyExtractor, "_get_connection"):
            extractor = PostgresIndexExtractor()
            extractor.init(self.conf)
            self.assertTrue(self.cluster_key in extractor.sql_stmt)


class TestPostgresIndexExtractorNoClusterKeyNoTableCatalog(unittest.TestCase):
    # test when USE_CATALOG_AS_CLUSTER_NAME is false and CLUSTER_KEY is NOT specified
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)

        config_dict = {
            f"extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}": "TEST_CONNECTION",
            PostgresIndexExtractor.DATABASE_KEY: "postgres",
            PostgresIndexExtractor.USE_CATALOG_AS_CLUSTER_NAME: False,
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_sql_statement(self) -> None:
        """
        Test Extraction with empty result from query
        """
        with patch.object(SQLAlchemyExtractor, "_get_connection"):
            extractor = PostgresIndexExtractor()
            extractor.init(self.conf)
            self.assertTrue(
                PostgresIndexExtractor.DEFAULT_CLUSTER_NAME in extractor.sql_stmt
            )


class TestPostgresIndexExtractorTableCatalogEnabled(unittest.TestCase):
    # test when USE_CATALOG_AS_CLUSTER_NAME is true (CLUSTER_KEY should be ignored)
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)
        self.cluster_key = "not_master"

        config_dict = {
            PostgresIndexExtractor.CLUSTER_KEY: self.cluster_key,
            PostgresIndexExtractor.DATABASE_KEY: "postgres",
            f"extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}": "TEST_CONNECTION",
            PostgresIndexExtractor.USE_CATALOG_AS_CLUSTER_NAME: True,
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_sql_statement(self) -> None:
        """
        Test Extraction with empty result from query
        """
        with patch.object(SQLAlchemyExtractor, "_get_connection"):
            extractor = PostgresIndexExtractor()
            extractor.init(self.conf)
            self.assertTrue("table_catalog" in extractor.sql_stmt)
            self.assertFalse(self.cluster_key in extractor.sql_stmt)


if __name__ == "__main__":
    unittest.main()
