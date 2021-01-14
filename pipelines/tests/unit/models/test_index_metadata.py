import logging
import unittest

from whale.models.index_metadata import IndexMetadata
from whale.models.table_metadata import ColumnMetadata


class TestIndexMetadata(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)

    def test_index_without_any_type(self):
        index_metadata = IndexMetadata(
            database="test_database",
            cluster="test_cluster",
            schema="test_schema",
            table="test_table",
            name="test_index",
            columns=["test_column_1", "test_column_2"],
        )

        expected = "* [] `test_index` [`test_column_1`, `test_column_2`]"

        self.assertEqual(index_metadata.format_for_markdown(), expected)

    def test_index_with_type(self):
        index_metadata = IndexMetadata(
            database="test_database",
            cluster="test_cluster",
            schema="test_schema",
            table="test_table",
            name="test_index",
            columns=["test_column_1", "test_column_2"],
            index_type="primary",
            architecture="clustered",
            constraint="unique",
        )

        expected = "* [primary, unique, clustered] `test_index` [`test_column_1`, `test_column_2`]"

        self.assertEqual(index_metadata.format_for_markdown(), expected)
