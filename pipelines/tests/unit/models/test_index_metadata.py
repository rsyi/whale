import logging
import unittest

from whale.models.index_metadata import TableIndexesMetadata, IndexMetadata
from whale.models.table_metadata import ColumnMetadata


class TestTableIndexesMetadata(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)

    def test_single_index_without_any_type(self):
        index_metadata = IndexMetadata(
            name="test_index",
            columns=["test_column_1", "test_column_2"],
        )

        table_indexes_metadata = TableIndexesMetadata(
            database="test_database",
            cluster="test_cluster",
            schema="test_schema",
            table="test_table",
            indexes=[index_metadata],
        )

        expected = "* [] `test_index` [`test_column_1`, `test_column_2`]"

        self.assertEqual(table_indexes_metadata.format_for_markdown(), expected)

    def test_single_index_with_type(self):
        index_metadata = IndexMetadata(
            name="test_index",
            columns=["test_column_1", "test_column_2"],
            index_type="primary",
            architecture="clustered",
            constraint="unique",
        )

        table_indexes_metadata = TableIndexesMetadata(
            database="test_database",
            cluster="test_cluster",
            schema="test_schema",
            table="test_table",
            indexes=[index_metadata],
        )

        expected = "* [primary, unique, clustered] `test_index` [`test_column_1`, `test_column_2`]"

        self.assertEqual(table_indexes_metadata.format_for_markdown(), expected)

    def test_multiple_indexes(self):
        index_metadata_without_type = IndexMetadata(
            name="test_index",
            columns=["test_column_1", "test_column_2"],
        )

        index_metadata_with_type = IndexMetadata(
            name="test_index",
            columns=["test_column_1", "test_column_2"],
            index_type="primary",
            architecture="clustered",
            constraint="unique",
        )

        table_indexes_metadata = TableIndexesMetadata(
            database="test_database",
            cluster="test_cluster",
            schema="test_schema",
            table="test_table",
            indexes=[index_metadata_without_type, index_metadata_with_type],
        )

        expected_list = [
            "* [] `test_index` [`test_column_1`, `test_column_2`]",
            "* [primary, unique, clustered] `test_index` [`test_column_1`, `test_column_2`]",
        ]

        self.assertEqual(table_indexes_metadata.format_for_markdown(), "\n".join(expected_list))


class TestIndexMetadata(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)

    def test_index_without_any_type(self):
        index_metadata = IndexMetadata(
            name="test_index",
            columns=["test_column_1", "test_column_2"],
        )

        expected = "* [] `test_index` [`test_column_1`, `test_column_2`]"

        self.assertEqual(index_metadata.format_for_markdown(), expected)

    def test_index_with_type(self):
        index_metadata = IndexMetadata(
            name="test_index",
            columns=["test_column_1", "test_column_2"],
            index_type="primary",
            architecture="clustered",
            constraint="unique",
        )

        expected = "* [primary, unique, clustered] `test_index` [`test_column_1`, `test_column_2`]"

        self.assertEqual(index_metadata.format_for_markdown(), expected)
