import logging
import unittest

from mock import patch, MagicMock
from pyhocon import ConfigFactory

from whale.extractor.splice_machine_metadata_extractor import SpliceMachineMetadataExtractor
from whale.extractor import splice_machine_metadata_extractor
from whale.models.table_metadata import TableMetadata
from whale.models.column_metadata import ColumnMetadata


class TestSpliceMachineMetadataExtractor(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        self.Extractor = SpliceMachineMetadataExtractor

        self.DATABASE = "TEST_DATABASE"
        self.CLUSTER = None

        config_dict = {
            self.Extractor.HOST_KEY: "TEST_CONNECTION",
            self.Extractor.DATABASE_KEY: self.DATABASE,
            self.Extractor.CLUSTER_KEY: self.CLUSTER,
            self.Extractor.USERNAME_KEY: "TEST_USERNAME",
            self.Extractor.PASSWORD_KEY: "TEST_PASSWORD",
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_extraction_with_empty_result(self):
        with patch.object(splice_machine_metadata_extractor, "splice_connect"):
            extractor = self.Extractor()
            extractor.init(self.conf)
            results = extractor.extract()
            self.assertEqual(results, None)

    def test_extraction_with_single_result(self):
        with patch.object(splice_machine_metadata_extractor, "splice_connect") as mock_connect:
            column = ColumnMetadata("column1", None, "int", 0)
            table = TableMetadata(
                self.DATABASE,
                self.CLUSTER,
                "test_schema",
                "test_table",
                None,
                [column],
            )

            # Connection returns a cursor
            mock_cursor = MagicMock()
            mock_execute = MagicMock()
            mock_fetchall = MagicMock()

            # self.connection = splice_connect(...)
            mock_connection = MagicMock()
            mock_connect.return_value = mock_connection
            # self.cursor = self.connection.cursor()
            mock_connection.cursor.return_value = mock_cursor

            # self.cursor.execute(...)
            mock_cursor.execute = mock_execute

            # for row in self.cursor.fetchall()
            mock_cursor.fetchall = mock_fetchall

            mock_fetchall.return_value = [
                [
                    table.schema,
                    table.name,
                    "not-a-view",
                    column.name,
                    column.sort_order,
                    column.type,
                ]
            ]

            extractor = self.Extractor()
            extractor.init(self.conf)
            actual = extractor.extract()
            expected = table

            self.assertEqual(expected.__repr__(), actual.__repr__())
            self.assertIsNone(extractor.extract())
