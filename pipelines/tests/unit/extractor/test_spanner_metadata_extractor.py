import logging
import unittest

from mock import patch, Mock, MagicMock
import mock
from pyhocon import ConfigFactory

from databuilder import Scoped
from whale.extractor.spanner_metadata_extractor import SpannerMetadataExtractor
from whale.extractor.spanner_metadata_extractor import spanner
from whale.models.table_metadata import TableMetadata
from whale.models.column_metadata import ColumnMetadata

logging.basicConfig(level=logging.INFO)


class TestSpannerMetadataExtractor(unittest.TestCase):
    def setUp(self):
        Extractor = SpannerMetadataExtractor
        scope = SpannerMetadataExtractor().get_scope()
        self.connection_name = "MOCK_CONNECTION_NAME"
        self.project_id = "MOCK_PROJECT"
        self.instance_id = "MOCK_INSTANCE"
        self.database_id = "MOCK_DATABASE"
        self.schema = "MOCK_SCHEMA"
        self.table = "MOCK_TABLE"
        config_dict = {
            f"{scope}.{Extractor.CONNECTION_NAME_KEY}": self.connection_name,
            f"{scope}.{Extractor.PROJECT_ID_KEY}": self.project_id,
            f"{scope}.{Extractor.INSTANCE_ID_KEY}": self.instance_id,
            f"{scope}.{Extractor.DATABASE_ID_KEY}": self.database_id,
        }

        self.conf = ConfigFactory.from_dict(config_dict)

    @patch("whale.extractor.spanner_metadata_extractor.spanner.Client", autospec=True)
    def test_empty_result(self, mock_client):
        extractor = SpannerMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()
        self.assertIsNone(result)

    @patch("whale.extractor.spanner_metadata_extractor.spanner.Client", autospec=True)
    def test_multiple_results(self, mock_client):
        col1_name = "col1"
        col2_name = "col2"
        col1_type = "int"
        col2_type = "char"
        col1_sort_order = "1"
        col2_sort_order = "2"
        mock_client.return_value.instance.return_value.database.return_value.snapshot.return_value.__enter__.return_value.execute_sql.return_value = [
            [col1_name, col1_type, col1_sort_order, self.schema, self.table],
            [col2_name, col2_type, col2_sort_order, self.schema, self.table],
        ]

        extractor = SpannerMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()
        assert result.database == self.connection_name
        assert result.cluster == self.project_id
        assert result.schema == f"{self.instance_id}.{self.database_id}"
        assert result.name == self.table
        self.assertEqual(
            result.columns[0].__repr__(),
            ColumnMetadata(
                col1_name, None, col1_type, col1_sort_order, None
            ).__repr__(),
        )
        self.assertEqual(
            result.columns[1].__repr__(),
            ColumnMetadata(
                col2_name, None, col2_type, col2_sort_order, None
            ).__repr__(),
        )
