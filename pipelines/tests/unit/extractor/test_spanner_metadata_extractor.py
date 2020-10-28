import logging
import unittest

from mock import patch, Mock
from pyhocon import ConfigFactory

from databuilder import Scoped
from whale.extractor.spanner_metadata_extractor import SpannerMetadataExtractor
from whale.models.table_metadata import TableMetadata

logging.basicConfig(level=logging.INFO)


NO_INSTANCES = {}
ONE_INSTANCE = {
    "instances": [
        {
            "name": "your-project-here/instances/your-instance",
            "config": "your-project-here/instanceConfigs/eur5",
            "displayName": "your-instance",
            "nodeCount": 1,
            "state": "READY",
        },
    ]
}
NO_DATABASES = {}
ONE_DATABASE = {
    "databases": [
        {
            "name": "projects/your-project-here/instances/your-instance/databases/your-database",
            "state": "READY",
            "createTime": "2020-10-27T09:48:37.955222Z",
        }
    ]
}
DATABASE_DATA_ONE_TABLE = {
    "statements": [
        "CREATE TABLE MyFirstTable (\n  col1 INT64,\n  col2 STRING,\n  col3 NUMERIC,\n) PRIMARY KEY(col1)",
    ]
}
DATABASE_DATA_TWO_TABLES = {
    "statements": [
        "CREATE TABLE MyFirstTable (\n  col1 INT64,\n  col2 STRING,\n  col3 NUMERIC,\n) PRIMARY KEY(col1)",
        "CREATE TABLE MySecondTable (\n  col1 FLOAT64,\n  col2 STRING,\n  col3 NUMERIC,\n) PRIMARY KEY(col1)",
    ]
}
DATABASE_ARRAY = {
    "statements": [
        "CREATE TABLE MyFirstTable (\n  col1 ARRAY<INT64>,\n  col2 STRING,\n  col3 NUMERIC,\n) PRIMARY KEY(col1)",
    ]
}
DATABASE_INDEX = {"statements": ["CREATE INDEX col1Lookup ON MyFirstTable(col1)"]}


try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


class MockSpannerClient:
    def __init__(self, instance_list_data, database_list_data, database_data):
        self.instance_list = Mock()
        self.instance_list.execute.return_value = instance_list_data

        self.database_list = Mock()
        self.database_list.execute.return_value = database_list_data

        self.database_getDdl = Mock()
        self.database_getDdl.execute.return_value = database_data

        self.instances_databases = Mock()
        self.instances_databases.list.return_value = self.database_list
        self.instances_databases.getDdl.return_value = self.database_getDdl

        self.instances_instances = Mock()
        self.instances_instances.list.return_value = self.instance_list
        self.instances_instances.databases.return_value = self.instances_databases

        self.instances = Mock()
        self.instances.instances.return_value = self.instances_instances

    def projects(self):
        return self.instances


class TestSpannerMetadataExtractor(unittest.TestCase):
    def setUp(self):
        config_dict = {
            "extractor.spanner_table_metadata.{}".format(
                SpannerMetadataExtractor.PROJECT_ID_KEY
            ): "your-project-here",
        }

        self.conf = ConfigFactory.from_dict(config_dict)

    @patch("whale.extractor.base_spanner_extractor.build")
    def test_can_handle_instances(self, mock_build):
        mock_build.return_value = MockSpannerClient(NO_INSTANCES, None, None)
        extractor = SpannerMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()
        self.assertIsNone(result)

    @patch("whale.extractor.base_spanner_extractor.build")
    def test_empty_instance(self, mock_build):
        mock_build.return_value = MockSpannerClient(ONE_INSTANCE, NO_DATABASES, None)
        extractor = SpannerMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()
        self.assertIsNone(result)

    @patch("whale.extractor.base_spanner_extractor.build")
    def test_one_table(self, mock_build):
        mock_build.return_value = MockSpannerClient(
            ONE_INSTANCE, ONE_DATABASE, DATABASE_DATA_ONE_TABLE
        )
        extractor = SpannerMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()

        self.assertEqual(result.database, "spanner")
        self.assertEqual(result.cluster, "your-project-here")
        self.assertEqual(result.schema, "your-instance.your-database")
        self.assertEqual(result.name, "MyFirstTable")
        self.assertEqual(result.description, None)

        first_col = result.columns[0]
        self.assertEqual(first_col.name, "col1")
        self.assertEqual(first_col.type, "INTEGER")
        self.assertEqual(first_col.description, "")

    @patch("whale.extractor.base_spanner_extractor.build")
    def test_two_tables(self, mock_build):
        mock_build.return_value = MockSpannerClient(
            ONE_INSTANCE, ONE_DATABASE, DATABASE_DATA_TWO_TABLES
        )
        extractor = SpannerMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result_1 = extractor.extract()
        result_2 = extractor.extract()
        self.assertEqual(result_2.database, "spanner")
        self.assertEqual(result_2.cluster, "your-project-here")
        self.assertEqual(result_2.schema, "your-instance.your-database")
        self.assertEqual(result_2.name, "MySecondTable")
        self.assertEqual(result_2.description, None)

        first_col = result_2.columns[0]
        self.assertEqual(first_col.name, "col1")
        self.assertEqual(first_col.type, "FLOAT")
        self.assertEqual(first_col.description, "")

    @patch("whale.extractor.base_spanner_extractor.build")
    def test_one_table_with_array(self, mock_build):
        mock_build.return_value = MockSpannerClient(
            ONE_INSTANCE, ONE_DATABASE, DATABASE_ARRAY
        )
        extractor = SpannerMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()
        self.assertIsNone(result)

    @patch("whale.extractor.base_spanner_extractor.build")
    def test_one_table_with_index(self, mock_build):
        mock_build.return_value = MockSpannerClient(
            ONE_INSTANCE, ONE_DATABASE, DATABASE_INDEX
        )
        extractor = SpannerMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()
        self.assertIsNone(result)
