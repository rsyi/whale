import logging
import unittest

from mock import patch, MagicMock
from pyhocon import ConfigFactory

from metaframe.engine.presto_engine import PrestoEngine
from metaframe.engine.sql_alchemy_engine import SQLAlchemyEngine
from metaframe.models.table_metadata import ColumnMetadata, TableMetadata

MOCK_CONNECTION_NAME = 'TEST_CONNECTION'
MOCK_DATABASE_NAME = 'mock_database'
MOCK_CATALOG_NAME = 'mock_catalog'
MOCK_SCHEMA_NAME = 'mock_schema'
MOCK_TABLE_NAME = 'mock_table'
MOCK_COLUMN_RESULT = ('ds', 'varchar(64)', 'partition key', '')
MOCK_COLUMN_RESULT = ('ds', 'varchar(64)', 'partition key', '')
MOCK_INFORMATION_SCHEMA_RESULT_1 = {
    'catalog': MOCK_CATALOG_NAME,
    'schema': MOCK_SCHEMA_NAME,
    'name': MOCK_TABLE_NAME,
    'description': None,
    'col_name': 'id',
    'col_sort_order': 0,
    'is_partition_col': 0,
    'col_description': 'unique id',
    'col_type': 'varchar(64)',
    'is_view': '0',
}
MOCK_INFORMATION_SCHEMA_RESULT_2 = {
    'catalog': MOCK_CATALOG_NAME,
    'schema': MOCK_SCHEMA_NAME,
    'name': MOCK_TABLE_NAME,
    'description': None,
    'col_name': 'ds',
    'col_sort_order': '1',
    'is_partition_col': '1',
    'col_description': 'datestamp',
    'col_type': 'varchar(64)',
    'is_view': '0',
}


def presto_engine_execute_side_effect(query, **kwargs):
    query = query.lower()
    if query == 'show schemas':
        yield (MOCK_SCHEMA_NAME,)
    elif query == 'show tables in {}' \
            .format(MOCK_SCHEMA_NAME):
        yield (MOCK_TABLE_NAME,)
    elif query == 'show columns in {}.{}' \
            .format(MOCK_SCHEMA_NAME, MOCK_TABLE_NAME):
        yield ['Column', 'Type', 'Extra', 'Comment']
        yield MOCK_COLUMN_RESULT
    elif 'information_schema' in query:
        yield MOCK_INFORMATION_SCHEMA_RESULT_1
        yield MOCK_INFORMATION_SCHEMA_RESULT_2

@patch.object(SQLAlchemyEngine, '_get_connection')
class TestPrestoEngine(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)

        config_dict = {
            PrestoEngine.CONN_STRING_KEY: MOCK_CONNECTION_NAME,
            PrestoEngine.DATABASE_KEY: MOCK_DATABASE_NAME,
            PrestoEngine.DEFAULT_CATALOG_NAME_KEY: MOCK_CATALOG_NAME
        }
        self.conf = ConfigFactory.from_dict(config_dict)
        self.engine = PrestoEngine()

    def test_get_all_table_metadata_from_information_schema(
            self, mock_settings) -> None:
        self.engine.init(self.conf)
        self.engine.execute = MagicMock(
            side_effect=presto_engine_execute_side_effect
        )

        expected = TableMetadata(
                database=MOCK_DATABASE_NAME,
                catalog=MOCK_CATALOG_NAME,
                schema=MOCK_SCHEMA_NAME,
                name=MOCK_TABLE_NAME,
                columns=[
                    ColumnMetadata(
                        name=MOCK_INFORMATION_SCHEMA_RESULT_1['col_name'],
                        description=MOCK_INFORMATION_SCHEMA_RESULT_1['col_description'],
                        col_type=MOCK_INFORMATION_SCHEMA_RESULT_1['col_type'],
                        sort_order=MOCK_INFORMATION_SCHEMA_RESULT_1['col_sort_order'],
                        is_partition_column=None
                    ),
                    ColumnMetadata(
                        name=MOCK_INFORMATION_SCHEMA_RESULT_2['col_name'],
                        description=MOCK_INFORMATION_SCHEMA_RESULT_2['col_description'],
                        col_type=MOCK_INFORMATION_SCHEMA_RESULT_2['col_type'],
                        sort_order=MOCK_INFORMATION_SCHEMA_RESULT_2['col_sort_order'],
                        is_partition_column=None
                        )],
                is_view=bool(MOCK_INFORMATION_SCHEMA_RESULT_1['is_view']),
        )
        results = self.engine.get_all_table_metadata_from_information_schema(
            catalog=MOCK_CATALOG_NAME)
        result = next(results)
        self.maxDiff = None
        self.assertEqual(result.__repr__(), expected.__repr__())

