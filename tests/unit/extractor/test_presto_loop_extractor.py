import logging
import unittest

from mock import patch, MagicMock
from pyhocon import ConfigFactory

from metaframe.extractor.presto_loop_extractor import PrestoLoopExtractor
from metaframe.engine.sql_alchemy_engine import SQLAlchemyEngine
from metaframe.models.table_metadata import ColumnMetadata, TableMetadata


MOCK_SCHEMA_NAME = 'mock_schema'
MOCK_TABLE_NAME = 'mock_table'
MOCK_COLUMN_RESULT = ('ds', 'varchar(64)', 'partition key', '')


def presto_engine_execute_side_effect(query, has_header=True):
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


@patch.object(SQLAlchemyEngine, '_get_connection')
class TestPrestoLoopExtractor(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)

        config_dict = {
            'conn_string': 'TEST_CONNECTION',
            'is_full_extraction_enabled': True,
            'is_table_metadata_enabled': False,
            'is_watermark_enabled': False,
            'is_stats_enabled': False,
            'is_analyze_enabled': False
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_extraction_with_empty_result(self, mock_settings) -> None:
        """
        Test Extraction with empty result from query.
        """
        extractor = PrestoLoopExtractor()
        extractor.init(self.conf)

        results = extractor.extract()
        self.assertEqual(results, None)

    def test_table_metadata_extraction_with_single_result(
            self, mock_settings) -> None:
        extractor = PrestoLoopExtractor()
        conf = self.conf.copy()
        conf.put('is_table_metadata_enabled', True)
        extractor.init(conf)
        extractor.execute = MagicMock(
            side_effect=presto_engine_execute_side_effect
        )

        results = extractor.extract()
        is_partition_column = True \
            if MOCK_COLUMN_RESULT[2] == 'partition key' \
            else False
        expected = TableMetadata(
                database=extractor._database,
                catalog=None,
                schema=MOCK_SCHEMA_NAME,
                name=MOCK_TABLE_NAME,
                columns=[ColumnMetadata(
                    name=MOCK_COLUMN_RESULT[0],
                    description=MOCK_COLUMN_RESULT[3],
                    col_type=MOCK_COLUMN_RESULT[1],
                    sort_order=0,
                    is_partition_column=is_partition_column)]
        )
        self.assertEqual(results.__repr__(), expected.__repr__())
