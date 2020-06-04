import logging
import pytest
import unittest

from mock import patch
from pyhocon import ConfigFactory

from metaframe.extractor.presto_loop_extractor import PrestoLoopExtractor
from metaframe.engine.sql_alchemy_engine import SQLAlchemyEngine


class TestPrestoLoopExtractor(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.INFO)

        config_dict = {
            'conn_string': 'TEST_CONNECTION',
            'is_table_metadata_enabled': True,
            'is_full_extraction_enabled': True,
            'is_watermark_enabled': True,
            'is_stats_enabled': True,
            'is_analyze_enabled': True
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_extraction_with_empty_result(self):
        """
        Test Extraction with empty result from query.
        """
        with patch.object(SQLAlchemyEngine, '_get_connection'):
            extractor = PrestoLoopExtractor()
            extractor.init(self.conf)

            results = extractor.extract()
            self.assertEqual(results, None)
