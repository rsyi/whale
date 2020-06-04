import logging
import pytest
import unittest

from mock import patch
from pyhocon import ConfigFactory

from metaframe.extractor.presto_loop_extractor import PrestoLoopExtractor
from metaframe.engine.sql_alchemy_engine import SQLAlchemyEngine


class TestPrestoLoopExtractor(unittest.TestCase):
    def setUp(self):
        # type: () -> None
        logging.basicConfig(level=logging.INFO)

        config_dict = {
            'conn_string': 'TEST_CONNECTION',
            'is_table_metadata_enabled': False,
            'is_full_extraction_enabled': False,
            'is_watermark_enabled': False,
            'is_stats_enabled': False,
            'is_analyze_enabled': False
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_extraction_with_empty_result(self):
        # type: () -> None
        """
        Test Extraction with empty result from query
        """
        with patch.object(SQLAlchemyEngine, '_get_connection'):
            extractor = PrestoLoopExtractor()
            extractor.init(self.conf)

            results = extractor.extract()
            self.assertEqual(results, None)
