import logging
import unittest

from pyhocon import ConfigFactory
from typing import Dict, Iterable, Any, Callable  # noqa: F401

from whalebuilder.models.table_metadata import TableMetadata
from whalebuilder.loader.whale_loader import WhaleLoader


class TestWhaleLoader(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        self._conf = ConfigFactory.from_dict({
            'base_directory': './.test_artifacts',
            'tmp_manifest_path': './.test_artifacts/tmp_manifest.txt'
            })

    def test_load_no_catalog(self):
        record = TableMetadata(
            database='mock_database',
            cluster=None,
            schema='mock_schema',
            name='mock_table',
            markdown_blob='Test',
        )
        loader = WhaleLoader()
        loader.init(self._conf)
        loader.load(record)

        loader.close()
        file_path = './.test_artifacts/mock_database/mock_schema.mock_table.md'
        with open(file_path, 'r') as f:
            written_record = f.read()

        self.assertTrue(record.markdown_blob in written_record)

    def test_load_catalog_specified(self):
        record = TableMetadata(
            database='mock_database',
            cluster='mock_catalog',
            schema='mock_schema',
            name='mock_table',
            markdown_blob='Test',
        )
        loader = WhaleLoader()
        loader.init(self._conf)
        loader.load(record)

        loader.close()
        file_path = \
            './.test_artifacts/' + \
            'mock_database/mock_catalog.mock_schema.mock_table.md'
        with open(file_path, 'r') as f:
            written_record = f.read()

        self.assertTrue(record.markdown_blob in written_record)
