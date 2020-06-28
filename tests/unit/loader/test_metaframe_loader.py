import logging
import unittest

from pyhocon import ConfigFactory
from typing import Dict, Iterable, Any, Callable  # noqa: F401

from metaframe.models.table_metadata import TableMetadata
from metaframe.loader.metaframe_loader import MetaframeLoader


class TestMetaframeLoader(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        self._conf = ConfigFactory.from_dict({
            'base_directory': './.test_artifacts',
            })

    def test_load_no_catalog(self):
        record = TableMetadata(
            database='mock_database',
            catalog=None,
            schema='mock_schema',
            name='mock_table',
            markdown_blob='Test',
        )
        loader = MetaframeLoader()
        loader.init(self._conf)
        loader.load(record)

        loader.close()
        file_path = './.test_artifacts/mock_database/mock_schema.mock_table.md'
        with open(file_path, 'r') as f:
            written_record = f.read()
        print(written_record)

        self.assertEqual(written_record, record.markdown_blob)

    def test_load_catalog_specified(self):
        record = TableMetadata(
            database='mock_database',
            catalog='mock_catalog',
            schema='mock_schema',
            name='mock_table',
            markdown_blob='Test',
        )
        loader = MetaframeLoader()
        loader.init(self._conf)
        loader.load(record)

        loader.close()
        file_path = \
            './.test_artifacts/' + \
            'mock_database/mock_catalog.mock_schema.mock_table.md'
        with open(file_path, 'r') as f:
            written_record = f.read()
        print(written_record)

        self.assertEqual(written_record, record.markdown_blob)
