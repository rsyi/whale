import logging
import unittest

from whale.models.table_metadata import TableMetadata
from whale.models.column_metadata import ColumnMetadata

class TestTableMetadata(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)

    def test_format_for_markdown(self):
        table_metadata = TableMetadata(
            database='test_database',
            cluster='test_cluster',
            schema='test_schema',
            name='test_table',
            columns=[
                ColumnMetadata(
                    name='test_column_1',
                    description=None,
                    data_type='INTEGER',
                    sort_order=1,
                ),
                ColumnMetadata(
                    name='test_column_2',
                    description=None,
                    data_type='BOOLEAN',
                    sort_order=2,
                ),
            ],
        )

        expected = """# `test_schema.test_table`
`test_database` | `test_cluster`

## Column details
* [INTEGER]   `test_column_1`
* [BOOLEAN]   `test_column_2`
"""

        self.assertEqual(table_metadata.format_for_markdown(), expected)

