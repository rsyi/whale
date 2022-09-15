from whale.models.column_metadata import ColumnMetadata
from whale.models.table_metadata import TableMetadata
from itertools import groupby
from typing import (
    Iterator
)
from databuilder.extractor.table_metadata_constants import PARTITION_BADGE
from databuilder.extractor.hive_table_metadata_extractor import HiveTableMetadataExtractor as DatabuilderHiveTableMetadataExtractor

class HiveTableMetadataExtractor(DatabuilderHiveTableMetadataExtractor):
    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        """
        Using itertools.groupby and raw level iterator, it groups to table and yields TableMetadata
        :return:
        """

        for key, group in groupby(self._get_raw_extract_iter(), self._get_table_key):
            columns = []

            for row in group:
                last_row = row
                column = None
                if row['is_partition_col'] == 1:
                    # create add a badge to indicate partition column
                    column = ColumnMetadata(row['col_name'], row['col_description'],
                                            row['col_type'], row['col_sort_order'], [PARTITION_BADGE])
                else:
                    column = ColumnMetadata(row['col_name'], row['col_description'],
                                            row['col_type'], row['col_sort_order'])
                print(key,column)
                columns.append(column)
            is_view = last_row['is_view'] == 1

            yield TableMetadata(database='hive', cluster = self._cluster,
                                schema=last_row['schema'],
                                name = last_row['name'],
                                description = last_row['description'],
                                columns = columns,
                                is_view=is_view)
