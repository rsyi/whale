import logging
import time
from collections import namedtuple
from itertools import groupby
from pyhocon import ConfigFactory
from typing import Dict, Iterable, Iterator, List, Optional

from metaframe.models.presto_watermark import PrestoWatermark
from metaframe.models.table_metadata import \
    ColumnMetadata, \
    TableColumnStats, \
    TableMetadata


LOGGER = logging.getLogger(__name__)


def _calculate_watermarks(
        partition_names: Iterable,
        partition_query_rows: List[tuple],
        watermark_type: str):
    """
    Calculates the high and low watermarks from a SqlAlchemyEngine.execute
    result for the query `select * from schema."table$partitions"`. This result
    has rows of the form:

    ('2020-02-01', 'Uplift University')
    ('2020-01-01', 'Uplift University')
    ('2020-02-01', 'Metaframe College')
    ('2020-01-01', 'Metaframe College')

    The column keys are named after the columns that the base table is
    partitioned on, and the values in the rows correspond to the partition
    value.
    """
    watermark_type = watermark_type.lower()

    # Turn list of rows into list of columns.
    list_of_partition_values = list(zip(*list(partition_query_rows)))

    watermarks = []
    for partition_name, partition_values \
            in zip(partition_names, list_of_partition_values):
        if watermark_type == 'high_watermark':
            watermarks.append((partition_name, max(partition_values)))
        elif watermark_type == 'low_watermark':
            watermarks.append((partition_name, min(partition_values)))
    return watermarks


class PrestoCommandsMixin:
    def _get_full_schema_address(self, catalog, schema):
        return '.'.join(filter(None, [catalog, schema]))

    def get_all_table_metadata_from_information_schema(
            self,
            catalog: Optional[str] = None,
            where_clause_suffix = '',
            ):

        unformatted_query = """
        SELECT
          a.table_catalog AS catalog
          , a.table_schema AS schema
          , a.table_name AS name
          , NULL AS description
          , a.column_name AS col_name
          , a.ordinal_position as col_sort_order
          , IF(a.extra_info = 'partition key', 1, 0) AS is_partition_col
          , a.comment AS col_description
          , a.data_type AS col_type
          , IF(b.table_name is not null, 1, 0) AS is_view
        FROM {catalog_prefix}information_schema.columns a
        LEFT JOIN {catalog_prefix}information_schema.views b
            ON a.table_catalog = b.table_catalog
            and a.table_schema = b.table_schema
            and a.table_name = b.table_name
        {where_clause_suffix}
        """

        LOGGER.info(
            'Pulling all table metadata in bulk from' +
            'information_schema in catalog name: {}'.format(catalog))

        if catalog is not None:
            catalog_prefix = catalog + '.'
        else:
            catalog_prefix = ''

        formatted_query = unformatted_query.format(
            catalog_prefix=catalog_prefix,
            where_clause_suffix=where_clause_suffix
        )

        LOGGER.info('SQL for presto: {}'.format(formatted_query))

        query_results = self.execute(
            formatted_query,
            is_dict_return_enabled=True)

        for _, group in groupby(
                query_results,
                self._get_table_key):
            columns = []
            for row in group:
                last_row = row
                columns.append(
                    ColumnMetadata(
                        row['col_name'],
                        row['col_description'],
                        row['col_type'],
                        row['col_sort_order']))

            yield TableMetadata(
                self._database,
                catalog or self._default_catalog_name,
                last_row['schema'],
                last_row['name'],
                last_row['description'],
                columns,
                is_view=bool(last_row['is_view']),
                )

    def get_table_metadata(
            self,
            schema: str,
            table: str,
            catalog: Optional[str] = None,
            is_view_query_enabled: Optional[bool] = False
            ):
        # Format table and schema addresses for queries.
        full_schema_address = self._get_full_schema_address(catalog, schema)
        full_table_address = '{}.{}'.format(full_schema_address, table)

        # Execute query that gets column type + partition information.
        columns_query = 'show columns in {}'.format(full_table_address)
        column_query_results = self.execute(columns_query, has_header=True)
        column_query_field_names = next(column_query_results)
        columns = []
        for i, column_query_result in enumerate(column_query_results):
            column_dict = \
                    dict(zip(column_query_field_names, column_query_result))
            columns.append(ColumnMetadata(
                name=column_dict['Column'],
                description=column_dict['Comment'],
                col_type=column_dict['Type'],
                sort_order=i,
                is_partition_column=column_dict['Extra'] == 'partition key',
            ))

        if is_view_query_enabled:
            # Execute query that returns if table is a view.
            view_query = """
                select table_type
                from information_schema.tables
                where table_schema='{table_schema}'
                  and table_name='{table_name}'
                """.format(table_schema=schema, table_name=table)
            view_query_results = self.execute(view_query, has_header=False)
            is_view = next(view_query_results)[0] == 'VIEW'
        else:
            is_view = False

        return TableMetadata(
             database=self._database,
             catalog=catalog,
             schema=schema,
             name=table,
             description=None,
             columns=columns,
             is_view=is_view,
        )

    def get_preview(
            self,
            schema: str,
            table: str,
            catalog: Optional[str] = None,
            n_rows: int = 10) -> Iterator[Dict]:
        """
        For partitioned tables, find the approximate latest partition and
        execute `SELECT * FROM X LIMIT X` within that partition. For
        non-partitioned tables, do the same, but not within a partition. The
        result is formatted as a dictionary of {column name: value} pairs, and
        returned as an iterator.
        """
        full_schema_address = self._get_full_schema_address(catalog, schema)
        partition_table_name = \
            '{}."{}$partitions"'.format(full_schema_address, table)
        # Hack: since partitions are usually date, this should usually get the
        # latest partition.
        partition_query = \
            'select * from {} order by 1 desc limit 1' \
            .format(partition_table_name)

        # Attempt to query partition info and format into a `where_clause`
        try:
            partition_query_results = \
                self.execute(partition_query, has_header=True)

            # Parse the `self.execute` results. The first row is the header.
            latest_partition_column_names = next(partition_query_results)
            latest_partition_values = next(partition_query_results)

            # Obtain partition types to enable typesafe comparisons in our
            # preview query.
            type_query = \
                'show columns from {}.{}'. \
                format(full_schema_address, table)
            type_query_results = self.execute(type_query, has_header=True)
            type_results_column_names = next(type_query_results)
            partition_type_dict = {}
            for row in type_query_results:
                row_dict = dict(zip(type_results_column_names, row))
                if row_dict['Extra'] == 'partition key':
                    partition_type_dict[row_dict['Column']] = row_dict['Type']

            where_clause = self._format_keys_and_values_as_where_clause(
                keys=latest_partition_column_names,
                values=latest_partition_values,
                type_dict=partition_type_dict,
            )

        # If table is not partitioned, specify an empty `where_clause`.
        except Exception:
            where_clause = ''

        preview_query = \
            'select * from {}.{} {} limit {}' \
            .format(full_schema_address, table, where_clause, n_rows)
        preview_query_results = self.execute(preview_query, has_header=True)
        preview_column_names = next(preview_query_results)
        for row in preview_query_results:
            formatted_preview = dict(zip(preview_column_names, row))
            yield formatted_preview

    def _format_keys_and_values_as_where_clause(
            self,
            keys: Iterable,
            values: Iterable,
            type_dict: dict) -> str:

        statements = []
        for i, (key, value) in enumerate(zip(keys, values)):
            condition_str = \
                    "{} = CAST('{}' AS {})" \
                    .format(key, value, type_dict[key])
            if i == 0:
                statements.append('where {}'.format(condition_str))
            else:
                statements.append('and {}'.format(condition_str))

        where_clause = ' '.join(statements)

        return where_clause

    def get_watermarks(
            self,
            schema: str,
            table: str,
            catalog: str = None) -> Iterator[PrestoWatermark]:
        """
        Get watermarks, which are high/low values of partition columns.
        """
        full_schema_address = self._get_full_schema_address(catalog, schema)
        partition_table_name = \
            '{}."{}$partitions"' \
            .format(full_schema_address, table)
        partition_query = 'SELECT * FROM {}'.format(partition_table_name)

        try:
            partition_query_results = \
                self.execute(partition_query, has_header=True)
            partition_column_names = next(partition_query_results)
            partition_query_rows = list(partition_query_results)
            watermarks_high = _calculate_watermarks(
                partition_names=partition_column_names,
                partition_query_rows=partition_query_rows,
                watermark_type='high_watermark')
            watermarks_low = _calculate_watermarks(
                partition_names=partition_column_names,
                partition_query_rows=partition_query_rows,
                watermark_type='low_watermark')

            yield PrestoWatermark(
                database=self._database,
                catalog=catalog or self._default_catalog_name,
                schema=schema,
                table_name=table,
                parts=watermarks_high,
                part_type='high_watermark')

            yield PrestoWatermark(
                database=self._database,
                catalog=catalog or self._default_catalog_name,
                schema=schema,
                table_name=table,
                parts=watermarks_low,
                part_type='low_watermark')
        except Exception as e:
            LOGGER.exception(e)

    def get_analyze(
            self,
            schema: str,
            table: str,
            catalog: str = None) -> Optional[int]:
        """
        Run `analyze table`, which calculates statistics for the table in
        preparation for `show stats`, while also returning the number of rows.
        """
        full_schema_address = self._get_full_schema_address(catalog, schema)
        full_table_address = full_schema_address + '.' + table
        analyze_query = 'analyze {}'.format(full_table_address)

        try:
            results = self.execute(analyze_query)
            return next(results)[0]
        except Exception:
            pass

    def get_stats(
            self,
            schema: str,
            table: str,
            catalog: str = None):
        """
        Run `show stats for table`, which returns some statistics for hive
        tables.
        """
        full_schema_address = self._get_full_schema_address(catalog, schema)
        full_table_address = full_schema_address + '.' + table
        stats_query = 'show stats for {}'.format(full_table_address)

        try:
            stats_results = self.execute(stats_query, has_header=True)
            stats_column_names = next(stats_results)

            for stats_values in stats_results:
                stats_dict = dict(zip(stats_column_names, stats_values))
                column_name = stats_dict.pop('column_name')
                if column_name:
                    for stat_name, stat_value in stats_dict.items():
                        if stat_name and stat_value:
                            LOGGER.debug(
                                'Creating column stats object for {}: {}'
                                .format(stat_name, stat_value))
                            yield TableColumnStats(
                                table_name=table,
                                col_name=column_name,
                                stat_name=stat_name,
                                stat_val=stat_value,
                                start_epoch=0,
                                end_epoch=int(time.time()),
                                db=self._database,
                                catalog=catalog or self._default_catalog_name,
                                schema=schema,
                            )

        except Exception as e:
            LOGGER.exception(e)

    def _get_table_key(self, row):
        # type: (Dict[str, Any]) -> Union[TableKey, None]
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            TableKey = namedtuple('TableKey', ['schema', 'table_name'])
            return TableKey(schema=row['schema'], table_name=row['name'])
        return None

    def _get_table_key(self, row):
        # type: (Dict[str, Any]) -> Union[TableKey, None]
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            TableKey = namedtuple('TableKey', ['schema', 'table_name'])
            return TableKey(schema=row['schema'], table_name=row['name'])
        return None

