import logging
import os
from pyhocon import ConfigFactory

from metaframe.engine.presto_engine import PrestoEngine
from metaframe.utils import get_table_file_path_base

LOGGER = logging.getLogger(__name__)


def parse_partitions(keys, partitions, part_type='high_watermark'):
    part_type = part_type.lower()

    zipped_partitions = list(zip(*list(partitions)))
    for key, parts in zip(keys, zipped_partitions):
        if part_type.lower() == 'high_watermark':
            yield (key, max(parts))
        elif part_type.lower() == 'low_watermark':
            yield (key, min(parts))


class PrestoLoopExtractor(PrestoEngine):
    """
    Extract additional metadata information by checking partitions for every
    table in the db. This relies on a `PrestoEngine` base class, which has
    several methods (`get_...`) to execute and organize the results of queries
    against single tables.
    """
    CONN_STRING_KEY = 'conn_string'
    EXCLUDED_SCHEMAS_KEY = 'excluded_schemas'
    INCLUDED_SCHEMAS_KEY = 'included_schemas'
    CATALOG_KEY = 'catalog'
    DATABASE_KEY = 'database'
    IS_FULL_EXTRACTION_ENABLED_KEY = 'is_full_extraction_enabled'
    IS_WATERMARK_ENABLED_KEY = 'is_watermark_enabled'
    IS_STATS_ENABLED_KEY = 'is_stats_enabled'
    IS_ANALYZE_ENABLED_KEY = 'is_analyze_enabled'
    IS_TABLE_METADATA_ENABLED_KEY = 'is_table_metadata_enabled'
    IS_VIEW_QUERY_ENABLED_KEY = 'is_view_query_enabled'

    DEFAULT_CONFIG = ConfigFactory.from_dict({
        CONN_STRING_KEY: None,
        EXCLUDED_SCHEMAS_KEY: None,
        INCLUDED_SCHEMAS_KEY: None,
        CATALOG_KEY: None,
        DATABASE_KEY: 'presto',
        IS_FULL_EXTRACTION_ENABLED_KEY: False,
        IS_WATERMARK_ENABLED_KEY: False,
        IS_STATS_ENABLED_KEY: False,
        IS_ANALYZE_ENABLED_KEY: False,
        IS_TABLE_METADATA_ENABLED_KEY: False,
        IS_VIEW_QUERY_ENABLED_KEY: False,
    })

    def init(self, conf):
        super().init(conf)
        self.conf = conf.with_fallback(self.DEFAULT_CONFIG)
        self._catalog = self.conf.get(PrestoLoopExtractor.CATALOG_KEY)
        self._database = self.conf.get_string(PrestoLoopExtractor.DATABASE_KEY)

        self._extract_iter = None
        self._excluded_schemas = \
            self.conf.get(PrestoLoopExtractor.EXCLUDED_SCHEMAS_KEY) or []
        self._included_schemas = \
            self.conf.get(PrestoLoopExtractor.INCLUDED_SCHEMAS_KEY) or []

        self._sql_stmt_schemas = \
            ' in '.join(filter(None, ['show schemas', self._catalog]))
        self._is_table_metadata_enabled = \
            self.conf.get(PrestoLoopExtractor.IS_TABLE_METADATA_ENABLED_KEY)
        self._is_stats_enabled = \
            self.conf.get(PrestoLoopExtractor.IS_STATS_ENABLED_KEY)
        self._is_analyze_enabled = \
            self.conf.get(PrestoLoopExtractor.IS_ANALYZE_ENABLED_KEY)
        self._is_full_extraction_enabled = \
            self.conf.get(PrestoLoopExtractor.IS_FULL_EXTRACTION_ENABLED_KEY)
        self._is_view_query_enabled = \
            self.conf.get(PrestoLoopExtractor.IS_VIEW_QUERY_ENABLED_KEY)

    def extract(self):
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            extraction = next(self._extract_iter)
            return extraction
        except StopIteration:
            return None

    def _get_extract_iter(self):
        schemas = self.execute(self._sql_stmt_schemas)
        for schema_row in schemas:
            schema = schema_row[0]
            LOGGER.info('Fetching all tables in {}.'.format(schema))

            if (schema not in self._excluded_schemas) \
                and (
                    (schema in self._included_schemas)
                    or not self._included_schemas):
                full_schema_address = \
                    '.'.join(filter(None, [self._catalog, schema]))
                tables = list(
                    self.execute(
                        'show tables in {}'
                        .format(full_schema_address)))
                n_tables = len(tables)
                LOGGER.info(
                    'There are {} tables in {}.'
                    .format(n_tables, schema))

                for i, table_row in enumerate(tables):
                    if (i % 10 == 0) or (i == n_tables-1):
                        LOGGER.info('On table {} of {}'.format(i+1, n_tables))
                    table = table_row[0]
                    file_name = get_table_file_path_base(
                        database=self._database,
                        catalog=self._catalog,
                        schema=schema,
                        table=table,
                    )
                    if not os.path.exists(file_name + '.md') \
                            or self._is_full_extraction_enabled:

                        if self._is_table_metadata_enabled:
                            table_metadata = \
                                self.get_table_metadata(
                                    schema,
                                    table,
                                    catalog=self._catalog,
                                    is_view_query_enabled
                                        =self._is_view_query_enabled)
                            yield table_metadata

                        if self._is_analyze_enabled:
                            self.get_analyze(schema, table, self._catalog)

                        if self._is_stats_enabled:
                            stats_generator = \
                                self.get_stats(schema, table, self._catalog)
                            yield from stats_generator
                    else:
                        LOGGER.info(
                            'Skipping {}.{} because the file already exists.'
                            .format(schema, table))

    def get_scope(self):
        return 'extractor.presto_loop'
