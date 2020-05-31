import logging
from pyhocon import ConfigFactory

from metaframe.engine.presto_engine import PrestoEngine

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
    DEFAULT_CONFIG = ConfigFactory.from_dict({
        'conn_string': None,
        'excluded_schemas': ['information_schema', 'looker', 'pg_catalog'],
        'cluster': None,
        'database': 'presto',
        'is_table_metadata_enabled': True,
        'is_watermark_enabled': False,
        'is_stats_enabled': False,
        'is_analyze_enabled': False,
    })

    def init(self, conf):
        super().init(conf)
        self.conf = conf.with_fallback(PrestoLoopExtractor.DEFAULT_CONFIG)
        self._cluster = self.conf.get('cluster')
        self._database = self.conf.get_string('database')

        self._extract_iter = None
        self._excluded_schemas = self.conf.get('excluded_schemas')
        self._sql_stmt_schemas = ' in '.join(filter(None, ['show schemas', self._cluster]))
        self._is_table_metadata_enabled = self.conf.get('is_table_metadata_enabled')
        self._is_stats_enabled = self.conf.get('is_stats_enabled')
        self._is_analyze_enabled = self.conf.get('is_analyze_enabled')

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

            if schema not in self._excluded_schemas:
                full_schema_address = '.'.join(filter(None, [self._cluster, schema]))
                tables = list(self.execute('show tables in {}'.format(full_schema_address)))
                n_tables = len(tables)
                LOGGER.info('There are {} tables in {}.'.format(n_tables, schema))

                for i, table_row in enumerate(tables):
                    if (i%10==0) or (i==n_tables-1):
                        LOGGER.info('On table {} of {}'.format(i, n_tables))
                    table = table_row[0]

                    if self._is_table_metadata_enabled:
                        table_metadata = self.get_table_metadata(schema, table, cluster=self._cluster)
                        yield table_metadata

                    if self._is_analyze_enabled:
                        self.get_analyze(schema, table, self._cluster)

                    if self._is_stats_enabled:
                        stats_generator = self.get_stats(schema, table, self._cluster)
                        yield from stats_generator

    def get_scope(self):
        return 'extractor.presto_loop'
