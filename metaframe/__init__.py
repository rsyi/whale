import os
import sys
import subprocess
import yaml

from pathlib import Path
from pyhocon import ConfigFactory
from databuilder.task.task import DefaultTask
from .loader.metaframe_loader import MetaframeLoader
from .transformer.markdown_transformer import MarkdownTransformer
from .extractor.presto_loop_extractor import PrestoLoopExtractor
from .extractor.amundsen_neo4j_metadata_extractor import AmundsenNeo4jMetadataExtractor
from databuilder.extractor.hive_table_metadata_extractor import HiveTableMetadataExtractor
from databuilder.models.table_metadata import TableMetadata

SQL_ALCHEMY_EXTRACTORS = {
    'presto': PrestoLoopExtractor,
    'hive-metastore': HiveTableMetadataExtractor,
}
BASE_DIR = os.path.join(Path.home(), '.metaframe/')

def main(is_full_extraction_enabled=False, verbose=True):
    with open(os.path.join(BASE_DIR, 'config/connections.yaml')) as f:
        connections = yaml.safe_load(f)

    for connection in connections:
        def get_connection_value(key, fallback=None):
            return connection[key] if key in connection else fallback

        # Parse configuration.
        host = connection['host']
        connection_type = connection['type']
        username = get_connection_value('username')
        password = get_connection_value('password')
        name = get_connection_value('name')

        # Parse optional configuration entries.
        cluster = get_connection_value('cluster')
        included_schemas = get_connection_value('included_schemas')
        excluded_schemas = get_connection_value('excluded_schemas')
        included_keys = get_connection_value('included_keys')
        excluded_keys = get_connection_value('excluded_keys')
        included_key_regex = get_connection_value('included_key_regex')
        excluded_key_regex = get_connection_value('excluded_key_regex')

        if connection_type=='presto':

            extractor = PrestoLoopExtractor()
            scope = extractor.get_scope()
            conn_string_key = '{}.conn_string'.format(scope)

            username_password_placeholder = \
                    '{}:{}'.format(username, password) if password is not None else ''

            conn_string = '{connection_type}://{username_password}{host}'.format(
                connection_type=connection_type,
                username_password=username_password_placeholder,
                host=host)

            conf = ConfigFactory.from_dict({
                conn_string_key: conn_string,
                'extractor.presto_loop.is_table_metadata_enabled': True,
                'extractor.presto_loop.is_full_extraction_enabled': \
                        is_full_extraction_enabled,
                'extractor.presto_loop.is_watermark_enabled': False,
                'extractor.presto_loop.is_stats_enabled': False,
                'extractor.presto_loop.is_analyze_enabled': False,
                'extractor.presto_loop.database': name,
                'extractor.presto_loop.cluster': cluster,
                'extractor.presto_loop.included_schemas': included_schemas,
                'extractor.presto_loop.excluded_schemas': excluded_schemas,
            })

        elif connection_type=='neo4j':
            extractor = AmundsenNeo4jMetadataExtractor()
            scope = extractor.get_scope()
            conf = ConfigFactory.from_dict({
                '{}.graph_url'.format(scope): 'bolt://' + host,
                '{}.neo4j_auth_user'.format(scope): username,
                '{}.neo4j_auth_pw'.format(scope): password,
                '{}.included_keys'.format(scope): included_keys,
                '{}.excluded_keys'.format(scope): excluded_keys,
                '{}.included_key_regex'.format(scope): included_key_regex,
                '{}.excluded_key_regex'.format(scope): excluded_key_regex,
            })

        conf.put('loader.markdown.database_name', name)

        task = DefaultTask(
            extractor=extractor,
            transformer=MarkdownTransformer(),
            loader=MetaframeLoader(),
        )
        task.init(conf)
        task.run()

if __name__=='__main__':
    main()
