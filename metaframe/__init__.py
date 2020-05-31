import os
import sys
import subprocess
import yaml

from pathlib import Path
from pyhocon import ConfigFactory
from databuilder.task.task import DefaultTask
from .loader.markdown_loader import MarkdownLoader
from .extractor.presto_loop_extractor import PrestoLoopExtractor
from databuilder.extractor.hive_table_metadata_extractor import HiveTableMetadataExtractor
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.models.table_metadata import TableMetadata

SQL_ALCHEMY_EXTRACTORS = {
    'presto': PrestoLoopExtractor,
    'hive-metastore': HiveTableMetadataExtractor,
}
BASE_DIR = os.path.join(Path.home(), '.metaframe/')

def main(verbose=True):
    with open(os.path.join(BASE_DIR, 'config/connections.yaml')) as f:
        connections = yaml.safe_load(f)

    for connection in connections:

        # Parse configuration.
        username = connection['username'] if 'username' in connection else None
        password = connection['password'] if 'password' in connection else None
        host = connection['host']
        connection_type = connection['type']
        name = connection['name'] if 'name' in connection else connection['type']

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
                'extractor.presto_loop.is_watermark_enabled': False,
                'extractor.presto_loop.is_stats_enabled': False,
                'extractor.presto_loop.is_analyze_enabled': False,
            })

        elif connection_type=='neo4j':
            query = """
            MATCH (n:Table) RETURN n
            """
            extractor = Neo4jExtractor()
            scope = extractor.get_scope()
            conf = ConfigFactory.from_dict({
                '{}.graph_url'.format(scope): host,
                '{}.cypher_query'.format(scope): query,
                '{}.neo4j_auth_user'.format(scope): username,
                '{}.neo4j_auth_pw'.format(scope): password,
                '{}.model_class'.format(scope): 'databuilder.models.table_metadata.TableMetadata',
            })

        conf.put('loader.markdown.database_name', name)

        task = DefaultTask(
            extractor=extractor,
            loader=MarkdownLoader(),
        )
        task.init(conf)
        task.run()

if __name__=='__main__':
    main()
