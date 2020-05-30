import os
import sys
import subprocess
import yaml

from pathlib import Path
from pyhocon import ConfigFactory
from databuilder.task.task import DefaultTask
from .loader.markdown_loader import MarkdownLoader
from .extractor.presto_table_metadata_extractor import PrestoTableMetadataExtractor
from databuilder.extractor.hive_table_metadata_extractor import HiveTableMetadataExtractor
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.models.table_metadata import TableMetadata

SQL_ALCHEMY_EXTRACTORS = {
    'presto': PrestoTableMetadataExtractor,
    'hive-metastore': HiveTableMetadataExtractor,
}
BASE_DIR = os.path.join(Path.home(), '.metaframe/')

def main():
    with open(os.path.join(BASE_DIR, 'config/connections.yaml')) as f:
        connections = yaml.safe_load(f)

    for connection in connections:

        # Parse configuration.
        username = connection['username']
        password = connection['password']
        host = connection['host']
        connection_type = connection['type']

        if connection_type in SQL_ALCHEMY_EXTRACTORS:

            extractor = SQL_ALCHEMY_EXTRACTORS[connection_type]()
            scope = extractor.get_scope()
            conn_string_key = '{}.extractor.sqlalchemy.conn_string'.format(scope)

            username_password_placeholder = \
                    '{}:{}'.format(username, password) if password is not None else ''

            conn_string = '{connection_type}://{username_password}{host}'.format(
                connection_type=connection_type,
                username_password=username_password_placeholder,
                host=host)

            conf = ConfigFactory.from_dict({
                conn_string_key: conn_string,
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

        task = DefaultTask(
            extractor=extractor,
            loader=MarkdownLoader(),
        )
        task.init(conf)
        task.run()

if __name__=='__main__':
    main()
