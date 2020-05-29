import os
import sys
import subprocess
import yaml

from pathlib import Path
from pyhocon import ConfigFactory
from databuilder.task.task import DefaultTask
from .loader.markdown_loader import MarkdownLoader
from .extractor.presto_table_metadata_extractor import PrestoTableMetadataExtractor

amundsen_map = {
    'presto': {
        'extractor': PrestoTableMetadataExtractor,
        'conn_string_key': 'extractor.presto_table_metadata.extractor.sqlalchemy.conn_string',
    }
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
        name = connection['name']

        if connection_type in ['presto']:

            username_password_placeholder = \
                    '{}:{}'.format(username, password) if password is not None else ''
            conn_string = '{connection_type}://{username_password}{host}'.format(
                connection_type=connection_type,
                username_password=username_password_placeholder,
                host=host)
            conf = ConfigFactory.from_dict({
                amundsen_map[connection_type]['conn_string_key']: conn_string,
            })

        task = DefaultTask(
            extractor=amundsen_map[connection_type]['extractor'](),
            loader=MarkdownLoader(),
        )
        task.init(conf)
        task.run()

if __name__=='__main__':
    main()
