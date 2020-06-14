import os
import yaml

from pathlib import Path
from databuilder.task.task import DefaultTask
from metaframe.loader.metaframe_loader import MetaframeLoader
from metaframe.transformer.markdown_transformer import MarkdownTransformer
from metaframe.utils.connections import dump_connection_config_in_schema

from metaframe.utils.extractor_wrappers import \
        configure_presto_extractor, \
        configure_neo4j_extractor, \
        run_build_script

BASE_DIR = os.path.join(Path.home(), '.metaframe/')
CONNECTION_PATH = os.path.join(BASE_DIR, 'config/connections.yaml')


def main(is_full_extraction_enabled=False, verbose=True):
    with open(CONNECTION_PATH) as f:
        raw_connection_dicts = yaml.safe_load(f)

    for raw_connection_dict in raw_connection_dicts:
        connection = dump_connection_config_in_schema(raw_connection_dict)

        if connection.type == 'presto':
            extractor, conf = configure_presto_extractor(
                    connection,
                    is_full_extraction_enabled=is_full_extraction_enabled)
        elif connection.type == 'neo4j':
            extractor, conf = configure_neo4j_extractor(connection)
        elif connection.type == 'build_script':
            run_build_script(connection)
            break

        conf.put('loader.metaframe.database_name', connection.name)

        task = DefaultTask(
            extractor=extractor,
            transformer=MarkdownTransformer(),
            loader=MetaframeLoader(),
        )
        task.init(conf)
        task.run()


if __name__ == '__main__':
    main()
