import os
import yaml

from pathlib import Path
from databuilder.task.task import DefaultTask
from metaframe.loader.metaframe_loader import MetaframeLoader
from metaframe.transformer.markdown_transformer import MarkdownTransformer
from metaframe.utils.connections import dump_connection_config_in_schema

from metaframe.utils.extractor_wrappers import \
        configure_bigquery_extractor, \
        configure_neo4j_extractor, \
        configure_presto_extractor, \
        configure_snowflake_extractor, \
        run_build_script

BASE_DIR = os.path.join(Path.home(), '.metaframe/')
CONNECTION_PATH = os.path.join(BASE_DIR, 'config/connections.yaml')


def create_and_run_tasks_from_yaml(
        is_full_extraction_enabled=False,
        verbose=True):
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
        elif connection.type == 'bigquery':
            extractor, conf = configure_bigquery_extractor(connection)
        elif connection.type == 'snowflake':
            extractor, conf = configure_snowflake_extractor(connection)
        elif connection.type == 'build_script':
            run_build_script(connection)
            break
        else:
            break

        conf.put('loader.metaframe.database_name',
            connection.name or connection.type)

        task = DefaultTask(
            extractor=extractor,
            transformer=MarkdownTransformer(),
            loader=MetaframeLoader(),
        )
        task.init(conf)
        task.run()

