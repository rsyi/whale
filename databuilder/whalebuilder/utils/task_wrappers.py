import os
import yaml

from pathlib import Path
from databuilder.task.task import DefaultTask
from whalebuilder.loader.metaframe_loader import MetaframeLoader
from whalebuilder.transformer.markdown_transformer import MarkdownTransformer
from whalebuilder.utils.connections import dump_connection_config_in_schema

from whalebuilder.utils.extractor_wrappers import \
        configure_bigquery_extractor, \
        configure_neo4j_extractor, \
        configure_presto_extractor, \
        configure_snowflake_extractor, \
        run_build_script

BASE_DIR = os.path.join(Path.home(), '.whale/')
CONNECTION_PATH = os.path.join(BASE_DIR, 'config/connections.yaml')


def create_and_run_tasks_from_yaml(
        is_full_extraction_enabled=False,
        verbose=True):
    with open(CONNECTION_PATH) as f:
        raw_connection_dicts = list(yaml.safe_load_all(f))

    for raw_connection_dict in raw_connection_dicts:
        connection = dump_connection_config_in_schema(raw_connection_dict)
        print(connection.metadata_source)

        if connection.metadata_source == 'Presto':
            extractor, conf = configure_presto_extractor(
                    connection,
                    is_full_extraction_enabled=is_full_extraction_enabled)
        elif connection.metadata_source == 'Neo4j':
            extractor, conf = configure_neo4j_extractor(connection)
        elif connection.metadata_source == 'Bigquery':
            extractor, conf = configure_bigquery_extractor(connection)
        elif connection.metadata_source == 'Snowflake':
            extractor, conf = configure_snowflake_extractor(connection)
        elif connection.metadata_source == 'build_script':
            run_build_script(connection)
            break
        else:
            break

        conf.put('loader.metaframe.database_name',
            connection.name or connection.metadata_source)

        task = DefaultTask(
            extractor=extractor,
            transformer=MarkdownTransformer(),
            loader=MetaframeLoader(),
        )
        task.init(conf)
        task.run()

