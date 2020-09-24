import os
import yaml

from whalebuilder.utils.paths import (
    BASE_DIR,
    CONNECTION_PATH,
    MANIFEST_PATH
)
from pathlib import Path
from databuilder.task.task import DefaultTask
from whalebuilder.loader.whale_loader import WhaleLoader
from whalebuilder.transformer.markdown_transformer import MarkdownTransformer
from whalebuilder.utils.connections import dump_connection_config_in_schema
from whalebuilder.utils import transfer_manifest

from whalebuilder.utils.extractor_wrappers import \
        configure_bigquery_extractors, \
        configure_neo4j_extractors, \
        configure_presto_extractors, \
        configure_snowflake_extractors, \
        run_build_script


def create_and_run_tasks_from_yaml(
        is_full_extraction_enabled=True,
        verbose=True):
    with open(CONNECTION_PATH) as f:
        raw_connection_dicts = list(yaml.safe_load_all(f))

    for raw_connection_dict in raw_connection_dicts:
        connection = dump_connection_config_in_schema(raw_connection_dict)

        if connection.metadata_source == 'Presto':
            extractors, conf = configure_presto_extractors(
                    connection,
                    is_full_extraction_enabled=is_full_extraction_enabled)
        elif connection.metadata_source == 'Neo4j':
            extractors, conf = configure_neo4j_extractors(connection)
        elif connection.metadata_source == 'Bigquery':
            extractors, conf = configure_bigquery_extractors(connection)
        elif connection.metadata_source == 'Snowflake':
            extractors, conf = configure_snowflake_extractors(connection)
        elif connection.metadata_source == 'build_script':
            run_build_script(connection)
            break
        else:
            break

        conf.put('loader.metaframe.database_name',
            connection.name or connection.metadata_source)
        conf.put('loader.filesystem.csv.file_path', MANIFEST_PATH)

        for extractor in extractors:
            task = DefaultTask(
                extractor=extractor,
                transformer=MarkdownTransformer(),
                loader=WhaleLoader(),
            )
            task.init(conf)
            task.run()

        transfer_manifest()