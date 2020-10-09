import os
import yaml

from whalebuilder.utils.paths import (  # noqa: F401
    BASE_DIR,
    CONNECTION_PATH,
    MANIFEST_PATH,
    TMP_MANIFEST_PATH
)
from whalebuilder.task import WhaleTask
from whalebuilder.loader.whale_loader import WhaleLoader
from whalebuilder.transformer.markdown_transformer import MarkdownTransformer
from whalebuilder.utils.connections import dump_connection_config_in_schema
from whalebuilder.utils import copy_manifest, transfer_manifest

from whalebuilder.utils.extractor_wrappers import \
        configure_bigquery_extractors, \
        configure_neo4j_extractors, \
        configure_postgres_extractors, \
        configure_presto_extractors, \
        configure_snowflake_extractors, \
        run_build_script


def create_and_run_tasks_from_yaml(verbose=True):
    with open(CONNECTION_PATH) as f:
        raw_connection_dicts = list(yaml.safe_load_all(f))

    # Create a manifest
    # If another ETL job is running, put the manifest elsewhere
    tmp_manifest_path = TMP_MANIFEST_PATH
    i = 0
    while os.path.exists(tmp_manifest_path):
        tmp_manifest_path = os.path.join(
            BASE_DIR,
            "manifests/tmp_manifest_" + str(i) + ".txt")
        i += 1

    for raw_connection_dict in raw_connection_dicts:
        connection = dump_connection_config_in_schema(raw_connection_dict)

        metadata_source_dict = {
            "presto": configure_presto_extractors,
            "neo4j": configure_neo4j_extractors,
            "bigquery": configure_bigquery_extractors,
            "snowflake": configure_snowflake_extractors,
            "postgres": configure_postgres_extractors,
        }

        if connection.metadata_source == 'build_script':
            run_build_script(connection)
            break
        else:
            configurer = metadata_source_dict[connection.metadata_source]
            extractors, conf = configurer(connection)

        manifest_key = 'loader.whale.tmp_manifest_path'
        conf.put('loader.whale.database_name', connection.name)
        conf.put(manifest_key, tmp_manifest_path)

        for i, extractor in enumerate(extractors):
            task = WhaleTask(
                extractor=extractor,
                transformer=MarkdownTransformer(),
                loader=WhaleLoader(),
            )
            task.init(conf)
            task.run()

            # The first extractor passes all tables, always
            # No need to update the manifest after the first time
            if i == 0:
                task.save_stats()
                conf.pop(manifest_key)
                copy_manifest(tmp_manifest_path)

    transfer_manifest(tmp_manifest_path)
