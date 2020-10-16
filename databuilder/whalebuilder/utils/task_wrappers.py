import logging
import os
import subprocess
import yaml

from pathlib import Path

from whalebuilder.utils import paths
from whalebuilder.task import WhaleTask
from whalebuilder.loader.whale_loader import WhaleLoader
from whalebuilder.transformer.markdown_transformer import MarkdownTransformer
from whalebuilder.utils.connections import dump_connection_config_in_schema
from whalebuilder.utils import copy_manifest, transfer_manifest

from whalebuilder.utils.extractor_wrappers import (
    configure_bigquery_extractors,
    configure_hive_metastore_extractors,
    configure_neo4j_extractors,
    configure_postgres_extractors,
    configure_presto_extractors,
    configure_redshift_extractors,
    configure_snowflake_extractors,
    run_build_script)

LOGGER = logging.getLogger(__name__)


def create_and_run_tasks_from_yaml(verbose=True):
    for path in [paths.CONFIG_DIR, paths.LOGS_DIR, paths.MANIFEST_DIR, paths.METADATA_PATH, paths.METRICS_PATH]:
        print(path)
        Path(path).mkdir(parents=True, exist_ok=True)

    if os.path.exists(paths.CONNECTION_PATH):
        with open(paths.CONNECTION_PATH, "r") as f:
            raw_connection_dicts = list(yaml.safe_load_all(f))
    else:
        raw_connection_dicts = []

    # Create a manifest
    # If another ETL job is running, put the manifest elsewhere
    tmp_manifest_path = paths.TMP_MANIFEST_PATH
    i = 0
    while os.path.exists(tmp_manifest_path):
        tmp_manifest_path = os.path.join(
            paths.BASE_DIR,
            "manifests/tmp_manifest_" + str(i) + ".txt")
        i += 1

    for raw_connection_dict in raw_connection_dicts:
        connection = dump_connection_config_in_schema(raw_connection_dict)

        metadata_source_dict = {
            "hivemetastore": configure_hive_metastore_extractors,
            "presto": configure_presto_extractors,
            "neo4j": configure_neo4j_extractors,
            "bigquery": configure_bigquery_extractors,
            "snowflake": configure_snowflake_extractors,
            "postgres": configure_postgres_extractors,
            "redshift": configure_redshift_extractors,
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
            is_metadata_extractor = i == 0
            task = WhaleTask(
                extractor=extractor,
                transformer=MarkdownTransformer(),
                loader=WhaleLoader(),
            )
            task.init(conf)

            # Enable try except for non-metadata extractors
            # No need to update the manifest for other extractors
            if is_metadata_extractor:
                task.run()
                task.save_stats()
                conf.pop(manifest_key)
                copy_manifest(tmp_manifest_path)
            else:
                try:
                    task.run()
                except Exception as e:
                    LOGGER.warning(e)
                    LOGGER.warning(f"Skipping {type(extractor)}.")

    transfer_manifest(tmp_manifest_path)
