import os

from pyhocon import ConfigFactory, ConfigTree
from whale.extractor.presto_loop_extractor import PrestoLoopExtractor
from whale.extractor.presto_table_metadata_extractor import PrestoTableMetadataExtractor
from whale.models.connection_config import ConnectionConfigSchema
from whale.extractor.amundsen_neo4j_metadata_extractor import (
    AmundsenNeo4jMetadataExtractor,
)
from whale.extractor.bigquery_metadata_extractor import BigQueryMetadataExtractor
from whale.extractor.spanner_metadata_extractor import SpannerMetadataExtractor
from whale.extractor.bigquery_watermark_extractor import BigQueryWatermarkExtractor
from whale.extractor.glue_extractor import GlueExtractor
from whale.extractor.snowflake_metadata_extractor import SnowflakeMetadataExtractor
from whale.extractor.splice_machine_metadata_extractor import (
    SpliceMachineMetadataExtractor,
)
from whale.extractor.postgres_metadata_extractor import PostgresMetadataExtractor
from whale.extractor.ugc_runner import UGCRunner
from whale.extractor.base_index_extractor import IndexExtractor
from whale.extractor.postgres_index_extractor import PostgresIndexExtractor
from whale.engine.sql_alchemy_engine import SQLAlchemyEngine
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.extractor.hive_table_metadata_extractor import (
    HiveTableMetadataExtractor,
)
from databuilder.extractor.redshift_metadata_extractor import RedshiftMetadataExtractor


BUILD_SCRIPT_TEMPLATE = """source {venv_path}/bin/activate \
    && {python_binary} {build_script_path}"""
SQL_ALCHEMY_SCOPE = SQLAlchemyExtractor().get_scope()
SQL_ALCHEMY_ENGINE_SCOPE = SQLAlchemyEngine().get_scope()

METRIC_RUNNER_SCOPE = UGCRunner().get_scope()
INDEX_SCOPE = IndexExtractor().get_scope()


def get_sql_alchemy_conn_string_key(scope):
    conn_string_key = f"{scope}.{SQL_ALCHEMY_SCOPE}.{SQLAlchemyExtractor.CONN_STRING}"
    return conn_string_key


def add_ugc_runner(extractors: list, conf: ConfigTree, connection):
    conf.put(f"{METRIC_RUNNER_SCOPE}.{UGCRunner.DATABASE_KEY}", connection.name)
    conf.put(
        f"{METRIC_RUNNER_SCOPE}.{SQL_ALCHEMY_ENGINE_SCOPE}.{SQLAlchemyEngine.CONN_STRING_KEY}",
        connection.conn_string,
    )
    conf.put(
        f"{METRIC_RUNNER_SCOPE}.{SQL_ALCHEMY_ENGINE_SCOPE}.{SQLAlchemyEngine.CREDENTIALS_PATH_KEY}",
        connection.key_path,
    )
    extractors.append(UGCRunner())
    return extractors, conf


def add_indexes(extractors: list, conf: ConfigTree, connection):
    conf.put(f"{INDEX_SCOPE}.{IndexExtractor.DATABASE_KEY}", connection.name)
    conf.put(
        f"{INDEX_SCOPE}.{SQL_ALCHEMY_ENGINE_SCOPE}.{SQLAlchemyEngine.CONN_STRING_KEY}",
        connection.conn_string,
    )
    conf.put(
        f"{INDEX_SCOPE}.{SQL_ALCHEMY_ENGINE_SCOPE}.{SQLAlchemyEngine.CREDENTIALS_PATH_KEY}",
        connection.key_path,
    )

    metadata_source_dict = {
        "postgres": PostgresIndexExtractor,
    }

    extractors.append(metadata_source_dict[connection.metadata_source]())
    return extractors, conf


def configure_bigquery_extractors(connection: ConnectionConfigSchema):
    Extractor = BigQueryMetadataExtractor
    extractor = Extractor()
    scope = extractor.get_scope()
    watermark_extractor = BigQueryWatermarkExtractor()
    watermark_scope = watermark_extractor.get_scope()

    conf = ConfigFactory.from_dict(
        {
            f"{scope}.connection_name": connection.name,
            f"{scope}.key_path": connection.key_path,
            f"{scope}.project_id": connection.project_id,
            f"{scope}.project_credentials": connection.project_credentials,
            f"{scope}.page_size": connection.page_size,
            f"{scope}.filter_key": connection.filter_key,
            f"{scope}.included_tables_regex": connection.included_tables_regex,
            f"{watermark_scope}.connection_name": connection.name,
            f"{watermark_scope}.key_path": connection.key_path,
            f"{watermark_scope}.project_id": connection.project_id,
            f"{watermark_scope}.project_credentials": connection.project_credentials,
            f"{watermark_scope}.included_tables_regex": connection.included_tables_regex,
        }
    )

    extractors = [extractor, watermark_extractor]
    extractors, conf = add_ugc_runner(extractors, conf, connection)

    return extractors, conf


def configure_spanner_extractors(connection: ConnectionConfigSchema):
    Extractor = SpannerMetadataExtractor
    extractor = Extractor()
    scope = extractor.get_scope()

    conf = ConfigFactory.from_dict(
        {
            f"{scope}.{Extractor.CONNECTION_NAME_KEY}": connection.name,
            f"{scope}.{Extractor.DATABASE_ID_KEY}": connection.database,
            f"{scope}.{Extractor.INSTANCE_ID_KEY}": connection.instance,
            f"{scope}.{Extractor.KEY_PATH_KEY}": connection.key_path,
            f"{scope}.{Extractor.PROJECT_ID_KEY}": connection.project_id,
        }
    )

    extractors = [extractor]
    extractors, conf = add_ugc_runner(extractors, conf, connection)

    return extractors, conf


def configure_glue_extractors(connection: ConnectionConfigSchema):
    Extractor = GlueExtractor
    extractor = Extractor()
    scope = extractor.get_scope()

    conf = ConfigFactory.from_dict(
        {
            f"{scope}.{Extractor.CONNECTION_NAME_KEY}": connection.name,
            f"{scope}.{Extractor.FILTER_KEY}": connection.filter_key,
            f"{scope}.{Extractor.IS_LOCATION_PARSING_ENABLED_KEY}": connection.is_location_parsing_enabled,
        }
    )

    extractors = [extractor]
    return extractors, conf


def configure_hive_metastore_extractors(connection: ConnectionConfigSchema):
    Extractor = HiveTableMetadataExtractor
    extractor = Extractor()
    scope = extractor.get_scope()
    conn_string_key = get_sql_alchemy_conn_string_key(scope)

    conf = ConfigFactory.from_dict(
        {
            conn_string_key: connection.conn_string,
            f"{scope}.{Extractor.CLUSTER_KEY}": connection.cluster,
            # f"{scope}.{Extractor.DATABASE_KEY}": connection.name,  # TODO: Modify metastore connector to work
            f"{scope}.{Extractor.WHERE_CLAUSE_SUFFIX_KEY}": connection.where_clause_suffix,
        }
    )

    extractors = [extractor]
    return extractors, conf


def configure_presto_extractors(connection: ConnectionConfigSchema):
    Extractor = PrestoTableMetadataExtractor
    extractor = Extractor()
    scope = extractor.get_scope()
    LoopExtractor = PrestoLoopExtractor
    loop_extractor = LoopExtractor()
    loop_scope = loop_extractor.get_scope()

    conn_string_key = get_sql_alchemy_conn_string_key(scope)

    conf = ConfigFactory.from_dict(
        {
            conn_string_key: connection.conn_string,
            f"{loop_scope}.{LoopExtractor.CONN_STRING_KEY}": connection.conn_string,
            f"{loop_scope}.{LoopExtractor.IS_TABLE_METADATA_ENABLED_KEY}": False,
            f"{loop_scope}.{LoopExtractor.IS_WATERMARK_ENABLED_KEY}": False,
            f"{loop_scope}.{LoopExtractor.IS_STATS_ENABLED_KEY}": False,
            f"{loop_scope}.{LoopExtractor.IS_ANALYZE_ENABLED_KEY}": False,
            f"{loop_scope}.{LoopExtractor.DATABASE_KEY}": connection.name,
            f"{loop_scope}.{LoopExtractor.CLUSTER_KEY}": connection.cluster,
            f"{scope}.{Extractor.DATABASE_KEY}": connection.name,
            f"{scope}.{Extractor.CLUSTER_KEY}": connection.cluster,
            f"{scope}.{Extractor.WHERE_CLAUSE_SUFFIX_KEY}": connection.where_clause_suffix,
        }
    )

    extractors = [extractor]
    extractors, conf = add_ugc_runner(extractors, conf, connection)

    return extractors, conf


def configure_neo4j_extractors(connection: ConnectionConfigSchema):
    extractor = AmundsenNeo4jMetadataExtractor()
    scope = extractor.get_scope()
    conf = ConfigFactory.from_dict(
        {
            f"{scope}.graph_url": connection.conn_string,
            f"{scope}.neo4j_auth_user": connection.username,
            f"{scope}.neo4j_auth_pw": connection.password,
            f"{scope}.included_keys": connection.included_keys,
            f"{scope}.excluded_keys": connection.excluded_keys,
            f"{scope}.included_key_regex": connection.included_key_regex,
            f"{scope}.excluded_key_regex": connection.excluded_key_regex,
        }
    )

    extractors = [extractor]

    return extractors, conf


def configure_postgres_extractors(connection: ConnectionConfigSchema):
    Extractor = PostgresMetadataExtractor
    extractor = Extractor()
    scope = extractor.get_scope()
    conn_string_key = get_sql_alchemy_conn_string_key(scope)

    conf = ConfigFactory.from_dict(
        {
            conn_string_key: connection.conn_string,
            f"{scope}.{Extractor.CLUSTER_KEY}": connection.cluster,
            f"{scope}.{Extractor.DATABASE_KEY}": connection.name,
            f"{scope}.{Extractor.WHERE_CLAUSE_SUFFIX_KEY}": connection.where_clause_suffix,
        }
    )

    extractors = [extractor]
    extractors, conf = add_ugc_runner(extractors, conf, connection)
    extractors, conf = add_indexes(extractors, conf, connection)

    return extractors, conf


def configure_redshift_extractors(connection: ConnectionConfigSchema):
    Extractor = RedshiftMetadataExtractor
    extractor = Extractor()
    scope = extractor.get_scope()
    conn_string_key = get_sql_alchemy_conn_string_key(scope)

    conf = ConfigFactory.from_dict(
        {
            conn_string_key: connection.conn_string,
            f"{scope}.{Extractor.CLUSTER_KEY}": connection.cluster,
            f"{scope}.{Extractor.DATABASE_KEY}": connection.name,
            f"{scope}.{Extractor.WHERE_CLAUSE_SUFFIX_KEY}": connection.where_clause_suffix,
        }
    )

    extractors = [extractor]
    extractors, conf = add_ugc_runner(extractors, conf, connection)

    return extractors, conf


def configure_snowflake_extractors(connection: ConnectionConfigSchema):
    Extractor = SnowflakeMetadataExtractor
    extractor = Extractor()
    scope = extractor.get_scope()

    conn_string_key = get_sql_alchemy_conn_string_key(scope)

    conf = ConfigFactory.from_dict(
        {
            conn_string_key: connection.conn_string,
            f"{scope}.{Extractor.DATABASE_KEY}": connection.name,
            f"{scope}.{Extractor.CLUSTER_KEY}": connection.database,
            f"{scope}.{Extractor.WHERE_CLAUSE_SUFFIX_KEY}": connection.where_clause_suffix,
        }
    )

    extractors = [extractor]
    extractors, conf = add_ugc_runner(extractors, conf, connection)

    return extractors, conf


def configure_splice_machine_extractors(connection: ConnectionConfigSchema):
    Extractor = SpliceMachineMetadataExtractor
    extractor = Extractor()
    scope = extractor.get_scope()

    conf = ConfigFactory.from_dict(
        {
            f"{scope}.{Extractor.HOST_KEY}": connection.uri,
            f"{scope}.{Extractor.USERNAME_KEY}": connection.username,
            f"{scope}.{Extractor.PASSWORD_KEY}": connection.password,
            f"{scope}.{Extractor.WHERE_CLAUSE_SUFFIX_KEY}": connection.where_clause_suffix,
        }
    )

    extractors = [extractor]
    # extractors, conf = add_ugc_runner(extractors, conf, connection)

    return extractors, conf


def configure_unscoped_sqlalchemy_engine(connection: ConnectionConfigSchema):
    Engine = SQLAlchemyEngine
    engine = Engine()
    conf = ConfigFactory.from_dict(
        {
            f"{Engine.CONN_STRING_KEY}": connection.conn_string,
            f"{Engine.CREDENTIALS_PATH_KEY}": connection.key_path,
        }
    )

    return engine, conf


def run_build_script(connection: ConnectionConfigSchema):
    if not connection.python_binary:
        python_binary = "python3"
    else:
        python_binary = os.path.expanduser(connection.python_binary)

    venv_path = os.path.expanduser(connection.venv_path)
    build_script_path = os.path.expanduser(connection.build_script_path)

    os.system(
        BUILD_SCRIPT_TEMPLATE.format(
            venv_path=venv_path,
            python_binary=python_binary,
            build_script_path=build_script_path,
        )
    )
