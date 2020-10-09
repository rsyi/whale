import os

from pyhocon import ConfigFactory
from whalebuilder.extractor.presto_loop_extractor import PrestoLoopExtractor
from whalebuilder.extractor.presto_table_metadata_extractor import PrestoTableMetadataExtractor
from whalebuilder.models.connection_config import ConnectionConfigSchema
from whalebuilder.extractor.amundsen_neo4j_metadata_extractor \
    import AmundsenNeo4jMetadataExtractor
from whalebuilder.extractor.bigquery_metadata_extractor \
    import BigQueryMetadataExtractor
from whalebuilder.extractor.bigquery_watermark_extractor \
    import BigQueryWatermarkExtractor
from whalebuilder.extractor.snowflake_metadata_extractor \
    import SnowflakeMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.extractor.postgres_metadata_extractor import PostgresMetadataExtractor
from databuilder.extractor.redshift_metadata_extractor import RedshiftMetadataExtractor


BUILD_SCRIPT_TEMPLATE = \
    """source {venv_path}/bin/activate \
    && {python_binary} {build_script_path}"""
SQL_ALCHEMY_SCOPE = SQLAlchemyExtractor().get_scope()


def get_sql_alchemy_conn_string_key(scope):
    conn_string_key = '{}.{}.{}'\
        .format(scope, SQL_ALCHEMY_SCOPE, SQLAlchemyExtractor.CONN_STRING)
    return conn_string_key


def format_conn_string(connection: ConnectionConfigSchema):
    username_password_placeholder = \
        '{}:{}'.format(connection.username, connection.password) \
        if connection.password is not None else ''

    if connection.metadata_source in ["redshift"]:
        connection_type = "postgres"
    else:
        connection_type = connection.metadata_source

    conn_string = \
        '{connection_type}://{username_password}@{uri}:{port}'.format(
            connection_type=connection_type,
            username_password=username_password_placeholder,
            uri=connection.uri,
            port=connection.port)
    return conn_string


def configure_bigquery_extractors(connection: ConnectionConfigSchema):
    extractor = BigQueryMetadataExtractor()
    scope = extractor.get_scope()
    watermark_extractor = BigQueryWatermarkExtractor()
    watermark_scope = watermark_extractor.get_scope()
    conf = ConfigFactory.from_dict({
        '{}.key_path'.format(scope): connection.key_path,
        '{}.project_id'.format(scope): connection.project_id,
        '{}.project_credentials'.format(scope): connection.project_credentials,
        '{}.page_size'.format(scope): connection.page_size,
        '{}.filter_key'.format(scope): connection.filter_key,
        '{}.included_tables_regex'.format(scope): connection.included_tables_regex,
        '{}.key_path'.format(watermark_scope): connection.key_path,
        '{}.project_id'.format(watermark_scope): connection.project_id,
        '{}.project_credentials'.format(watermark_scope): connection.project_credentials,
        '{}.included_tables_regex'.format(watermark_scope): connection.included_tables_regex,
    })

    extractors = [extractor, watermark_extractor]

    return extractors, conf


def configure_presto_extractors(connection: ConnectionConfigSchema):
    extractor = PrestoTableMetadataExtractor()
    scope = extractor.get_scope()
    loop_extractor = PrestoLoopExtractor()
    loop_scope = loop_extractor.get_scope()

    conn_string_key = get_sql_alchemy_conn_string_key(scope)
    loop_conn_string_key = '{}.conn_string'.format(loop_scope)
    conn_string = format_conn_string(connection)

    conf = ConfigFactory.from_dict({
        conn_string_key: conn_string,
        loop_conn_string_key: conn_string,
        '{}.is_table_metadata_enabled'.format(loop_scope): False,
        '{}.is_watermark_enabled'.format(loop_scope): False,
        '{}.is_stats_enabled'.format(loop_scope): False,
        '{}.is_analyze_enabled'.format(loop_scope): False,
        '{}.database'.format(loop_scope): connection.name,
        '{}.cluster'.format(loop_scope): connection.cluster,
        '{}.database'.format(scope): connection.name,
        '{}.cluster'.format(scope): connection.cluster,
        '{}.where_clause_suffix'.format(scope): connection.where_clause_suffix,
    })

    extractors = [extractor]

    return extractors, conf


def configure_neo4j_extractors(connection: ConnectionConfigSchema):
    extractor = AmundsenNeo4jMetadataExtractor()
    scope = extractor.get_scope()
    conn_string = 'bolt://{uri}:{port}'.format(
        uri=connection.uri,
        port=connection.port)
    conf = ConfigFactory.from_dict({
        '{}.graph_url'.format(scope): conn_string,
        '{}.neo4j_auth_user'.format(scope): connection.username,
        '{}.neo4j_auth_pw'.format(scope): connection.password,
        '{}.included_keys'.format(scope): connection.included_keys,
        '{}.excluded_keys'.format(scope): connection.excluded_keys,
        '{}.included_key_regex'.format(scope): connection.included_key_regex,
        '{}.excluded_key_regex'.format(scope): connection.excluded_key_regex,
    })

    extractors = [extractor]

    return extractors, conf


def configure_postgres_extractors(connection: ConnectionConfigSchema):
    Extractor = PostgresMetadataExtractor
    extractor = Extractor()
    scope = extractor.get_scope()
    conn_string_key = get_sql_alchemy_conn_string_key(scope)
    conn_string = format_conn_string(connection)

    conf = ConfigFactory.from_dict({
        conn_string_key: conn_string,
        "{}.{}".format(scope, Extractor.CLUSTER_KEY): connection.cluster,
        "{}.{}".format(scope, Extractor.DATABASE_KEY): connection.name,
        "{}.{}".format(scope, Extractor.WHERE_CLAUSE_SUFFIX_KEY): connection.where_clause_suffix,
    })

    extractors = [extractor]
    return extractors, conf



def configure_redshift_extractors(connection: ConnectionConfigSchema):
    Extractor = RedshiftMetadataExtractor
    extractor = Extractor()
    scope = extractor.get_scope()
    conn_string_key = get_sql_alchemy_conn_string_key(scope)
    conn_string = format_conn_string(connection)

    conf = ConfigFactory.from_dict({
        conn_string_key: conn_string,
        "{}.{}".format(scope, Extractor.CLUSTER_KEY): connection.cluster,
        "{}.{}".format(scope, Extractor.DATABASE_KEY): connection.name,
        "{}.{}".format(scope, Extractor.WHERE_CLAUSE_SUFFIX_KEY): connection.where_clause_suffix,
    })

    extractors = [extractor]
    return extractors, conf


def configure_snowflake_extractors(connection: ConnectionConfigSchema):
    extractor = SnowflakeMetadataExtractor()
    scope = extractor.get_scope()

    conn_string_key = get_sql_alchemy_conn_string_key(scope)
    conn_string = format_conn_string(connection)

    conf = ConfigFactory.from_dict({
        conn_string_key: conn_string,
        '{}.database'.format(scope): connection.name,
        '{}.cluster'.format(scope): connection.cluster,
    })

    extractors = [extractor]

    return extractors, conf


def run_build_script(connection: ConnectionConfigSchema):
    if not connection.python_binary:
        python_binary = 'python3'
    else:
        python_binary = os.path.expanduser(connection.python_binary)

    venv_path = os.path.expanduser(connection.venv_path)
    build_script_path = os.path.expanduser(connection.build_script_path)

    os.system(BUILD_SCRIPT_TEMPLATE.format(
        venv_path=venv_path,
        python_binary=python_binary,
        build_script_path=build_script_path
    ))
