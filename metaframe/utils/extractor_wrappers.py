import os

from pyhocon import ConfigFactory
from metaframe.extractor.presto_loop_extractor import PrestoLoopExtractor
from metaframe.models.connection_config import ConnectionConfigSchema
from metaframe.extractor.amundsen_neo4j_metadata_extractor \
    import AmundsenNeo4jMetadataExtractor
from metaframe.extractor.bigquery_metadata_extractor \
    import BigQueryMetadataExtractor
from metaframe.extractor.snowflake_metadata_extractor \
    import SnowflakeMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor


BUILD_SCRIPT_TEMPLATE = \
    """source {venv_path}/bin/activate \
    && {python_binary} {build_script_path}"""
SQL_ALCHEMY_SCOPE = SQLAlchemyExtractor().get_scope()


def configure_bigquery_extractor(connection: ConnectionConfigSchema):
    extractor = BigQueryMetadataExtractor()
    scope = extractor.get_scope()
    conf = ConfigFactory.from_dict({
        '{}.key_path'.format(scope): connection.key_path,
        '{}.project_id'.format(scope): connection.project_id,
        '{}.project_credentials'.format(scope): connection.project_credentials,
        '{}.page_size'.format(scope): connection.page_size,
        '{}.filter_key'.format(scope): connection.filter_key,
    })

    return extractor, conf


def configure_presto_extractor(
        connection: ConnectionConfigSchema,
        is_full_extraction_enabled: bool = False):
    extractor = PrestoLoopExtractor()
    scope = extractor.get_scope()
    conn_string_key = '{}.conn_string'.format(scope)

    username_password_placeholder = \
        '{}:{}'.format(connection.username, connection.password) \
        if connection.password is not None else ''

    conn_string = '{connection_type}://{username_password}@{host}'.format(
        connection_type=connection.type,
        username_password=username_password_placeholder,
        host=connection.host)

    conf = ConfigFactory.from_dict({
        conn_string_key: conn_string,
        '{}.is_table_metadata_enabled'.format(scope): True,
        '{}.is_full_extraction_enabled'.format(scope):
            is_full_extraction_enabled,
        '{}.is_watermark_enabled'.format(scope): False,
        '{}.is_stats_enabled'.format(scope): False,
        '{}.is_analyze_enabled'.format(scope): False,
        '{}.database'.format(scope): connection.name,
        '{}.catalog'.format(scope): connection.catalog,
        '{}.included_schemas'.format(scope): connection.included_schemas,
        '{}.excluded_schemas'.format(scope): connection.excluded_schemas,
    })

    return extractor, conf


def configure_neo4j_extractor(connection: ConnectionConfigSchema):
    extractor = AmundsenNeo4jMetadataExtractor()
    scope = extractor.get_scope()
    conf = ConfigFactory.from_dict({
        '{}.graph_url'.format(scope): 'bolt://' + connection.host,
        '{}.neo4j_auth_user'.format(scope): connection.username,
        '{}.neo4j_auth_pw'.format(scope): connection.password,
        '{}.included_keys'.format(scope): connection.included_keys,
        '{}.excluded_keys'.format(scope): connection.excluded_keys,
        '{}.included_key_regex'.format(scope): connection.included_key_regex,
        '{}.excluded_key_regex'.format(scope): connection.excluded_key_regex,
    })

    return extractor, conf


def configure_snowflake_extractor(connection: ConnectionConfigSchema):
    extractor = SnowflakeMetadataExtractor()
    scope = extractor.get_scope()

    conn_string_key = '{}.{}.conn_string'\
        .format(scope, SQL_ALCHEMY_SCOPE)

    username_password_placeholder = \
        '{}:{}'.format(connection.username, connection.password) \
        if connection.password is not None else ''

    conn_string = '{connection_type}://{username_password}@{host}'.format(
        connection_type=connection.type,
        username_password=username_password_placeholder,
        host=connection.host)

    conf = ConfigFactory.from_dict({
        conn_string_key: conn_string,
        '{}.database'.format(scope): connection.name,
        '{}.catalog'.format(scope): connection.catalog,
    })

    return extractor, conf


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
