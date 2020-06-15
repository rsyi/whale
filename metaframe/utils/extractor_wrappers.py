import os

from pyhocon import ConfigFactory
from metaframe.extractor.presto_loop_extractor import PrestoLoopExtractor
from metaframe.models.connection_config import ConnectionConfigSchema
from metaframe.extractor.amundsen_neo4j_metadata_extractor \
    import AmundsenNeo4jMetadataExtractor


BUILD_SCRIPT_TEMPLATE = \
    """source {venv_path}/bin/activate \
    && {python_binary} {build_script_path}"""


def configure_presto_extractor(
        connection: ConnectionConfigSchema,
        is_full_extraction_enabled: bool = False):
    extractor = PrestoLoopExtractor()
    scope = extractor.get_scope()
    conn_string_key = '{}.conn_string'.format(scope)

    username_password_placeholder = \
        '{}:{}'.format(connection.username, connection.password) \
        if connection.password is not None else ''

    conn_string = '{connection_type}://{username_password}{host}'.format(
        connection_type=connection.type,
        username_password=username_password_placeholder,
        host=connection.host)

    conf = ConfigFactory.from_dict({
        conn_string_key: conn_string,
        'extractor.presto_loop.is_table_metadata_enabled': True,
        'extractor.presto_loop.is_full_extraction_enabled':
            is_full_extraction_enabled,
        'extractor.presto_loop.is_watermark_enabled': False,
        'extractor.presto_loop.is_stats_enabled': False,
        'extractor.presto_loop.is_analyze_enabled': False,
        'extractor.presto_loop.database': connection.name,
        'extractor.presto_loop.cluster': connection.cluster,
        'extractor.presto_loop.included_schemas': connection.included_schemas,
        'extractor.presto_loop.excluded_schemas': connection.excluded_schemas,
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
