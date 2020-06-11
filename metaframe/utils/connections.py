from metaframe.models.connection_config import ConnectionConfigSchema


def dump_connection_config_in_schema(connection):
    connection_config = ConnectionConfigSchema(**connection)
    return connection_config
