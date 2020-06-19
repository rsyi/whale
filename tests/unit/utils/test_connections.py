from metaframe.utils.connections import dump_connection_config_in_schema
from metaframe.models.connection_config import ConnectionConfigSchema

TEST_CONNECTION = {
    'type': 'build_script'
}


def test_dump_connection_config_in_schema():
    result = dump_connection_config_in_schema(TEST_CONNECTION)
    assert type(result) == ConnectionConfigSchema
    assert result.type == TEST_CONNECTION['type']
