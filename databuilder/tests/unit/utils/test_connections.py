from whalebuilder.utils.connections import dump_connection_config_in_schema
from whalebuilder.models.connection_config import ConnectionConfigSchema

TEST_CONNECTION = {
    'type': 'build_script',
    'metadata_source': 'bigquery',
}


def test_dump_connection_config_in_schema():
    result = dump_connection_config_in_schema(TEST_CONNECTION)
    assert type(result) == ConnectionConfigSchema
    assert result.metadata_source == TEST_CONNECTION['metadata_source']
