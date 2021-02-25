import boto3
import google
import googleapiclient
import google_auth_httplib2
import http

from mock import patch

from databuilder import Scoped
from whale.models.connection_config import ConnectionConfigSchema
from whale.engine.sql_alchemy_engine import SQLAlchemyEngine
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from whale.extractor.bigquery_metadata_extractor import BaseBigQueryExtractor
from whale.extractor.base_postgres_metadata_extractor import BasePostgresMetadataExtractor
from whale.utils.extractor_wrappers import (
    configure_bigquery_extractors,
    configure_glue_extractors,
    configure_neo4j_extractors,
    configure_presto_extractors,
    configure_postgres_extractors,
    configure_redshift_extractors,
    configure_snowflake_extractors,
)
from databuilder.extractor.neo4j_extractor import Neo4jExtractor

TEST_PRESTO_CONNECTION_CONFIG = ConnectionConfigSchema(
    name="test_connection",
    username="mock_username",
    password="mock_password",
    uri="mock_uri",
    port="9999",
    metadata_source="presto",
    cluster="mock_cluster",
    included_schemas="mock_schema_included",
    excluded_schemas="mock_schema_excluded",
)

TEST_BIGQUERY_CONNECTION_CONFIG = ConnectionConfigSchema(
    metadata_source="bigquery",
    key_path="mock_key_path",
    project_id="mock_project_id",
    page_size=10,
    filter_key="mock_filter_key",
)

TEST_GLUE_CONNECTION_CONFIG = ConnectionConfigSchema(
    metadata_source="glue",
    filter_key="mock_filter",
)

TEST_NEO4J_CONNECTION_CONFIG = ConnectionConfigSchema(
    name="test_connection",
    username="mock_username",
    password="mock_password",
    uri="mock_uri",
    port="9999",
    metadata_source="presto",
    cluster="mock_cluster",
    included_keys=["mock_key_included"],
    excluded_keys=["mock_key_excluded"],
)

TEST_POSTGRES_CONNECTION_CONFIG = ConnectionConfigSchema(
    name="test_connection",
    username="mock_username",
    password="mock_password",
    uri="mock_uri",
    port="9999",
    metadata_source="postgres",
    cluster="mock_cluster",
)


# @patch.object(SQLAlchemyEngine, '_get_connection')
@patch.object(SQLAlchemyExtractor, "_execute_query")
def test_configure_presto_extractor(mock_settings):
    """
    Test that the extractor can initialize.
    """
    extractors, conf = configure_presto_extractors(TEST_PRESTO_CONNECTION_CONFIG)
    extractor = extractors[0]
    scoped_conf = Scoped.get_scoped_conf(conf, extractor.get_scope())
    assert extractor.init(scoped_conf) == None


@patch.object(BaseBigQueryExtractor, "init")
def test_configure_bigquery_extractor(*mock_settings):
    """
    Test that the extractor can initialize.
    """
    extractors, conf = configure_bigquery_extractors(TEST_BIGQUERY_CONNECTION_CONFIG)
    extractor = extractors[0]
    scoped_conf = Scoped.get_scoped_conf(conf, extractor.get_scope())
    assert extractor.init(scoped_conf) == None


@patch.object(boto3, "client")
def test_configure_glue_extractor(*mock_settings):
    """
    Test that the extractor can initialize.
    """
    extractors, conf = configure_glue_extractors(TEST_GLUE_CONNECTION_CONFIG)
    extractor = extractors[0]
    scoped_conf = Scoped.get_scoped_conf(conf, extractor.get_scope())
    assert extractor.init(scoped_conf) == None


@patch.object(SQLAlchemyExtractor, "_execute_query")
@patch.object(SQLAlchemyExtractor, "_get_connection")
def test_configure_postgres_extractor(*mock_settings):
    """
    Test that the extractor can initialize.
    """
    for configurer in [configure_postgres_extractors, configure_redshift_extractors]:
        extractors, conf = configurer(TEST_POSTGRES_CONNECTION_CONFIG)
        extractor = extractors[0]
        scoped_conf = Scoped.get_scoped_conf(conf, extractor.get_scope())
        assert extractor.init(scoped_conf) == None


@patch.object(Neo4jExtractor, "_get_driver")
def test_configure_neo4j_extractor(mock_settings):
    """
    Test that the extractor can initialize.
    """
    extractors, conf = configure_neo4j_extractors(TEST_NEO4J_CONNECTION_CONFIG)
    extractor = extractors[0]
    scoped_conf = Scoped.get_scoped_conf(conf, extractor.get_scope())
    assert extractor.init(scoped_conf) == None
