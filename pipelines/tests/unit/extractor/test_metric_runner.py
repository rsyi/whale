import logging
import os
import unittest
import pytest

from mock import patch
from pyhocon import ConfigFactory
from databuilder import Scoped

from whale.models.metric_value import MetricValue
from whale.extractor.metric_runner import MetricRunner
from whale.utils import paths
from whale.models.table_metadata import TableMetadata
from whale.loader import whale_loader
from whale.engine.sql_alchemy_engine import SQLAlchemyEngine


logging.basicConfig(level=logging.INFO)


@patch.object(SQLAlchemyEngine, "_get_connection")
@patch.object(SQLAlchemyEngine, "execute")
def test_sends_sql_query_to_sql_alchemy(mock_execution, get_connection, tmp_path):
    database = "mock_database"
    schema = "mock_schema"
    table = "mock_table"
    table_stub_path = os.path.join(tmp_path, database, f"{schema}.{table}.md")

    loader_config = ConfigFactory.from_dict(
        {"table_stub_paths": [table_stub_path], "base_directory": tmp_path}
    )

    extractor_config = ConfigFactory.from_dict(
        {
            MetricRunner.DATABASE_KEY: database,
            "table_stub_paths": [table_stub_path],
        }
    )

    record = TableMetadata(
        database=database,
        cluster=None,
        schema=schema,
        name=table,
    )

    loader = whale_loader.WhaleLoader()
    loader.init(loader_config)
    loader.load(record)

    sql_query = """select count(distinct id) from {schema}.{table} where id is null"""

    add_metrics_to_markdown(table_stub_path, "null-ids", sql_query)

    extractor = MetricRunner()
    extractor.init(extractor_config)
    extractor.extract()
    mock_execution.assert_called_once_with(sql_query, is_dict_return_enabled=False)


def add_metrics_to_markdown(file_path, metric_name, sql_query):

    with open(file_path, "a") as f:
        f.write(
            f"""
```metrics
{metric_name}:
  sql: |
    {sql_query}"""
        )
