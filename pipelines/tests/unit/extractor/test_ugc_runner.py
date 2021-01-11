import logging
import os
import unittest
import pytest
import yaml
import tempfile

from slack_sdk import WebClient
from mock import patch, call
from pyhocon import ConfigFactory
from databuilder import Scoped

from whale.models.metric_value import SlackAlert
from whale.extractor.ugc_runner import UGCRunner
from whale.utils import paths
from whale.models.table_metadata import TableMetadata
from whale.loader import whale_loader
from whale.engine.sql_alchemy_engine import SQLAlchemyEngine
from whale.utils.markdown_delimiters import DEFINED_METRICS_DELIMITER


@patch.object(WebClient, "chat_postMessage")
@patch.object(SQLAlchemyEngine, "_get_connection")
@patch.object(SQLAlchemyEngine, "execute")
class TestMetricRunner(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)
        self.database = "mock_database"
        self.schema = "mock_schema"
        self.table = "mock_table"

        tmp_path = tempfile.NamedTemporaryFile()
        self.table_stub_path = os.path.join(
            tmp_path.name, self.database, f"{self.schema}.{self.table}.md"
        )

        self.loader_config = ConfigFactory.from_dict(
            {
                "table_stub_paths": [self.table_stub_path],
                "base_directory": tmp_path.name,
            }
        )

        self.extractor_config = ConfigFactory.from_dict(
            {
                UGCRunner.DATABASE_KEY: self.database,
                "table_stub_paths": [self.table_stub_path],
            }
        )

        self.record = TableMetadata(
            database=self.database,
            cluster=None,
            schema=self.schema,
            name=self.table,
        )

    @pytest.fixture(autouse=True)
    def mock_env_token(self, monkeypatch):
        monkeypatch.setenv("WHALE_SLACK_TOKEN", "test-token")

    def test_sends_sql_query_to_sql_alchemy(
        self, mock_execution, get_connection, mock_slack_client
    ) -> None:
        mock_execution.return_value = [(0,)]

        mock_slack_client.return_value = {"message": {"text": "Placeholder"}}

        loader = whale_loader.WhaleLoader()
        loader.init(self.loader_config)
        loader.load(self.record)

        sql_query = f"select count(distinct id) from {self.schema}.{self.table} where id is null"

        slack_alerts = [
            SlackAlert(
                condition=">0",
                message=f"Nulls found in {self.schema}.{self.table} column id.",
                channels=["#data-monitoring", "@bob"],
            ),
        ]

        self.add_metrics_and_alerts_to_markdown("null-ids", sql_query, slack_alerts)

        extractor = UGCRunner()
        extractor.init(self.extractor_config)
        extractor.extract()
        mock_execution.assert_called_once_with(
            f" | {sql_query}", is_dict_return_enabled=False
        )

    def test_sends_alerts_to_slack_sdk(
        self,
        mock_execution,
        get_connection,
        mock_slack_client,
    ):
        mock_execution.return_value = [(100,)]

        loader = whale_loader.WhaleLoader()
        loader.init(self.loader_config)
        loader.load(self.record)

        sql_query = f"select count(distinct id) from {self.schema}.{self.table} where id is null"
        alert_message = f"Nulls found in {self.schema}.{self.table} column id."

        slack_alerts = [
            SlackAlert(
                condition=">0",
                message=alert_message,
                channels=["#data-monitoring", "@bob"],
            ),
            SlackAlert(
                condition=">50",
                message=alert_message,
                channels=["#incident-room", "@joseph"],
            ),
        ]

        self.add_metrics_and_alerts_to_markdown("null-ids", sql_query, slack_alerts)

        mock_slack_client.return_value = {"message": {"text": alert_message}}

        extractor = UGCRunner()
        extractor.init(self.extractor_config)

        results = extractor.extract()

        calls = [
            call(channel="#data-monitoring", text=alert_message),
            call(channel="@bob", text=alert_message),
            call(channel="#incident-room", text=alert_message),
            call(channel="@joseph", text=alert_message),
        ]
        mock_slack_client.assert_has_calls(calls, any_order=True)

    def test_handles_invalid_slack_alerts_and_skips_if_condition_is_false(
        self,
        mock_execution,
        get_connection,
        mock_slack_client,
    ):
        mock_execution.return_value = [(5,)]

        loader = whale_loader.WhaleLoader()
        loader.init(self.loader_config)
        loader.load(self.record)

        sql_query = f"select count(distinct id) from {self.schema}.{self.table} where id is null"
        alert_message = f"Nulls found in {self.schema}.{self.table} column id."

        slack_alerts = [
            SlackAlert(
                condition=">0",
                message=alert_message,
                channels=["#data-monitoring", "@bob"],
            ),
            SlackAlert(
                condition="not a valid condition",
                message=alert_message,
                channels=["@jack"],
            ),
            SlackAlert(
                condition=">2",
                message=alert_message,
                channels=[],
            ),
        ]

        self.add_metrics_and_alerts_to_markdown("null-ids", sql_query, slack_alerts)

        mock_slack_client.return_value = {"message": {"text": alert_message}}

        extractor = UGCRunner()
        extractor.init(self.extractor_config)

        results = extractor.extract()

        calls = [
            call(channel="#data-monitoring", text=alert_message),
            call(channel="@bob", text=alert_message),
        ]
        mock_slack_client.assert_has_calls(calls, any_order=True)

    def add_metrics_and_alerts_to_markdown(
        self,
        metric_name,
        sql_query,
        slack_alerts: list = [],
    ):
        yaml_dict = {
            metric_name: {
                "sql": f" | {sql_query}",
            }
        }

        if slack_alerts:
            yaml_dict[metric_name]["alerts"] = [
                {
                    "condition": alert.condition,
                    "message": alert.message,
                    "slack": alert.channels,
                }
                for alert in slack_alerts
            ]

        with open(self.table_stub_path, "a") as f:
            f.write(
                f"""
{DEFINED_METRICS_DELIMITER}
{yaml.dump(yaml_dict, sort_keys=False)}"""
            )
