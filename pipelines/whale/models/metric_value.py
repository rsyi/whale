import os
import logging
from pathlib import Path
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from whale.utils import paths
from whale.utils import get_table_file_path_relative

LOGGER = logging.getLogger(__name__)


class MetricValue(object):
    """
    Generic stat object.
    """

    def __init__(
        self,
        database: str,
        cluster: str,
        schema: str,
        table: str,
        execution_time: str,
        name: str,
        value: str,
        description: str = None,
        markdown_blob: str = None,
        is_global: bool = False,
    ):
        self.database = database
        self.cluster = cluster
        self.schema = schema
        self.table = table
        self.execution_time = execution_time
        self.description = description
        self.name = name
        self.value = value
        self.is_global = is_global
        self.markdown_blob = markdown_blob

    def record(self):
        relative_file_path = (
            get_table_file_path_relative(
                self.database, self.cluster, self.schema, self.table
            )
            + f"/{self.name}.csv"
        )
        record_path = os.path.join(paths.METRICS_PATH, relative_file_path)
        record_dirname = os.path.dirname(record_path)
        Path(record_dirname).mkdir(parents=True, exist_ok=True)
        with open(record_path, "a") as f:
            f.write(f"{self.value},{self.execution_time}\n")


class SlackAlert(object):
    """
    Sending alerts to the SlackSDK
    """

    def __init__(
        self,
        condition: str,
        message: str,
        channels: str,
    ):
        self.condition = condition
        self.message = message
        self.channels = channels

    def send_slack_alert(self, metric_value):
        if self.condition is None:
            return

        if self.channels is None:
            return

        if self.message is None:
            return

        token = os.environ.get("WHALE_SLACK_TOKEN")
        if not token:
            LOGGER.warning("Could not find environment variable WHALE_SLACK_TOKEN.")
            return

        if not self.evaluate_condition(metric_value):
            return

        client = WebClient(token=token)

        for channel in self.channels:
            try:
                # TODO: Think of a better Slack message than just the message indicated.
                response = client.chat_postMessage(channel=channel, text=self.message)
                assert response["message"]["text"] == self.message

            except SlackApiError as e:
                assert e.response["ok"] is False
                assert e.response["error"]
                LOGGER.warning(f"Got an error: {e.response['error']}")

    def evaluate_condition(self, metric_value):
        try:
            return eval(f"{metric_value} {self.condition}")
        except Exception as e:
            LOGGER.warning(e)
            LOGGER.warning(
                f"Could not evaluate expression `value {self.condition}`, where value is {metric_value}."
            )
            return False
