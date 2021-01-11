import logging
import os
from datetime import datetime
from pyhocon import ConfigFactory
import subprocess
import yaml
from databuilder import Scoped

from whale.engine.sql_alchemy_engine import SQLAlchemyEngine
from whale.utils.paths import METADATA_PATH
from whale.utils import get_table_info_from_path
from whale.utils.sql import template_query
from whale.utils.parsers import (
    parse_ugc,
    sections_from_markdown,
    DEFINED_METRICS_SECTION,
)
from whale.utils.markdown_delimiters import METRICS_DELIMITER
from whale.models.metric_value import MetricValue, SlackAlert

SQLALCHEMY_ENGINE_SCOPE = SQLAlchemyEngine().get_scope()
SQLALCHEMY_CONN_STRING_KEY = (
    f"{SQLALCHEMY_ENGINE_SCOPE}.{SQLAlchemyEngine.CONN_STRING_KEY}"
)

LOGGER = logging.getLogger(__name__)


class UGCRunner(SQLAlchemyEngine):

    DATABASE_KEY = "database"
    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            "table_stub_paths": None,
            SQLALCHEMY_CONN_STRING_KEY: None,
        }
    )

    def init(self, conf):
        self.conf = conf.with_fallback(self.DEFAULT_CONFIG)
        self.sql_alch_conf = Scoped.get_scoped_conf(self.conf, SQLALCHEMY_ENGINE_SCOPE)

        self.database = self.conf.get(UGCRunner.DATABASE_KEY)
        table_stub_paths = self.conf.get("table_stub_paths")
        if table_stub_paths is None:
            self.table_stub_paths = self._find_all_table_stub_paths()
        else:
            self.table_stub_paths = table_stub_paths

        self._extract_iter = None

    def extract(self):
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            extraction = next(self._extract_iter)
            return extraction
        except StopIteration:
            return None

    def _find_all_table_stub_paths(self) -> list:
        try:
            results = subprocess.check_output(
                f"grep -l '{METRICS_DELIMITER}' ~/.whale/metadata/{self.database}/* -d skip",
                shell=True,
            )
            results = results.decode("utf-8")
            table_stubs_with_metrics = results.split("\n")[:-1]
            table_stub_paths = [
                os.path.join(METADATA_PATH, table_stub)
                for table_stub in table_stubs_with_metrics
            ]
        except subprocess.CalledProcessError:
            table_stub_paths = []
        return table_stub_paths

    def _get_extract_iter(self):
        # Loop through all table stubs that contain ```metrics
        if self.table_stub_paths:
            super().init(self.sql_alch_conf)
        for table_stub_path in self.table_stub_paths:
            database, cluster, schema, table = get_table_info_from_path(table_stub_path)
            metric_yamls = self._get_metrics_queries_from_table_stub_path(
                table_stub_path
            )

            # Get all ```metrics definitions in each file (there can be multiple)
            for metric_yaml in metric_yamls:
                metric_yaml = yaml.safe_load(metric_yaml)

                # Loop through all metrics defined in this ```metrics section.
                for metric_name, metric_details in metric_yaml.items():
                    execution_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    sql_result = self._compute_sql_result(
                        metric_details["sql"], database
                    )

                    description = metric_details.get("description")
                    is_global = metric_details.get("is_global", False)
                    alerts = metric_details.get("alerts")

                    self._send_slack_alerts(alerts, sql_result)

                    yield MetricValue(
                        database=database,
                        cluster=cluster,
                        schema=schema,
                        table=table,
                        name=metric_name,
                        description=description,
                        execution_time=execution_time,
                        value=sql_result,
                        is_global=is_global,
                    )

    def _compute_sql_result(self, sql_query, database):
        connected_query = template_query(sql_query, connection_name=database)
        result = list(self.execute(connected_query, is_dict_return_enabled=False))

        try:
            return result[0][0]
        except Exception as e:
            LOGGER.warning("Running {sql_query} led to {e}.")
            return None

    def _get_metrics_queries_from_table_stub_path(self, table_stub_path):
        sections = sections_from_markdown(table_stub_path)
        ugc_sections = parse_ugc(sections["ugc"])
        return ugc_sections[DEFINED_METRICS_SECTION]

    def _send_slack_alerts(self, alerts, sql_result):
        if not alerts:
            return

        alert_list = alerts if isinstance(alerts, list) else [alerts]

        for alert in alert_list:
            slack_alert = SlackAlert(
                condition=alert["condition"],
                message=alert["message"],
                channels=alert["slack"],
            )

            slack_alert.send_slack_alert(sql_result)

    def get_scope(self):
        return "extractor.markdown_metric"
