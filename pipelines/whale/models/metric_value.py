import os
from pathlib import Path

from whale.utils import paths
from whale.utils import get_table_file_path_relative


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
