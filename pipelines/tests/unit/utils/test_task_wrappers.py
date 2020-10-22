import pandas as pd
import pytest
from mock import patch
from whale.engine.sql_alchemy_engine import SQLAlchemyEngine
from whale.utils.task_wrappers import run, pull
from ..fixtures import mock_file_structure
from whale.utils import paths
from whale.utils import task_wrappers


@patch.object(task_wrappers, "get_connection")
@patch.object(SQLAlchemyEngine, "execute")
def test_run(mock_execution, get_connection):
    mock_execution.return_value = iter([["hi"], [0]])
    get_connection.return_value = {
        "metadata_source": "Bigquery",
    }
    sql = "select count(anonymous_id) from `bigquery-sample-no-prod-stuff`.census.button_clicked;"
    df = run(sql)
    assert type(df) == pd.DataFrame
    assert "hi" in df.columns


def test_pull_without_files(mock_file_structure):
    pull()
