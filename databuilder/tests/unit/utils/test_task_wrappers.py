import pandas as pd
import pytest
from mock import patch
from whalebuilder.engine.sql_alchemy_engine import SQLAlchemyEngine
from whalebuilder.utils.task_wrappers import run, pull
from ..fixtures import mock_file_structure
from whalebuilder.utils import paths


@patch.object(SQLAlchemyEngine, "execute",)
def test_run(mock_execution):
    mock_execution.return_value = iter([["hi"], [0]])
    sql = "select count(anonymous_id) from `bigquery-sample-no-prod-stuff`.census.button_clicked;"
    df = run(sql)
    assert type(df) == pd.DataFrame
    assert "hi" in df.columns


def test_pull_without_files(mock_file_structure):
    pull()
