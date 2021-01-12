import pandas as pd
import pytest
from mock import patch
from whale.engine.sql_alchemy_engine import SQLAlchemyEngine
from whale import run, pull, execute_markdown_sql_blocks, EXECUTION_FLAG
from .fixtures import mock_whale_dir
import whale


def no_op_template(query):
    return query


@patch("google.auth.default", lambda scopes: ["dummy", "dummy"])
@patch.object(whale, "template_query")
@patch.object(whale, "get_connection")
@patch.object(SQLAlchemyEngine, "execute")
def test_run(mock_execution, get_connection, mock_template_query):
    mock_execution.return_value = iter([["hi"], [0]])
    get_connection.return_value = {
        "metadata_source": "bigquery",
    }
    sql = "select count(anonymous_id) from `bigquery-sample-no-prod-stuff`.census.button_clicked;"
    df = run(sql)
    assert type(df) == pd.DataFrame
    assert "hi" in df.columns


def test_pull_without_files(mock_whale_dir):
    pull()


@patch.object(whale, "run")
def test_execute_markdown_sql_blocks_rips_and_replaces(
        mock_run,
        tmp_path):

    MOCK_SQL_RESULTS = pd.DataFrame([0])
    mock_run.return_value = MOCK_SQL_RESULTS

    mock_contents = f"""
# `schema.table`
`db` | `catalog`

## Column details
* [STRING]    `column1`

-------------------------------------------------------------------------------
*Do not make edits above this line.*


```sql
select
{EXECUTION_FLAG}
```
"""

    db_path = tmp_path / "db"
    db_path.mkdir()
    markdown_path = db_path / "catalog.schema.table.md"
    with open(markdown_path, "w") as f:
        f.write(mock_contents)

    execute_markdown_sql_blocks(markdown_path)
    with open(markdown_path, "r") as f:
        contents = f.read()

    assert MOCK_SQL_RESULTS.to_string() in contents
    assert EXECUTION_FLAG not in contents
