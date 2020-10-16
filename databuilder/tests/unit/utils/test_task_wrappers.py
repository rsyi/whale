import pytest
from whalebuilder.utils.task_wrappers import create_and_run_tasks_from_yaml
from ..fixtures import mock_file_structure
from whalebuilder.utils import paths

def test_create_and_run_tasks_from_yaml_runs_without_files(mock_file_structure):
    create_and_run_tasks_from_yaml()
