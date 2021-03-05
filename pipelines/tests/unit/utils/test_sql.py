from ..fixtures import mock_whale_dir, mock_template_dir
from pathlib import Path
from mock import call, patch
import pytest

from whale.utils.sql import (
    template_query,
    _validate_and_print_result,
)

VALID_TEMPLATE = "{% set main = 5 %}"
VALID_FILE_NAME = "valid.sql"
INVALID_TEMPLATE = "{% notvalidtext %}"
INVALID_FILE_NAME = "invalid.sql"
QUERY_WITHOUT_TEMPLATE = "hello"


def write_template_to_file(path: Path, content: str):
    with open(path, "w") as f:
        f.write(content)


class TestTemplateDirectory:
    @pytest.fixture(autouse=True)
    def setup(self, mock_template_dir):
        mock_template_dir.mkdir(exist_ok=True)
        write_template_to_file(mock_template_dir / VALID_FILE_NAME, VALID_TEMPLATE)
        write_template_to_file(mock_template_dir / INVALID_FILE_NAME, INVALID_TEMPLATE)

    @patch("builtins.print")
    def test__validate_and_print_result_returns_correct_passing_result(
        self, mock_print, mock_template_dir
    ):
        error = _validate_and_print_result(mock_template_dir / VALID_FILE_NAME)
        mock_print.assert_called_with("[\x1b[32m✓\x1b[0m] valid.sql")
        assert error is None

    @patch("builtins.print")
    def test__validate_and_print_result_returns_correct_failing_result(
        self, mock_print, mock_template_dir
    ):
        error = _validate_and_print_result(mock_template_dir / INVALID_FILE_NAME)
        mock_print.assert_has_calls([call("[\x1b[31mx\x1b[0m] invalid.sql")]),
        assert error is not None

    def test_template_query_is_no_op_without_jinja(self):
        templated_query = template_query(QUERY_WITHOUT_TEMPLATE)
        assert templated_query == QUERY_WITHOUT_TEMPLATE

    def test_template_query_templates_empty_correctly(self):
        templated_query = template_query(VALID_TEMPLATE)
        assert templated_query == ""

    def test_template_query_templates_correctly_with_file(self):
        query = "{{ main }}"
        templated_query = template_query(
            query,
            connection_name="valid",
        )
        assert templated_query == "\n5"
