import logging
import os
import unittest
import pytest

from pyhocon import ConfigFactory
from typing import Dict, Iterable, Any, Callable  # noqa: F401

from whale.models.table_metadata import TableMetadata
from whale.loader import whale_loader
from whale.utils import paths


@pytest.fixture
def patched_config(tmp_path):
    metadata_subpath = paths.get_subdir_without_whale(str(paths.METADATA_PATH))
    tmp_manifest_subpath = paths.get_subdir_without_whale(str(paths.TMP_MANIFEST_PATH))
    new_metadata_path = tmp_path / metadata_subpath
    new_metadata_path.mkdir()
    new_tmp_manifest_path = tmp_path / tmp_manifest_subpath
    new_tmp_manifest_path.parent.mkdir()

    return ConfigFactory.from_dict(
        {
            "base_directory": tmp_path / metadata_subpath,
            "tmp_manifest_path": tmp_path / tmp_manifest_subpath,
        }
    )


def test_load_no_catalog(patched_config):
    record = TableMetadata(
        database="mock_database",
        cluster=None,
        schema="mock_schema",
        name="mock_table",
    )
    loader = whale_loader.WhaleLoader()
    loader.init(patched_config)
    loader.load(record)

    loader.close()
    file_path = os.path.join(
        patched_config.get("base_directory"), "mock_database/mock_schema.mock_table.md"
    )
    with open(file_path, "r") as f:
        written_record = f.read()

    assert "mock_schema" in written_record
    assert "mock_table" in written_record
    assert "mock_database" in written_record


def test_load_catalog_specified(patched_config):
    record = TableMetadata(
        database="mock_database",
        cluster="mock_catalog",
        schema="mock_schema",
        name="mock_table",
    )
    loader = whale_loader.WhaleLoader()
    loader.init(patched_config)
    loader.load(record)

    loader.close()
    file_path = os.path.join(
        patched_config.get("base_directory"),
        "mock_database/mock_catalog.mock_schema.mock_table.md",
    )
    with open(file_path, "r") as f:
        written_record = f.read()

    assert "mock_schema" in written_record
    assert "mock_table" in written_record
    assert "mock_catalog" in written_record
    assert "mock_database" in written_record
