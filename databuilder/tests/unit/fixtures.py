import pytest
from whalebuilder.utils import paths

@pytest.fixture()
def mock_file_structure(monkeypatch, tmp_path):
    for attr_name, path in {
        "BASE_DIR": paths.BASE_DIR,
        "CONFIG_DIR": paths.CONFIG_DIR,
        "CONNECTION_PATH": paths.CONNECTION_PATH,
        "LOGS_DIR": paths.LOGS_DIR,
        "MANIFEST_DIR": paths.MANIFEST_DIR,
        "MANIFEST_PATH": paths.MANIFEST_PATH,
        "METRICS_PATH": paths.METRICS_PATH,
        "METADATA_PATH": paths.METADATA_PATH,
    }.items():
        basename = paths.get_subdir_without_whale(path)
        d = tmp_path / basename
        monkeypatch.setattr(paths, attr_name, d)
