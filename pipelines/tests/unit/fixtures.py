from pathlib import Path
import pytest
from whale.utils import paths
from whale.utils import sql


def get_mocked_path(mocked_base_dir, path_to_mock: Path):
    basename = paths.get_subdir_without_whale(path_to_mock)
    return mocked_base_dir / basename


@pytest.fixture(params=[True, False])
def mock_whale_dir(monkeypatch, tmp_path, request):
    is_path_creation_enabed = request.param
    for attr_name, path in {
        "BASE_DIR": paths.BASE_DIR,
        "CONFIG_DIR": paths.CONFIG_DIR,
        "CONNECTION_PATH": paths.CONNECTION_PATH,
        "LOGS_DIR": paths.LOGS_DIR,
        "MANIFEST_DIR": paths.MANIFEST_DIR,
        "MANIFEST_PATH": paths.MANIFEST_PATH,
        "METRICS_PATH": paths.METRICS_PATH,
        "METADATA_PATH": paths.METADATA_PATH,
        "TEMPLATE_DIR": paths.TEMPLATE_DIR,
    }.items():
        d = get_mocked_path(tmp_path, path)
        monkeypatch.setattr(paths, attr_name, d)

        if is_path_creation_enabed:
            if d.is_dir():
                d.parent.mkdir(parents=True, exist_ok=True)
            elif d.is_file():
                d.touch(exist_ok=True)
        if attr_name in ["TEMPLATE_DIR"]:
            monkeypatch.setattr(sql, attr_name, d)
    return tmp_path


@pytest.mark.parametrize(mock_whale_dir, [True], indirect=True)
@pytest.fixture()
def mock_template_dir(mock_whale_dir):
    return mock_whale_dir / paths.TEMPLATE_DIR.parts[-1]
