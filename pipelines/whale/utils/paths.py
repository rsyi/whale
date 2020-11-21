from pathlib import Path
import os

BASE_DIR = Path(os.path.join(Path.home(), ".whale/"))
CONFIG_DIR = BASE_DIR / "config/"
CONFIG_PATH = CONFIG_DIR / "config.yaml"
CONNECTION_PATH = CONFIG_DIR / "connections.yaml"
LOGS_DIR = BASE_DIR / "logs/"
MACROS_DIR = BASE_DIR / "macros/"
# MACROS_PATH = MACROS_DIR / "macros."
MANIFEST_DIR = BASE_DIR / "manifests/"
MANIFEST_PATH = MANIFEST_DIR / "manifest.txt"
METADATA_PATH = BASE_DIR / "metadata/"
METRICS_PATH = BASE_DIR / "metrics/"
TMP_MANIFEST_PATH = MANIFEST_DIR / "tmp_manifest.txt"
ETL_LOG_PATH = LOGS_DIR / "cron.log"
TABLE_COUNT_PATH = LOGS_DIR / "table_count.csv"
TEMPLATE_DIR = BASE_DIR / "templates/"


def get_subdir_without_whale(path):
    parts = Path(path).parts
    for i, part in enumerate(parts):
        if part == ".whale":
            first_non_whale_index = i + 1
    return Path(*parts[first_non_whale_index:])
