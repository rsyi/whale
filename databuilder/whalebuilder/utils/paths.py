from pathlib import Path
import os

BASE_DIR = os.path.join(Path.home(), ".whale/")
CONFIG_DIR = os.path.join(BASE_DIR, "config/")
CONFIG_PATH = os.path.join(CONFIG_DIR, "config.yaml")
CONNECTION_PATH = os.path.join(CONFIG_DIR, "connections.yaml")
LOGS_DIR = os.path.join(BASE_DIR, "logs/")
MANIFEST_DIR = os.path.join(BASE_DIR, "manifests/")
MANIFEST_PATH = os.path.join(MANIFEST_DIR, "manifest.txt")
METADATA_PATH = os.path.join(BASE_DIR, "metadata/")
METRICS_PATH = os.path.join(BASE_DIR, "metrics/")
TMP_MANIFEST_PATH = os.path.join(MANIFEST_DIR, "tmp_manifest.txt")
ETL_LOG_PATH = os.path.join(LOGS_DIR, "cron.log")
TABLE_COUNT_PATH = os.path.join(LOGS_DIR, "table_count.csv")

def get_subdir_without_whale(path):
    parts = Path(path).parts
    for i, part in enumerate(parts):
        if part == '.whale':
            first_non_whale_index = i + 1
    return Path(*parts[first_non_whale_index:])
