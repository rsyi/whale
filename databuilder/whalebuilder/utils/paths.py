from pathlib import Path
import os

BASE_DIR = os.path.join(Path.home(), ".whale/")
CONNECTION_PATH = os.path.join(BASE_DIR, "config/connections.yaml")
MANIFEST_PATH = os.path.join(BASE_DIR, "manifests/manifest.txt")
METADATA_PATH = os.path.join(BASE_DIR, '/metadata/')
TMP_MANIFEST_PATH = os.path.join(BASE_DIR, "manifests/tmp_manifest.txt")
ETL_LOG_PATH = os.path.join(BASE_DIR, "logs/cron.log")
TABLE_COUNT_PATH = os.path.join(BASE_DIR, "logs/table_count.csv")
