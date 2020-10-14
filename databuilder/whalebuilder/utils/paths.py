from pathlib import Path
import os

BASE_DIR = os.path.join(Path.home(), ".whale/")
CONNECTION_PATH = os.path.join(BASE_DIR, "config/connections.yaml")
LOGS_DIR = os.path.join(BASE_DIR, "logs/")
MANIFEST_DIR = os.path.join(BASE_DIR, "manifests/")
MANIFEST_PATH = os.path.join(MANIFEST_DIR, "manifest.txt")
METADATA_PATH = os.path.join(BASE_DIR, "metadata/")
METRICS_PATH = os.path.join(BASE_DIR, "metrics/")
TMP_MANIFEST_PATH = os.path.join(MANIFEST_DIR, "tmp_manifest.txt")
ETL_LOG_PATH = os.path.join(LOGS_DIR, "cron.log")
TABLE_COUNT_PATH = os.path.join(LOGS_DIR, "table_count.csv")
