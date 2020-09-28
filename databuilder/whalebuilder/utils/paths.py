from pathlib import Path
import os

BASE_DIR = os.path.join(Path.home(), ".whale/")
CONNECTION_PATH = os.path.join(BASE_DIR, "config/connections.yaml")
MANIFEST_PATH = os.path.join(BASE_DIR, "manifests/manifest.txt")
TMP_MANIFEST_PATH = os.path.join(BASE_DIR, "manifests/tmp_manifest.txt")
ETL_LOG_PATH = os.path.join(BASE_DIR, "logs/cron.log")
