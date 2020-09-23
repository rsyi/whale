from pathlib import Path
import os

BASE_DIR = os.path.join(Path.home(), '.whale/')
CONNECTION_PATH = os.path.join(BASE_DIR, 'config/connections.yaml')
MANIFEST_PATH = os.path.join(BASE_DIR, 'manifest.txt')
TMP_MANIFEST_PATH = os.path.join(BASE_DIR, 'tmp_manifest.txt')
