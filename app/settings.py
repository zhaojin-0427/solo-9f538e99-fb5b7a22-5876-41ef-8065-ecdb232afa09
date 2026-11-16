import os
from pathlib import Path

BASE_URL = os.environ.get("FHIR_BASE_URL", "http://localhost:8000").rstrip("/")
DB_PATH = Path(os.environ.get("FHIR_DB_PATH", "data/fhir.sqlite3"))
MAX_BUNDLE_ENTRIES = int(os.environ.get("FHIR_MAX_BUNDLE_ENTRIES", "30"))
MAX_BODY_BYTES = int(os.environ.get("FHIR_MAX_BODY_BYTES", "2000000"))
SQLITE_BUSY_TIMEOUT_MS = int(os.environ.get("FHIR_SQLITE_BUSY_TIMEOUT_MS", "15000"))
