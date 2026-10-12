"""Seed the local demo's Simple Auth Manager password before Airflow starts."""

import json
import os
from pathlib import Path


password = os.environ.get("AIRFLOW_ADMIN_PASSWORD")
if not password:
    raise SystemExit("AIRFLOW_ADMIN_PASSWORD must be set")

password_file = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
password_file.write_text(json.dumps({"admin": password}) + "\n", encoding="utf-8")
password_file.chmod(0o600)
