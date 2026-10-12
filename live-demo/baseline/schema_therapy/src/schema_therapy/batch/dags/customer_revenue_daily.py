"""Daily Supabase to DuckDB ingestion for customer revenue reporting."""

import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from airflow.sdk import dag, task

from schema_therapy.batch.ingest import ingest_supabase_to_duckdb


@dag(
    schedule="@daily",
    start_date=datetime(2026, 9, 26, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
)
def customer_revenue_daily():
    @task
    def ingest_supabase() -> None:
        ingest_supabase_to_duckdb()

    @task
    def transform_with_dbt() -> None:
        project_dir = Path(
            os.environ.get("DBT_PROJECT_DIR")
            or Path(__file__).resolve().parents[4] / "transformation"
        )
        subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                str(project_dir),
                "--profiles-dir",
                str(project_dir),
                "--select",
                "+customer_revenue_daily",
                "time_spine_daily",
            ],
            cwd=project_dir,
            check=True,
        )

    ingest_supabase() >> transform_with_dbt()


customer_revenue_daily()
