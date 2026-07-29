"""Builds a throwaway project tree matching a case's failure_mode."""

from pathlib import Path

from {{ cookiecutter.project_slug }}.agentic.contracts import FailureContext

DAG_SOURCE = '''from airflow.sdk import dag, task


@dag()
def example_events_dag():
    @task
    def extract() -> list[dict]:
        return [{"id": 1, "customer_name": "example"}]
'''

DDL_SOURCE = """CREATE TABLE IF NOT EXISTS events (
    id BIGINT,
    name VARCHAR,
    ingested_at TIMESTAMP DEFAULT now()
);
"""

LOG_SOURCE = (
    "[2026-07-29T00:00:00] INFO - Task load starting\n"
    "[2026-07-29T00:00:01] ERROR - Task failed with exception\n"
    "Traceback (most recent call last):\n"
    "SchemaDriftError: record carries undeclared field(s) ['customer_name']; "
    "the events table declares columns ['id', 'name', 'ingested_at']. "
    "Either the producer renamed a field or the DDL is out of date.\n"
)


def build_case_project(tmp_path: Path, case) -> tuple[Path, FailureContext]:
    dag_dir = tmp_path / "src" / "pkg" / "batch" / "dags"
    dag_dir.mkdir(parents=True)
    (dag_dir / "example_dag.py").write_text(DAG_SOURCE)

    ddl_dir = tmp_path / "src" / "pkg" / "lakehouse"
    ddl_dir.mkdir(parents=True)
    (ddl_dir / "create_tables.sql").write_text(DDL_SOURCE)

    (tmp_path / "load.log").write_text(LOG_SOURCE)

    ctx = FailureContext(
        dag_id="example_events_dag",
        dag_run_id=f"eval_{case.id}",
        task_id="load",
        try_number=1,
        exception_type="SchemaDriftError",
        exception_message="record carries undeclared field(s) ['customer_name']",
        log_path=str(tmp_path / "load.log"),
        project_root=str(tmp_path),
    )
    return tmp_path, ctx
