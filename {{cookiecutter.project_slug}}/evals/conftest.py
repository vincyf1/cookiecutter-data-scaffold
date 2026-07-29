"""Builds a throwaway project tree matching a case's failure_mode.

The tree is synthetic rather than a real bake, so a case is independent of
which Patterns this project was generated with: the schema_drift fixture
carries a DDL even when the Lakehouse Pattern is off, and the
missing_config fixture carries a pipeline.toml even when it is on. Each
case still mirrors the shape the corresponding demo actually ships.
"""

from pathlib import Path

from {{ cookiecutter.project_slug }}.agentic.contracts import FailureContext

SCHEMA_DRIFT_DAG = '''from airflow.sdk import dag, task


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

SCHEMA_DRIFT_LOG = (
    "[2026-07-29T00:00:00] INFO - Task load starting\n"
    "[2026-07-29T00:00:01] ERROR - Task failed with exception\n"
    "Traceback (most recent call last):\n"
    "SchemaDriftError: record carries undeclared field(s) ['customer_name']; "
    "the events table declares columns ['id', 'name', 'ingested_at']. "
    "Either the producer renamed a field or the DDL is out of date.\n"
)

MISSING_CONFIG_DAG = '''from airflow.sdk import dag, task

from pkg.batch.sinks import write_events


@dag()
def example_events_dag():
    @task
    def load(records: list[dict]) -> None:
        write_events(records, output_setting="output_file")
'''

PIPELINE_TOML = """[pipeline]
output_path = "data/events/events.parquet"
compression = "snappy"
"""

MISSING_CONFIG_LOG = (
    "[2026-07-29T00:00:00] INFO - Task load starting\n"
    "[2026-07-29T00:00:01] ERROR - Task failed with exception\n"
    "Traceback (most recent call last):\n"
    "MissingConfigError: pipeline config declares no setting 'output_file'; "
    "pipeline.toml declares ['compression', 'output_path']. "
    "Either the caller renamed a setting or the config file is out of date.\n"
)

FAILURE_EXCEPTIONS = {
    "schema_drift": (
        "SchemaDriftError",
        "record carries undeclared field(s) ['customer_name']",
    ),
    "missing_config": (
        "MissingConfigError",
        "pipeline config declares no setting 'output_file'",
    ),
}


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def build_case_project(tmp_path: Path, case) -> tuple[Path, FailureContext]:
    dag_path = tmp_path / "src" / "pkg" / "batch" / "dags" / "example_dag.py"

    if case.failure_mode == "missing_config":
        _write(dag_path, MISSING_CONFIG_DAG)
        _write(tmp_path / "src" / "pkg" / "batch" / "pipeline.toml", PIPELINE_TOML)
        log = MISSING_CONFIG_LOG
    else:
        _write(dag_path, SCHEMA_DRIFT_DAG)
        _write(tmp_path / "src" / "pkg" / "lakehouse" / "create_tables.sql", DDL_SOURCE)
        log = SCHEMA_DRIFT_LOG

    (tmp_path / "load.log").write_text(log)
    exception_type, exception_message = FAILURE_EXCEPTIONS[case.failure_mode]

    ctx = FailureContext(
        dag_id="example_events_dag",
        dag_run_id=f"eval_{case.id}",
        task_id="load",
        try_number=1,
        exception_type=exception_type,
        exception_message=exception_message,
        log_path=str(tmp_path / "load.log"),
        project_root=str(tmp_path),
    )
    return tmp_path, ctx
