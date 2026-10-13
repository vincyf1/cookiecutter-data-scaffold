"""Builds a throwaway project tree matching a case's failure_mode.

The tree is synthetic rather than a real bake, so a case is independent of
which Patterns this project was generated with. It still mirrors the shape
the demo ships: a DAG whose extract() emits a renamed field, and the Event
contract that load() validates each record against.
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

EVENT_SOURCE = '''from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any


class SchemaDriftError(ValueError):
    """A record lacks a field the Event contract declares."""


@dataclass(frozen=True, slots=True)
class Event:
    id: int
    name: str

    @classmethod
    def from_mapping(cls, record: Mapping[str, Any]) -> "Event":
        declared = [field.name for field in fields(cls)]
        missing = [name for name in declared if name not in record]
        if missing:
            raise SchemaDriftError(f"record lacks field(s) {missing}")
        return cls(id=record["id"], name=record["name"])
'''

SCHEMA_DRIFT_MESSAGE = (
    "record lacks field(s) ['name'] and carries undeclared field(s) "
    "['customer_name']; the Event contract declares ['id', 'name']. "
    "Either the producer renamed a field or the contract is out of date."
)

SCHEMA_DRIFT_LOG = (
    "[2026-07-29T00:00:00] INFO - Task load starting\n"
    "[2026-07-29T00:00:01] ERROR - Task failed with exception\n"
    "Traceback (most recent call last):\n"
    f"SchemaDriftError: {SCHEMA_DRIFT_MESSAGE}\n"
)

FAILURE_EXCEPTIONS = {
    "schema_drift": ("SchemaDriftError", SCHEMA_DRIFT_MESSAGE),
}


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def build_case_project(tmp_path: Path, case) -> tuple[Path, FailureContext]:
    if case.failure_mode not in FAILURE_EXCEPTIONS:
        supported = ", ".join(sorted(FAILURE_EXCEPTIONS))
        raise ValueError(
            f"eval case {case.id!r} has unknown failure_mode {case.failure_mode!r}; "
            f"expected one of: {supported}"
        )

    _write(tmp_path / "src" / "pkg" / "batch" / "dags" / "example_dag.py", SCHEMA_DRIFT_DAG)
    _write(tmp_path / "src" / "pkg" / "events.py", EVENT_SOURCE)
    (tmp_path / "load.log").write_text(SCHEMA_DRIFT_LOG)
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
