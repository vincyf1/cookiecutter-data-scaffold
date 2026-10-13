"""Capture of a healing run as one JSON array per failure event.

The array is the unit of write, so atomicity is structural rather than
transactional: the document is written or it is not. Element order is
emission order, which in a single-threaded agent loop is causal order --
that is why `seq` replaces a parent/child span tree.
"""

import logging
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from {{ cookiecutter.project_slug }}.agentic.contracts import Trace, TraceElement

logger = logging.getLogger(__name__)

# Relative to the project root, never to the process CWD: an Airflow worker's
# CWD is not the project and is not a mounted volume, so a CWD-relative
# default silently strands every trace inside the container.
TRACE_SUBDIR = Path("artifacts") / "traces"
SUMMARY_MAX_CHARS = 2000


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(moment: datetime) -> str:
    return moment.isoformat()


def truncate(value: str, limit: int = SUMMARY_MAX_CHARS) -> str:
    """Summaries are bounded; full payloads are never persisted."""
    if len(value) <= limit:
        return value
    return value[:limit] + f"... [truncated {len(value) - limit} chars]"


class TraceRecorder:
    """Accumulates elements in memory; emits a Trace at the end."""

    def __init__(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        trace_id: str | None = None,
    ) -> None:
        self.trace_id = trace_id or uuid.uuid4().hex
        self.dag_id = dag_id
        self.dag_run_id = dag_run_id
        self.task_id = task_id
        self.try_number = try_number
        self._elements: list[TraceElement] = []
        self._started = _now()

    @property
    def elements(self) -> list[TraceElement]:
        """The elements recorded so far, in emission order.

        Read-only: callers (runner.py, evals/replay.py) need to snapshot the
        list into an AgentResult, not to append to it -- appending is
        `add()`/`element()`'s job, which is what keeps `seq` contiguous.
        """
        return list(self._elements)

    def _next_seq(self) -> int:
        return len(self._elements)

    def add(self, kind: str, name: str, **fields) -> TraceElement:
        """Record a zero-duration element (used for `result` entries)."""
        moment = _now()
        element = TraceElement(
            seq=self._next_seq(),
            kind=kind,
            name=name,
            status=fields.pop("status", "ok"),
            started_at=_iso(moment),
            ended_at=_iso(moment),
            duration_ms=0,
            **fields,
        )
        self._elements.append(element)
        return element

    @contextmanager
    def element(self, kind: str, name: str, input_summary: dict | None = None):
        """Time one element. Marks it `error` and re-raises if the body raises."""
        started = _now()
        seq = self._next_seq()
        placeholder = TraceElement(
            seq=seq,
            kind=kind,
            name=name,
            status="running",
            started_at=_iso(started),
            input_summary=input_summary,
        )
        self._elements.append(placeholder)
        box: dict = {}
        try:
            yield box
        except Exception as exc:
            placeholder.status = "error"
            placeholder.error = f"{type(exc).__name__}: {exc}"
            raise
        else:
            placeholder.status = "ok"
            placeholder.output_summary = box.get("output_summary")
            placeholder.input_tokens = box.get("input_tokens")
            placeholder.output_tokens = box.get("output_tokens")
        finally:
            ended = _now()
            placeholder.ended_at = _iso(ended)
            placeholder.duration_ms = int((ended - started).total_seconds() * 1000)

    def finish(self, status: str) -> Trace:
        """Append the terminal element and freeze the trace.

        Call this from a `finally` so a crashed run still yields a
        well-formed array ending in status "error".
        """
        ended = _now()
        self._elements.append(
            TraceElement(
                seq=self._next_seq(),
                kind="agent_run",
                name="heal",
                status=status,
                started_at=_iso(self._started),
                ended_at=_iso(ended),
                duration_ms=int((ended - self._started).total_seconds() * 1000),
            )
        )
        return Trace(
            trace_id=self.trace_id,
            dag_id=self.dag_id,
            dag_run_id=self.dag_run_id,
            task_id=self.task_id,
            try_number=self.try_number,
            occurred_at=_iso(self._started),
            status=status,
            elements=self._elements,
        )


class TraceSink(Protocol):
    def write(self, trace: Trace) -> None: ...


class JsonlSink:
    """One JSON document per failure event, on local disk."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = Path(base_dir)

    def write(self, trace: Trace) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        (self.base_dir / f"{trace.trace_id}.json").write_text(trace.to_json_array())

{%- if cookiecutter.include_lakehouse %}


DDL_PATH = Path(__file__).parent / "create_traces_table.sql"

# Traces get their own database file, not the pipeline's.
#
# DuckDB takes an exclusive lock per file and Airflow runs each task in its own
# process, so a sink pointed at the events database contends with the very
# pipeline it is recording. That is not theoretical: in the Docker demo the
# `load` task died with
#
#   IOException: Conflicting lock is held in airflow worker (PID 145)
#
# before it could raise the schema-drift error the demo exists to show. A
# tracing layer that can take down the task it traces is not worth the join
# convenience -- and the join is still one line away:
#
#   ATTACH 'artifacts/traces.duckdb' AS traces;
#   SELECT * FROM traces.agent_traces;
TRACE_DB = Path("artifacts") / "traces.duckdb"


class DuckDBSink:
    """One row per failure event; the element array lands in a JSON column."""

    def __init__(self, db_path: str | Path = TRACE_DB) -> None:
        self.db_path = db_path

    def write(self, trace: Trace) -> None:
        import duckdb

        # The demo's artifacts/ exists, but a caller can point this anywhere,
        # and duckdb.connect will not create a missing parent for us.
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(self.db_path))
        try:
            con.execute(DDL_PATH.read_text())
            con.execute(
                "INSERT OR REPLACE INTO agent_traces "
                "(trace_id, dag_id, dag_run_id, task_id, try_number, "
                "occurred_at, status, elements) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    trace.trace_id,
                    trace.dag_id,
                    trace.dag_run_id,
                    trace.task_id,
                    trace.try_number,
                    trace.occurred_at,
                    trace.status,
                    trace.to_json_array(),
                ],
            )
        finally:
            con.close()


def default_sink(project_root: Path) -> TraceSink:
    # project_root is load-bearing now that the trace database is this
    # Pattern's own file: resolving against it means the sink lands in the
    # same place whatever directory the Airflow task happens to run from.
    return DuckDBSink(Path(project_root) / TRACE_DB)

{%- else %}


def default_sink(project_root: Path) -> TraceSink:
    return JsonlSink(Path(project_root) / TRACE_SUBDIR)

{%- endif %}


def write_trace_safely(sink: TraceSink, trace: Trace) -> bool:
    """Persist a trace without ever failing the healing task.

    The healing task exists to fix a schema failure; a schema failure in
    the trace table taking down the fixer is a circular failure worth
    designing out. The cost is silent trace loss, mitigated by this warning
    landing in the Airflow task log.
    """
    try:
        sink.write(trace)
        return True
    except Exception as exc:  # noqa: BLE001 - breadth is the point; see docstring
        logger.warning("failed to persist agent trace %s: %s", trace.trace_id, exc)
        return False
