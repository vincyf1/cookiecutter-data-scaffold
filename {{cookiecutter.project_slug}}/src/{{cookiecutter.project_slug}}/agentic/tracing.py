"""Capture of a healing run as one JSON array per failure event.

The array is the unit of write, so atomicity is structural rather than
transactional: the document is written or it is not. Element order is
emission order, which in a single-threaded agent loop is causal order --
that is why `seq` replaces a parent/child span tree.
"""

import logging
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from {{ cookiecutter.project_slug }}.agentic.contracts import Trace, TraceElement

logger = logging.getLogger(__name__)

DEFAULT_TRACE_DIR = Path("artifacts/traces")
SUMMARY_MAX_CHARS = 2000


def _now() -> datetime:
    return datetime.now(timezone.utc)


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

    def __init__(self, base_dir: Path = DEFAULT_TRACE_DIR) -> None:
        self.base_dir = Path(base_dir)

    def write(self, trace: Trace) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        (self.base_dir / f"{trace.trace_id}.json").write_text(trace.to_json_array())

{%- if cookiecutter.include_lakehouse %}


DDL_PATH = Path(__file__).parent / "create_traces_table.sql"


class DuckDBSink:
    """One row per failure event; the element array lands in a JSON column."""

    def __init__(self, db_path: str = "lakehouse.duckdb") -> None:
        self.db_path = db_path

    def write(self, trace: Trace) -> None:
        import duckdb

        con = duckdb.connect(self.db_path)
        try:
            con.execute(DDL_PATH.read_text())
            con.execute(
                "INSERT OR REPLACE INTO agent_traces "
                "(trace_id, dag_id, dag_run_id, task_id, try_number, occurred_at, status, elements) "
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


def default_sink() -> TraceSink:
    return DuckDBSink()

{%- else %}


def default_sink() -> TraceSink:
    return JsonlSink()

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
    except Exception as exc:
        logger.warning("failed to persist agent trace %s: %s", trace.trace_id, exc)
        return False
