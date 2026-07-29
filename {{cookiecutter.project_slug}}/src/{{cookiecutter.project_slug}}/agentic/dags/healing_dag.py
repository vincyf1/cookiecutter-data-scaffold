"""The only Airflow-aware module in the agentic Pattern.

Keeping the import here is what lets every other module be unit-tested
with plain pytest and no scheduler. run_healing() takes its collaborators
as arguments so the whole loop is exercisable without Airflow at all.
"""

import logging
from pathlib import Path

from airflow.sdk import task

from {{ cookiecutter.project_slug }}.agentic.contracts import FailureContext, Trace
from {{ cookiecutter.project_slug }}.agentic.publisher import (
    PublishedPatch,
    default_publisher,
)
from {{ cookiecutter.project_slug }}.agentic.runner import ClaudeRunner
from {{ cookiecutter.project_slug }}.agentic.tools import ToolContext
from {{ cookiecutter.project_slug }}.agentic.tracing import (
    TraceRecorder,
    default_sink,
    write_trace_safely,
)

logger = logging.getLogger(__name__)


def project_root() -> Path:
    """Walk up from this module to the generated project's root.

    This module lives at src/{{ cookiecutter.project_slug }}/agentic/dags/healing_dag.py,
    so parents[0]=dags, [1]=agentic, [2]={{ cookiecutter.project_slug }}, [3]=src,
    [4]=the project root.
    """
    return Path(__file__).resolve().parents[4]


def build_failure_context(
    context: dict, root: Path, log_path: Path | None = None
) -> FailureContext:
    ti = context["task_instance"]
    exception = context.get("exception")
    resolved_log = log_path or root / "logs" / f"{ti.dag_id}" / f"{ti.task_id}.log"
    return FailureContext(
        dag_id=ti.dag_id,
        dag_run_id=context["dag_run"].run_id,
        task_id=ti.task_id,
        try_number=ti.try_number,
        exception_type=type(exception).__name__ if exception else "UnknownError",
        exception_message=str(exception) if exception else "",
        log_path=str(resolved_log),
        project_root=str(root),
    )


def run_healing(
    ctx: FailureContext,
    runner=None,
    sink=None,
    publisher=None,
) -> tuple[Trace, PublishedPatch]:
    """Diagnose, trace, and publish. Collaborators are injectable for tests."""
    runner = runner or ClaudeRunner()
    sink = sink or default_sink()
    publisher = publisher or default_publisher()

    recorder = TraceRecorder(
        dag_id=ctx.dag_id,
        dag_run_id=ctx.dag_run_id,
        task_id=ctx.task_id,
        try_number=ctx.try_number,
    )
    tool_ctx = ToolContext(project_root=Path(ctx.project_root), log_path=Path(ctx.log_path))

    try:
        result = runner.run(ctx, recorder)
    except Exception:
        # finish() in the failure path too, so a crashed run still yields a
        # well-formed array ending in status "error".
        write_trace_safely(sink, recorder.finish("error"))
        raise

    tool_ctx.edits = list(result.edits)
    trace = recorder.finish("ok")
    write_trace_safely(sink, trace)
    published = publisher.publish(trace.trace_id, result, tool_ctx)
    logger.info(
        "healing run %s: %s (confidence=%s) -> %s",
        trace.trace_id,
        result.diagnosis.patch_summary,
        result.diagnosis.confidence,
        published.patch_path or "no patch",
    )
    return trace, published


@task
def heal_on_failure(**context) -> str:
    """Runs only when an upstream task failed (trigger_rule=ALL_FAILED)."""
    ctx = build_failure_context(context, project_root())
    trace, published = run_healing(ctx)
    return published.patch_path or trace.trace_id
