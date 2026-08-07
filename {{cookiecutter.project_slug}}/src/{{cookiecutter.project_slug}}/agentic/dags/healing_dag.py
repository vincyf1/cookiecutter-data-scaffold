"""The only Airflow-aware module in the agentic Pattern.

Keeping the import here is what lets every other module be unit-tested
with plain pytest and no scheduler. run_healing() takes its collaborators
as arguments so the whole loop is exercisable without Airflow at all.
"""

import logging
import os
from dataclasses import dataclass
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

# The failing task publishes what only it knows -- the exception it died on
# and which attempt died -- under this key. `context.get("exception")` is a
# *callback* context key; heal_on_failure is an ordinary downstream task, so
# its own context never carries it. XCom is the one channel that crosses
# from a failed task to an ALL_FAILED downstream task in the same run.
FAILURE_XCOM_KEY = "agentic_failure"


def project_root() -> Path:
    """Walk up from this module to the generated project's root.

    This module lives at src/{{ cookiecutter.project_slug }}/agentic/dags/healing_dag.py,
    so parents[0]=dags, [1]=agentic, [2]={{ cookiecutter.project_slug }}, [3]=src,
    [4]=the project root.
    """
    return Path(__file__).resolve().parents[4]


def record_failure(context) -> None:
    """`on_failure_callback` for the task the healer watches.

    Runs in the failing task's own process, where `context["exception"]` and
    the real `try_number` exist, and hands both to the healer via XCom.
    """
    ti = context["task_instance"]
    exception = context.get("exception")
    ti.xcom_push(
        key=FAILURE_XCOM_KEY,
        value={
            "task_id": ti.task_id,
            "try_number": ti.try_number,
            "exception_type": type(exception).__name__ if exception else "UnknownError",
            "exception_message": str(exception) if exception else "",
        },
    )


@dataclass
class _LogTemplateVars:
    """The subset of TaskInstance that `log_filename_template` references.

    FileTaskHandler renders the same template against a real TaskInstance,
    but its renderer needs a DB session that a task running under the
    Airflow 3 task SDK does not have -- and the TaskInstance we would need
    is the *failed* task's, not ours. These five attributes are what the
    shipped template actually reads.
    """

    dag_id: str
    run_id: str
    task_id: str
    map_index: int = -1
    try_number: int = 1


def _log_settings() -> tuple[Path, str | None]:
    """Airflow's own answer to "where do worker logs go".

    Both halves are configuration, not convention: `base_log_folder` and
    `log_filename_template` are what FileTaskHandler itself reads, so
    deriving from them is the only way a customized layout stays resolvable.
    """
    try:
        from airflow.configuration import conf

        return Path(conf.get("logging", "base_log_folder")), conf.get(
            "logging", "log_filename_template"
        )
    except Exception:  # noqa: BLE001 # pragma: no cover - any config read failure falls back
        home = os.environ.get("AIRFLOW_HOME") or str(Path.home() / "airflow")
        return Path(home) / "logs", None


def _newest_log(directory: Path) -> Path | None:
    logs = [p for p in directory.glob("*.log") if p.is_file()]
    if not logs:
        return None
    return max(logs, key=lambda p: p.stat().st_mtime)


def resolve_log_path(dag_id: str, run_id: str, task_id: str, try_number: int) -> Path:
    """Where the *failed* task's log file landed on this worker."""
    base, template = _log_settings()
    if not template:
        return base / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}"

    from airflow.utils.helpers import parse_template_string, render_template

    str_tpl, jinja_tpl = parse_template_string(template)
    variables = _LogTemplateVars(
        dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=try_number
    )
    if jinja_tpl is not None:
        rendered = render_template(
            jinja_tpl, {"ti": variables, "try_number": try_number}, native=False
        )
    else:
        rendered = str_tpl.format(
            dag_id=dag_id, task_id=task_id, run_id=run_id, try_number=try_number
        )
    candidate = base / rendered

    if candidate.exists():
        return candidate
    # A retried task's last attempt is the one that failed the DAG run, and
    # our try_number may be off by one if the XCom payload is missing. The
    # attempt directory is still exact, so fall back within it rather than
    # returning a path we know does not exist.
    newest = _newest_log(candidate.parent)
    return newest or candidate


def build_failure_context(
    context: dict,
    root: Path,
    failed_task_id: str,
    log_path: Path | None = None,
) -> FailureContext:
    """Describe the failure of `failed_task_id`, not of the task calling this.

    `failed_task_id` is passed in rather than read off `context["task_instance"]`
    because the healer *is* a task: Airflow injects the healer's own
    TaskInstance here, so reading `ti.task_id` would name the healer as the
    thing that failed and point the log path at the healer's own log.
    """
    ti = context["task_instance"]
    run_id = context["dag_run"].run_id
    payload = ti.xcom_pull(task_ids=failed_task_id, key=FAILURE_XCOM_KEY) or {}
    try_number = int(payload.get("try_number") or 1)
    resolved_log = log_path or resolve_log_path(
        ti.dag_id, run_id, failed_task_id, try_number
    )
    return FailureContext(
        dag_id=ti.dag_id,
        dag_run_id=run_id,
        task_id=failed_task_id,
        try_number=try_number,
        exception_type=payload.get("exception_type") or "UnknownError",
        exception_message=payload.get("exception_message") or "",
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
    root = Path(ctx.project_root)
    runner = runner or ClaudeRunner()
    # Both defaults are rooted at the project, not at the worker's CWD.
    # LocalPatchPublisher and GitHubPublisher would otherwise disagree about
    # where artifacts live, and the worker's CWD is not a mounted volume, so
    # CWD-relative artifacts never reach the host.
    sink = sink or default_sink(root)
    publisher = publisher or default_publisher(root)

    recorder = TraceRecorder(
        dag_id=ctx.dag_id,
        dag_run_id=ctx.dag_run_id,
        task_id=ctx.task_id,
        try_number=ctx.try_number,
    )
    tool_ctx = ToolContext(project_root=root, log_path=Path(ctx.log_path))

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
def heal_on_failure(failed_task_id: str, **context) -> str:
    """Runs only when an upstream task failed (trigger_rule=ALL_FAILED)."""
    ctx = build_failure_context(context, project_root(), failed_task_id)
    trace, published = run_healing(ctx)
    return published.patch_path or trace.trace_id
