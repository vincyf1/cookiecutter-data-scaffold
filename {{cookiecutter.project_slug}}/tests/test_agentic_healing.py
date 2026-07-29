import json

import pytest

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    FailureContext,
    ProposedEdit,
)
from {{ cookiecutter.project_slug }}.agentic.dags.healing_dag import (
    build_failure_context,
    run_healing,
)
from {{ cookiecutter.project_slug }}.agentic.publisher import LocalPatchPublisher
from {{ cookiecutter.project_slug }}.agentic.runner import FakeRunner
from {{ cookiecutter.project_slug }}.agentic.tracing import JsonlSink


@pytest.fixture()
def project(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "dag.py").write_text('record = {"id": 1, "customer_name": "x"}\n')
    (tmp_path / "load.log").write_text(
        "SchemaDriftError: record carries undeclared field(s) ['customer_name']\n"
    )
    return tmp_path


@pytest.fixture()
def failure_ctx(project):
    return FailureContext(
        dag_id="example_events_dag",
        dag_run_id="run_1",
        task_id="load",
        try_number=1,
        exception_type="SchemaDriftError",
        exception_message="record carries undeclared field(s) ['customer_name']",
        log_path=str(project / "load.log"),
        project_root=str(project),
    )


@pytest.fixture()
def runner():
    return FakeRunner(
        diagnosis=AgentDiagnosis(
            root_cause="extract() emits customer_name; the DDL declares name",
            offending_field="customer_name",
            confidence="high",
            patch_summary="rename customer_name to name",
        ),
        edits=[
            ProposedEdit(
                path="src/dag.py",
                old_str="customer_name",
                new_str="name",
                rationale="matches DDL column",
            )
        ],
        tool_sequence=["read_task_log", "read_repo_file", "propose_patch"],
    )


def test_build_failure_context_reads_the_airflow_context(project):
    class FakeTaskInstance:
        dag_id = "example_events_dag"
        task_id = "load"
        try_number = 2
        log_url = "http://localhost/log"

    context = {
        "task_instance": FakeTaskInstance(),
        "dag_run": type("R", (), {"run_id": "run_9"})(),
        "exception": ValueError("boom"),
    }

    ctx = build_failure_context(context, project, log_path=project / "load.log")

    assert ctx.dag_id == "example_events_dag"
    assert ctx.dag_run_id == "run_9"
    assert ctx.try_number == 2
    assert ctx.exception_type == "ValueError"
    assert ctx.exception_message == "boom"


def test_run_healing_produces_a_trace_and_a_patch(project, failure_ctx, runner):
    trace, published = run_healing(
        failure_ctx,
        runner=runner,
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    assert trace.status == "ok"
    assert (project / "traces" / f"{trace.trace_id}.json").is_file()
    assert (project / "artifacts" / f"{trace.trace_id}.patch").is_file()
    assert published.title.startswith("fix:")


def test_run_healing_records_the_tool_sequence_in_order(project, failure_ctx, runner):
    trace, _ = run_healing(
        failure_ctx,
        runner=runner,
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    elements = json.loads((project / "traces" / f"{trace.trace_id}.json").read_text())
    tool_calls = [e["name"] for e in elements if e["kind"] == "tool_call"]
    assert tool_calls == ["read_task_log", "read_repo_file", "propose_patch"]
    assert elements[-1]["kind"] == "agent_run"


def test_run_healing_writes_a_terminal_error_trace_when_the_agent_raises(
    project, failure_ctx
):
    class ExplodingRunner:
        def run(self, ctx, recorder):
            raise RuntimeError("model unavailable")

    with pytest.raises(RuntimeError):
        run_healing(
            failure_ctx,
            runner=ExplodingRunner(),
            sink=JsonlSink(project / "traces"),
            publisher=LocalPatchPublisher(project / "artifacts"),
        )

    written = list((project / "traces").glob("*.json"))
    assert len(written) == 1
    elements = json.loads(written[0].read_text())
    assert elements[-1]["kind"] == "agent_run"
    assert elements[-1]["status"] == "error"


def test_run_healing_survives_a_failing_trace_sink(project, failure_ctx, runner, caplog):
    class ExplodingSink:
        def write(self, trace):
            raise RuntimeError("table agent_traces does not exist")

    trace, published = run_healing(
        failure_ctx,
        runner=runner,
        sink=ExplodingSink(),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    assert published.patch_path is not None
    assert "failed to persist agent trace" in caplog.text
