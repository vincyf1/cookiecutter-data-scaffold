import pytest

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    FailureContext,
    ProposedEdit,
)
from {{ cookiecutter.project_slug }}.agentic.runner import (
    MODEL,
    FakeRunner,
    _model_kwargs,
    dispatch_tool,
)
from {{ cookiecutter.project_slug }}.agentic.tools import ToolContext, ToolError
from {{ cookiecutter.project_slug }}.agentic.tracing import TraceRecorder


@pytest.fixture()
def failure_ctx(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "dag.py").write_text('record = {"id": 1, "customer_name": "x"}\n')
    (tmp_path / "load.log").write_text("SchemaDriftError: unexpected field 'customer_name'\n")
    return FailureContext(
        dag_id="example_events_dag",
        dag_run_id="run_1",
        task_id="load",
        try_number=1,
        exception_type="SchemaDriftError",
        exception_message="unexpected field 'customer_name'",
        log_path=str(tmp_path / "load.log"),
        project_root=str(tmp_path),
    )


def test_model_kwargs_uses_temperature_for_haiku():
    kwargs = _model_kwargs("claude-haiku-4-5")
    assert kwargs["temperature"] == 0
    assert "output_config" not in kwargs


def test_model_kwargs_uses_effort_for_opus_and_never_temperature():
    kwargs = _model_kwargs("claude-opus-5")
    assert "temperature" not in kwargs
    assert kwargs["output_config"] == {"effort": "medium"}


def test_model_kwargs_falls_back_conservatively_for_unknown_models():
    # The intersection valid on every model: no sampling params, no effort.
    assert _model_kwargs("some-future-model") == {}


def test_default_model_comes_from_the_cookiecutter_variable():
    assert MODEL == "{{ cookiecutter.agentic_model }}"


def test_dispatch_tool_routes_to_read_task_log(failure_ctx, tmp_path):
    tool_ctx = ToolContext(project_root=tmp_path, log_path=tmp_path / "load.log")
    out = dispatch_tool(tool_ctx, "read_task_log", {"task_id": "load"})
    assert "customer_name" in out


def test_dispatch_tool_rejects_an_unknown_tool(tmp_path):
    tool_ctx = ToolContext(project_root=tmp_path, log_path=tmp_path / "load.log")
    with pytest.raises(ToolError, match="unknown tool"):
        dispatch_tool(tool_ctx, "rm_rf", {})


def test_fake_runner_produces_a_result_and_records_the_tool_sequence(failure_ctx):
    runner = FakeRunner(
        diagnosis=AgentDiagnosis(
            root_cause="field renamed",
            offending_field="customer_name",
            confidence="high",
            patch_summary="rename to name",
        ),
        edits=[
            ProposedEdit(
                path="src/dag.py",
                old_str="customer_name",
                new_str="name",
                rationale="matches DDL",
            )
        ],
        tool_sequence=["read_task_log", "read_repo_file", "propose_patch"],
    )
    recorder = TraceRecorder(
        dag_id=failure_ctx.dag_id,
        dag_run_id=failure_ctx.dag_run_id,
        task_id=failure_ctx.task_id,
        try_number=failure_ctx.try_number,
    )

    result = runner.run(failure_ctx, recorder)

    assert result.diagnosis.offending_field == "customer_name"
    assert [e.name for e in result.elements if e.kind == "tool_call"] == [
        "read_task_log",
        "read_repo_file",
        "propose_patch",
    ]


def test_fake_runner_needs_no_api_key(monkeypatch, failure_ctx):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    runner = FakeRunner(
        diagnosis=AgentDiagnosis(root_cause="x", confidence="low", patch_summary="y"),
        edits=[],
        tool_sequence=[],
    )
    recorder = TraceRecorder(dag_id="d", dag_run_id="r", task_id="t", try_number=1)
    assert runner.run(failure_ctx, recorder).diagnosis.root_cause == "x"
