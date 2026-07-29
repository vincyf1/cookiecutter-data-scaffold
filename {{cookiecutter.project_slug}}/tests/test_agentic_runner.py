import copy
import json
from dataclasses import dataclass, field

import pytest

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    FailureContext,
    ProposedEdit,
)
from {{ cookiecutter.project_slug }}.agentic.runner import (
    MODEL,
    ClaudeRunner,
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


def test_model_kwargs_returns_independent_copies_of_nested_dicts():
    from {{ cookiecutter.project_slug }}.agentic.runner import _MODEL_PARAMS

    before = copy.deepcopy(_MODEL_PARAMS)

    first = _model_kwargs("claude-opus-5")
    first["output_config"]["effort"] = "high"

    second = _model_kwargs("claude-opus-5")

    assert second == {"output_config": {"effort": "medium"}}
    assert _MODEL_PARAMS == before


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


@dataclass
class _FakeUsage:
    input_tokens: int = 1
    output_tokens: int = 1


@dataclass
class _FakeTextBlock:
    text: str
    type: str = "text"


@dataclass
class _FakeResponse:
    content: list
    stop_reason: str = "end_turn"
    usage: _FakeUsage = field(default_factory=_FakeUsage)


class _FakeMessages:
    """Stands in for client.messages -- no network, canned responses in order."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def create(self, **kwargs):
        # runner.py mutates its `messages` list in place after this call
        # returns, so a snapshot (not the live list) is what a caller must
        # capture to see what was actually sent on this turn.
        kwargs = {**kwargs, "messages": list(kwargs["messages"])}
        self.calls.append(kwargs)
        return self._responses.pop(0)


class _FakeClient:
    def __init__(self, responses):
        self.messages = _FakeMessages(responses)


def test_claude_runner_carries_the_final_assistant_turn_into_the_diagnosis_call(
    failure_ctx,
):
    """The model can finish by writing prose instead of exhausting tool calls.

    Regression test: that concluding turn must reach `_final_diagnosis` so it
    sees the model's own synthesis, not just the raw tool history -- and it
    must be recorded exactly once, never duplicated onto a tool-use path.
    """
    conclusion = _FakeTextBlock(text="I've read the log and the source.")
    diagnosis_payload = {
        "root_cause": "field renamed",
        "offending_field": "customer_name",
        "confidence": "high",
        "patch_summary": "rename to name",
    }
    diagnosis_block = _FakeTextBlock(text=json.dumps(diagnosis_payload))

    fake_client = _FakeClient(
        responses=[
            _FakeResponse(content=[conclusion], stop_reason="end_turn"),
            _FakeResponse(content=[diagnosis_block], stop_reason="end_turn"),
        ]
    )

    runner = ClaudeRunner(model="claude-haiku-4-5")
    runner._client = lambda: fake_client

    recorder = TraceRecorder(
        dag_id=failure_ctx.dag_id,
        dag_run_id=failure_ctx.dag_run_id,
        task_id=failure_ctx.task_id,
        try_number=failure_ctx.try_number,
    )

    result = runner.run(failure_ctx, recorder)

    assert result.diagnosis.offending_field == "customer_name"

    # First call: only the initial user turn.
    assert [m["role"] for m in fake_client.messages.calls[0]["messages"]] == ["user"]

    # Second call (the diagnosis turn) must include the assistant's
    # conclusion exactly once, sandwiched between the original user turn
    # and the synthetic "return your diagnosis" turn -- not duplicated,
    # and not dropped.
    diagnosis_messages = fake_client.messages.calls[1]["messages"]
    roles = [m["role"] for m in diagnosis_messages]
    assert roles == ["user", "assistant", "user"]
    assert diagnosis_messages[1]["content"] == [conclusion]


def test_final_diagnosis_does_not_collide_on_output_config_for_a_non_haiku_model(
    failure_ctx,
):
    # claude-opus-5/claude-sonnet-5 get output_config={"effort": ...} back
    # from _model_kwargs, which would collide with the explicit
    # output_config= _final_diagnosis already passes for the json_schema
    # format -- splatting both in the same call raises `TypeError: got
    # multiple values for keyword argument 'output_config'`. The default
    # claude-haiku-4-5 model never exercises this path (it gets
    # temperature=0 instead), so a test using only the default would not
    # catch a regression here.
    diagnosis_payload = {
        "root_cause": "field renamed",
        "offending_field": "customer_name",
        "confidence": "high",
        "patch_summary": "rename to name",
    }
    diagnosis_block = _FakeTextBlock(text=json.dumps(diagnosis_payload))

    fake_client = _FakeClient(
        responses=[
            _FakeResponse(content=[diagnosis_block], stop_reason="end_turn"),
            _FakeResponse(content=[diagnosis_block], stop_reason="end_turn"),
        ]
    )

    runner = ClaudeRunner(model="claude-opus-5")
    runner._client = lambda: fake_client

    recorder = TraceRecorder(
        dag_id=failure_ctx.dag_id,
        dag_run_id=failure_ctx.dag_run_id,
        task_id=failure_ctx.task_id,
        try_number=failure_ctx.try_number,
    )

    result = runner.run(failure_ctx, recorder)

    assert result.diagnosis.offending_field == "customer_name"
    kwargs = fake_client.messages.calls[-1]
    assert kwargs["output_config"]["format"]["type"] == "json_schema"
    assert kwargs["output_config"]["effort"] == "medium"
    assert "temperature" not in kwargs
