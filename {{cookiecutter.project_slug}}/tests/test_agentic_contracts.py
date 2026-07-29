import json

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    AgentResult,
    FailureContext,
    ProposedEdit,
    Trace,
    TraceElement,
)


def _element(seq: int, kind: str, name: str) -> TraceElement:
    return TraceElement(
        seq=seq,
        kind=kind,
        name=name,
        status="ok",
        started_at="2026-07-29T00:00:00Z",
        ended_at="2026-07-29T00:00:01Z",
        duration_ms=1000,
    )


def test_failure_context_round_trips():
    ctx = FailureContext(
        dag_id="example_events_dag",
        dag_run_id="run_1",
        task_id="load",
        try_number=1,
        exception_type="SchemaDriftError",
        exception_message="unexpected field 'customer_name'",
        log_path="/logs/load.log",
        project_root="/project",
    )
    assert FailureContext.model_validate(ctx.model_dump()) == ctx


def test_trace_serializes_to_a_json_array():
    trace = Trace(
        trace_id="t1",
        dag_id="d",
        dag_run_id="r",
        task_id="load",
        try_number=1,
        occurred_at="2026-07-29T00:00:00Z",
        status="ok",
        elements=[_element(0, "agent_run", "heal"), _element(1, "tool_call", "read_task_log")],
    )

    parsed = json.loads(trace.to_json_array())

    assert isinstance(parsed, list)
    assert [e["seq"] for e in parsed] == [0, 1]
    assert parsed[1]["name"] == "read_task_log"


def test_trace_json_array_omits_trace_level_metadata():
    # The array holds elements only; correlation columns live beside it in storage.
    trace = Trace(
        trace_id="t1",
        dag_id="d",
        dag_run_id="r",
        task_id="load",
        try_number=1,
        occurred_at="2026-07-29T00:00:00Z",
        status="ok",
        elements=[_element(0, "agent_run", "heal")],
    )

    parsed = json.loads(trace.to_json_array())

    assert "trace_id" not in parsed[0]


def test_agent_result_holds_diagnosis_edits_and_elements():
    result = AgentResult(
        diagnosis=AgentDiagnosis(
            root_cause="field renamed",
            offending_field="customer_name",
            confidence="high",
            patch_summary="rename customer_name to name",
        ),
        edits=[
            ProposedEdit(
                path="src/pkg/dag.py",
                old_str="customer_name",
                new_str="name",
                rationale="matches DDL column",
            )
        ],
        elements=[_element(0, "result", "done")],
    )

    assert result.diagnosis.confidence == "high"
    assert result.edits[0].new_str == "name"
