import json

import pytest

from {{ cookiecutter.project_slug }}.agentic.tracing import (
    JsonlSink,
    TraceRecorder,
    write_trace_safely,
)


def _recorder() -> TraceRecorder:
    return TraceRecorder(
        dag_id="example_events_dag",
        dag_run_id="run_1",
        task_id="load",
        try_number=1,
    )


def test_recorder_numbers_elements_in_emission_order():
    rec = _recorder()
    with rec.element("tool_call", "read_task_log"):
        pass
    with rec.element("tool_call", "propose_patch"):
        pass

    trace = rec.finish("ok")

    assert [e.seq for e in trace.elements] == [0, 1, 2]
    assert [e.name for e in trace.elements] == ["read_task_log", "propose_patch", "heal"]


def test_recorder_records_duration_and_ok_status():
    rec = _recorder()
    with rec.element("tool_call", "read_repo_file"):
        pass

    trace = rec.finish("ok")
    element = trace.elements[0]

    assert element.status == "ok"
    assert element.ended_at is not None
    assert element.duration_ms is not None and element.duration_ms >= 0


def test_recorder_marks_a_raising_element_as_error_and_reraises():
    rec = _recorder()

    with pytest.raises(ValueError):
        with rec.element("tool_call", "read_repo_file"):
            raise ValueError("path escapes project root")

    trace = rec.finish("error")

    assert trace.elements[0].status == "error"
    assert "escapes project root" in trace.elements[0].error


def test_finish_appends_a_terminal_agent_run_element():
    trace = _recorder().finish("error")

    terminal = trace.elements[-1]
    assert terminal.kind == "agent_run"
    assert terminal.name == "heal"
    assert terminal.status == "error"
    assert trace.status == "error"


def test_jsonl_sink_writes_a_bare_json_array(tmp_path):
    rec = _recorder()
    with rec.element("tool_call", "read_task_log"):
        pass
    trace = rec.finish("ok")

    JsonlSink(tmp_path).write(trace)

    written = (tmp_path / f"{trace.trace_id}.json").read_text()
    parsed = json.loads(written)
    assert isinstance(parsed, list)
    assert parsed[0]["name"] == "read_task_log"


def test_write_trace_safely_swallows_sink_failure(caplog):
    class ExplodingSink:
        def write(self, trace):
            raise RuntimeError("table agent_traces does not exist")

    trace = _recorder().finish("ok")

    assert write_trace_safely(ExplodingSink(), trace) is False
    assert "agent_traces" in caplog.text


def test_write_trace_safely_reports_success(tmp_path):
    trace = _recorder().finish("ok")
    assert write_trace_safely(JsonlSink(tmp_path), trace) is True
{%- if cookiecutter.include_lakehouse %}


def test_duckdb_sink_stores_one_row_per_failure_event(tmp_path):
    import duckdb

    from {{ cookiecutter.project_slug }}.agentic.tracing import DuckDBSink

    db_path = str(tmp_path / "lakehouse.duckdb")
    rec = _recorder()
    with rec.element("tool_call", "read_task_log"):
        pass
    trace = rec.finish("ok")

    DuckDBSink(db_path).write(trace)

    con = duckdb.connect(db_path)
    rows = con.execute("SELECT trace_id, dag_id, status, elements FROM agent_traces").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == trace.trace_id
    assert rows[0][1] == "example_events_dag"
    assert rows[0][2] == "ok"
    assert json.loads(rows[0][3])[0]["name"] == "read_task_log"


def test_both_sinks_persist_identical_documents(tmp_path):
    import duckdb

    from {{ cookiecutter.project_slug }}.agentic.tracing import DuckDBSink

    rec = _recorder()
    with rec.element("tool_call", "read_task_log"):
        pass
    trace = rec.finish("ok")

    JsonlSink(tmp_path).write(trace)
    db_path = str(tmp_path / "lakehouse.duckdb")
    DuckDBSink(db_path).write(trace)

    from_jsonl = (tmp_path / f"{trace.trace_id}.json").read_text()
    con = duckdb.connect(db_path)
    from_duckdb = con.execute("SELECT elements FROM agent_traces").fetchone()[0]

    assert json.loads(from_jsonl) == json.loads(from_duckdb)
{%- endif %}
