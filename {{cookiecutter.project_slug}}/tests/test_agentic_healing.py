import json

import pytest

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    FailureContext,
    ProposedEdit,
)
from {{ cookiecutter.project_slug }}.agentic.dags.healing_dag import (
    FAILURE_XCOM_KEY,
    build_failure_context,
    record_failure,
    resolve_log_path,
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


class HealerTaskInstance:
    """What Airflow actually injects into heal_on_failure's context.

    Note the task_id: heal_on_failure is an ordinary downstream task, so the
    TaskInstance it is handed is *its own*. Every test below builds the
    context this way on purpose -- a build_failure_context that reads
    `ti.task_id` would name the healer as the failing task and derive the
    healer's own log path.
    """

    dag_id = "example_events_dag"
    task_id = "heal_on_failure"
    try_number = 1

    def __init__(self, xcom: dict | None = None) -> None:
        self.xcom = xcom
        self.pulled: list[tuple] = []

    def xcom_pull(self, task_ids=None, key=None):
        self.pulled.append((task_ids, key))
        return self.xcom


def _healer_context(xcom: dict | None = None) -> dict:
    return {
        "task_instance": HealerTaskInstance(xcom),
        "dag_run": type("R", (), {"run_id": "run_9"})(),
    }


def test_build_failure_context_describes_the_failed_task_not_the_healer(project):
    context = _healer_context()

    ctx = build_failure_context(
        context, project, "load", log_path=project / "load.log"
    )

    assert ctx.dag_id == "example_events_dag"
    assert ctx.dag_run_id == "run_9"
    assert ctx.task_id == "load"
    assert ctx.task_id != context["task_instance"].task_id


def test_build_failure_context_derives_the_log_path_from_the_failed_task(project):
    context = _healer_context()

    ctx = build_failure_context(context, project, "load")

    # Whatever Airflow's configured layout is, the healer's own task id must
    # not appear in the path it hands the agent.
    assert "heal_on_failure" not in ctx.log_path
    assert "load" in ctx.log_path


def test_build_failure_context_reads_the_exception_the_failed_task_pushed(project):
    context = _healer_context(
        {
            "task_id": "load",
            "try_number": 2,
            "exception_type": "ValueError",
            "exception_message": "boom",
        }
    )

    ctx = build_failure_context(
        context, project, "load", log_path=project / "load.log"
    )

    assert ctx.exception_type == "ValueError"
    assert ctx.exception_message == "boom"
    assert ctx.try_number == 2
    # Pulled from the failed task, keyed so an unrelated XCom cannot satisfy it.
    assert context["task_instance"].pulled == [("load", FAILURE_XCOM_KEY)]


def test_build_failure_context_degrades_when_no_failure_payload_was_pushed(project):
    ctx = build_failure_context(
        _healer_context(None), project, "load", log_path=project / "load.log"
    )

    assert ctx.exception_type == "UnknownError"
    assert ctx.exception_message == ""
    assert ctx.try_number == 1


def test_record_failure_pushes_the_exception_and_attempt():
    class FailingTaskInstance:
        task_id = "load"
        try_number = 3

        def __init__(self) -> None:
            self.pushed: dict = {}

        def xcom_push(self, key, value):
            self.pushed[key] = value

    ti = FailingTaskInstance()

    record_failure({"task_instance": ti, "exception": ValueError("boom")})

    payload = ti.pushed[FAILURE_XCOM_KEY]
    assert payload["task_id"] == "load"
    assert payload["try_number"] == 3
    assert payload["exception_type"] == "ValueError"
    assert payload["exception_message"] == "boom"


def test_resolve_log_path_is_rooted_at_airflows_log_folder():
    from airflow.configuration import conf

    path = resolve_log_path("example_events_dag", "run_9", "load", 1)

    assert str(path).startswith(conf.get("logging", "base_log_folder"))
    assert "load" in str(path)


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

    _trace, published = run_healing(
        failure_ctx,
        runner=runner,
        sink=ExplodingSink(),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    assert published.patch_path is not None
    assert "failed to persist agent trace" in caplog.text


def test_run_healing_defaults_artifacts_to_the_project_root_not_the_cwd(
    project, failure_ctx, runner, monkeypatch, tmp_path
):
    # An Airflow worker's CWD is $AIRFLOW_HOME, which is not a mounted
    # volume. Run from somewhere that is neither the project nor a mount and
    # assert the patch still lands under the project root the context names.
    elsewhere = tmp_path / "worker_cwd"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    monkeypatch.delenv("AGENTIC_GITHUB_REPO", raising=False)

    trace, published = run_healing(
        failure_ctx, runner=runner, sink=JsonlSink(project / "traces")
    )

    assert published.patch_path == str(project / "artifacts" / f"{trace.trace_id}.patch")
    assert (project / "artifacts" / f"{trace.trace_id}.json").is_file()
    assert not (elsewhere / "artifacts").exists()
{%- if not cookiecutter.include_lakehouse %}


def test_run_healing_defaults_traces_to_the_project_root_not_the_cwd(
    project, failure_ctx, runner, monkeypatch, tmp_path
):
    elsewhere = tmp_path / "worker_cwd"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    monkeypatch.delenv("AGENTIC_GITHUB_REPO", raising=False)

    trace, _ = run_healing(failure_ctx, runner=runner)

    assert (project / "artifacts" / "traces" / f"{trace.trace_id}.json").is_file()
    assert not (elsewhere / "artifacts" / "traces").exists()
{%- endif %}
