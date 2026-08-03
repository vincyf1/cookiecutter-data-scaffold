import json
import os
import subprocess

import pytest

from conftest import bake_with


def test_agentic_flag_is_in_context(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    assert result.exit_code == 0
    assert result.context["include_agentic"] is True
    assert result.context["agentic_model"] == "claude-haiku-4-5"
    assert result.context["agentic_grader_model"] == "claude-haiku-4-5"


def test_agentic_without_batch_is_rejected(cookies, capfd):
    result = bake_with(cookies, include_agentic=True, include_batch=False)
    assert result.exit_code != 0
    assert result.exception is not None
    # pre_gen_project.py's guard runs in a subprocess and prints straight to
    # the inherited fd 1 (cookiecutter's FailedHookException carries no
    # message of its own beyond "exit status: 1"), so capfd -- not capsys --
    # is what sees it. Asserting on this text is what distinguishes "the
    # include_agentic-requires-include_batch guard fired" from "the bake
    # failed for some unrelated reason".
    out, _ = capfd.readouterr()
    assert "include_agentic requires include_batch" in out


def test_agentic_with_batch_bakes_cleanly(cookies):
    result = bake_with(
        cookies,
        include_agentic=True,
        include_batch=True,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=False,
    )
    assert result.exit_code == 0


@pytest.mark.parametrize("agentic", [True, False])
def test_bake_succeeds_with_flag_either_way(cookies, agentic):
    result = bake_with(cookies, include_agentic=agentic, include_batch=True)
    assert result.exit_code == 0


def test_agentic_module_present_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    slug = result.context["project_slug"]
    agentic = result.project_path / "src" / slug / "agentic"
    assert (agentic / "contracts.py").is_file()
    assert (result.project_path / "tests" / "test_agentic_contracts.py").is_file()


def test_agentic_module_absent_when_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    slug = result.context["project_slug"]
    assert not (result.project_path / "src" / slug / "agentic").exists()
    assert not (result.project_path / "tests" / "test_agentic_contracts.py").exists()


def test_agentic_tracing_files_present_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    slug = result.context["project_slug"]
    agentic = result.project_path / "src" / slug / "agentic"
    assert (agentic / "tracing.py").is_file()
    assert (agentic / "create_traces_table.sql").is_file()
    assert (result.project_path / "tests" / "test_agentic_tracing.py").is_file()


def test_agentic_tracing_files_absent_when_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    slug = result.context["project_slug"]
    agentic = result.project_path / "src" / slug / "agentic"
    assert not (agentic / "tracing.py").exists()
    assert not (agentic / "create_traces_table.sql").exists()
    assert not (result.project_path / "tests" / "test_agentic_tracing.py").exists()


def test_create_traces_table_sql_is_removed_without_lakehouse(cookies):
    # create_traces_table.sql is read only by DuckDBSink, which never
    # renders when include_lakehouse=False. Shipping it anyway would be
    # unused code left behind -- the exact thing the README promises this
    # template never does. It is declared on include_lakehouse's "remove"
    # list (not include_agentic's) in hooks/post_gen_project.py, so it is
    # gone regardless of which Pattern happens to be processed first.
    result = bake_with(
        cookies, include_agentic=True, include_batch=True, include_lakehouse=False
    )
    slug = result.context["project_slug"]
    assert not (
        result.project_path / "src" / slug / "agentic" / "create_traces_table.sql"
    ).exists()


def test_agentic_tools_present_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    slug = result.context["project_slug"]
    agentic = result.project_path / "src" / slug / "agentic"
    assert (agentic / "tools.py").is_file()
    assert (result.project_path / "tests" / "test_agentic_tools.py").is_file()


def test_agentic_tools_absent_when_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    slug = result.context["project_slug"]
    agentic = result.project_path / "src" / slug / "agentic"
    assert not (agentic / "tools.py").exists()
    assert not (result.project_path / "tests" / "test_agentic_tools.py").exists()


def test_gitignore_agentic_rules_track_the_flag(cookies):
    # artifacts/ and evals/reports/ are both deleted by the post-gen hook when
    # the Pattern is off, so their ignore rules would be ignoring paths that
    # cannot exist -- a dead branch of exactly the kind the template promises
    # not to ship.
    on = bake_with(cookies, include_agentic=True, include_batch=True)
    off = bake_with(cookies, include_agentic=False, include_batch=True)
    on_text = (on.project_path / ".gitignore").read_text()
    off_text = (off.project_path / ".gitignore").read_text()
    assert "artifacts/*" in on_text
    assert "evals/reports/" in on_text
    assert "artifacts" not in off_text
    assert "evals" not in off_text
    # The rules shared with every other Pattern must survive either way.
    assert "logs/*" in off_text


def test_pydantic_dependency_tracks_the_flag(cookies):
    on = bake_with(cookies, include_agentic=True, include_batch=True)
    off = bake_with(cookies, include_agentic=False, include_batch=True)
    assert "pydantic" in (on.project_path / "pyproject.toml").read_text()
    assert "pydantic" not in (off.project_path / "pyproject.toml").read_text()


def test_agentic_runner_present_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    slug = result.context["project_slug"]
    agentic = result.project_path / "src" / slug / "agentic"
    assert (agentic / "runner.py").is_file()
    assert (agentic / "prompts" / "diagnose.md").is_file()
    assert (result.project_path / "tests" / "test_agentic_runner.py").is_file()


def test_agentic_runner_absent_when_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    slug = result.context["project_slug"]
    agentic = result.project_path / "src" / slug / "agentic"
    assert not (agentic / "runner.py").exists()
    assert not (agentic / "prompts" / "diagnose.md").exists()
    assert not (result.project_path / "tests" / "test_agentic_runner.py").exists()


def test_agentic_publisher_present_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    slug = result.context["project_slug"]
    agentic = result.project_path / "src" / slug / "agentic"
    assert (agentic / "publisher.py").is_file()
    assert (result.project_path / "tests" / "test_agentic_publisher.py").is_file()


def test_agentic_publisher_absent_when_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    slug = result.context["project_slug"]
    agentic = result.project_path / "src" / slug / "agentic"
    assert not (agentic / "publisher.py").exists()
    assert not (result.project_path / "tests" / "test_agentic_publisher.py").exists()


def test_anthropic_dependency_tracks_the_flag(cookies):
    on = bake_with(cookies, include_agentic=True, include_batch=True)
    off = bake_with(cookies, include_agentic=False, include_batch=True)
    assert "anthropic" in (on.project_path / "pyproject.toml").read_text()
    assert "anthropic" not in (off.project_path / "pyproject.toml").read_text()


def test_evals_present_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    evals = result.project_path / "evals"
    assert (evals / "__init__.py").is_file()
    assert (evals / "harness.py").is_file()
    assert (evals / "replay.py").is_file()
    assert (evals / "conftest.py").is_file()
    assert (evals / "test_cases.py").is_file()
    assert (evals / "cases" / "schema_drift.yaml").is_file()
    assert (evals / "cassettes" / "schema_drift.json").is_file()
    assert (evals / "cases" / "missing_config.yaml").is_file()
    assert (evals / "cassettes" / "missing_config.json").is_file()


def test_every_eval_case_has_a_cassette(cookies):
    # The replay tier is the deterministic gate; a case with no cassette
    # fails it by FileNotFoundError rather than by scoring badly.
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    evals = result.project_path / "evals"
    case_ids = {path.stem for path in (evals / "cases").glob("*.yaml")}
    cassette_ids = {path.stem for path in (evals / "cassettes").glob("*.json")}
    assert case_ids <= cassette_ids


def test_evals_absent_when_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    assert not (result.project_path / "evals").exists()


def test_pyproject_testpaths_track_the_flag(cookies):
    on = bake_with(cookies, include_agentic=True, include_batch=True)
    off = bake_with(cookies, include_agentic=False, include_batch=True)
    on_text = (on.project_path / "pyproject.toml").read_text()
    off_text = (off.project_path / "pyproject.toml").read_text()
    assert "evals" in on_text
    assert "testpaths" in on_text
    assert "testpaths" not in off_text
    assert "evals" not in off_text


def test_compose_mounts_the_agentic_dags_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    compose = (result.project_path / "docker-compose.yml").read_text()
    assert "ANTHROPIC_API_KEY" in compose


def test_compose_omits_the_api_key_when_agentic_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    compose = (result.project_path / "docker-compose.yml").read_text()
    assert "ANTHROPIC_API_KEY" not in compose


def test_ci_runs_evals_deterministically_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    ci = (result.project_path / ".github" / "workflows" / "ci.yml").read_text()
    assert "not live and not graded" in ci


def test_compose_mounts_the_artifact_and_log_volumes(cookies):
    # The healing task writes its patch under the project root and reads the
    # failing task's log from Airflow's base_log_folder. Neither is on a
    # mounted volume by default, so both are lost to the host without these.
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    compose = (result.project_path / "docker-compose.yml").read_text()
    assert "./artifacts:/opt/airflow/project/artifacts" in compose
    assert "./logs:/opt/airflow/logs" in compose


def test_compose_init_chowns_artifacts_only_when_agentic_enabled(cookies):
    # The chown list is built from one Jinja expression, so asserting the
    # exact command is what catches a stray separator or a dropped path --
    # `mkdir -p /sources/logs/sources/artifacts` is a single valid path and
    # would leave ./artifacts unwritable with nothing failing loudly.
    #
    # Both services must interpolate the *same* uid expression: init chowns
    # to whatever airflow-standalone will run as. If they ever diverge, init
    # hands the mounts to a uid that is not the one doing the writing.
    import yaml

    for agentic, dirs in [(True, "/sources/logs /sources/artifacts"), (False, "/sources/logs")]:
        result = bake_with(cookies, include_agentic=agentic, include_batch=True)
        compose = yaml.safe_load((result.project_path / "docker-compose.yml").read_text())
        services = compose["services"]

        assert services["airflow-init"]["command"] == [
            "-c",
            f'mkdir -p {dirs} && chown -R "${{AIRFLOW_UID:-50000}}:0" {dirs}',
        ]
        assert services["airflow-standalone"]["user"] == "${AIRFLOW_UID:-50000}:0"


def test_compose_omits_the_artifacts_mount_when_agentic_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    compose = (result.project_path / "docker-compose.yml").read_text()
    assert "artifacts" not in compose


def test_artifacts_directory_is_shipped_and_gitignored(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    assert (result.project_path / "artifacts" / ".gitkeep").is_file()
    gitignore = (result.project_path / ".gitignore").read_text()
    assert "artifacts/*" in gitignore
    assert "!artifacts/.gitkeep" in gitignore


def test_artifacts_directory_is_absent_when_agentic_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    assert not (result.project_path / "artifacts").exists()


def test_generated_readme_documents_the_agentic_pattern(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    readme = (result.project_path / "README.md").read_text()
    assert "failure_mode" in readme
    assert "artifacts/" in readme


@pytest.mark.parametrize(
    ("lakehouse", "mode"),
    [(True, "schema_drift"), (False, "missing_config")],
)
def test_readme_documents_the_failure_mode_this_bake_actually_ships(
    cookies, lakehouse, mode
):
    # The README's lakehouse-off branch used to promise a schema_drift
    # failure that this configuration cannot raise -- there is no DDL to
    # drift from, so `load` succeeded and the ALL_FAILED healer was skipped.
    result = bake_with(
        cookies, include_agentic=True, include_batch=True, include_lakehouse=lakehouse
    )
    readme = (result.project_path / "README.md").read_text()
    dag = (
        result.project_path
        / "src"
        / result.context["project_slug"]
        / "batch"
        / "dags"
        / "example_dag.py"
    ).read_text()

    assert mode in readme
    assert f'FAILURE_MODES = ["none", "{mode}"]' in dag


def test_pipeline_config_ships_only_where_there_is_no_ddl(cookies):
    # batch/pipeline.toml is the declared source the missing_config failure
    # validates against -- the role create_tables.sql plays when the
    # Lakehouse Pattern is on. Shipping both would leave an unused one.
    without = bake_with(cookies, include_batch=True, include_lakehouse=False)
    slug = without.context["project_slug"]
    assert (without.project_path / "src" / slug / "batch" / "config.py").is_file()
    assert (without.project_path / "src" / slug / "batch" / "pipeline.toml").is_file()
    assert (without.project_path / "tests" / "test_batch_config.py").is_file()

    with_ddl = bake_with(cookies, include_batch=True, include_lakehouse=True)
    assert not (with_ddl.project_path / "src" / slug / "batch" / "config.py").exists()
    assert not (with_ddl.project_path / "src" / slug / "batch" / "pipeline.toml").exists()
    assert not (with_ddl.project_path / "tests" / "test_batch_config.py").exists()


# --------------------------------------------------------------------------
# Slow tier: the DAG-to-healer seam, exercised through a real Airflow DagRun.
#
# Every unit test on either side of that seam passed while four defects lived
# inside it -- the healer described its own TaskInstance instead of the failed
# one, the lakehouse-off demo failure never fired, the log path pointed at a
# directory Airflow does not use, and artifacts landed outside the project.
# None of that is visible without actually running the DAG, which is what this
# does. It needs no API key and no network: FakeRunner stands in for the model,
# and tracing, diff rendering, and publishing all run for real.
#
# The tool layer is NOT exercised here -- FakeRunner replays tool *names* into
# the trace without dispatching them, so nothing touches ToolContext. Real tool
# dispatch is covered one tier up, by ReplayRunner in evals/test_replay.py.
# --------------------------------------------------------------------------

HEAL_DRIVER = '''"""Runs one real DagRun and records what the healer was told.

Written into the generated project by tests/test_bake_agentic.py.
"""

import json
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
CAPTURE = PROJECT / "heal_capture.json"

from {slug}.agentic.contracts import AgentDiagnosis, ProposedEdit
from {slug}.agentic.dags import healing_dag
from {slug}.agentic.runner import FakeRunner

captured = {{}}


class CapturingFakeRunner(FakeRunner):
    def run(self, ctx, recorder):
        captured["ctx"] = ctx.model_dump()
        return super().run(ctx, recorder)


# run_healing() resolves its runner as `runner or ClaudeRunner()` against this
# module global, so rebinding it is what keeps the run offline.
healing_dag.ClaudeRunner = lambda: CapturingFakeRunner(
    diagnosis=AgentDiagnosis(
        root_cause="the demo failure",
        offending_field={old_str!s},
        confidence="high",
        patch_summary="apply the demo fix",
    ),
    edits=[
        ProposedEdit(
            path="src/{slug}/batch/dags/example_dag.py",
            old_str={old_str!s},
            new_str={new_str!s},
            rationale="the demo fix",
        )
    ],
    tool_sequence=["read_task_log", "read_repo_file", "propose_patch"],
)

from {slug}.batch.dags.example_dag import example_events_dag

example_events_dag().test(run_conf={{"failure_mode": "{mode}"}})

from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.session import create_session

with create_session() as session:
    states = {{}}
    for task_id in ("load", "heal_on_failure"):
        ti = (
            session.query(TaskInstance)
            .filter_by(dag_id="example_events_dag", task_id=task_id)
            .order_by(TaskInstance.start_date.desc())
            .first()
        )
        states[task_id] = ti.state
        if task_id == "load":
            # Airflow's own renderer, against the real failed TaskInstance.
            # This is the ground truth the healer's derived path must match.
            base = conf.get("logging", "base_log_folder")
            handler = FileTaskHandler(base_log_folder=base)
            relative = handler._render_filename(ti, ti.try_number, session=session)
            captured["canonical_log_path"] = str(Path(base) / relative)
    captured["states"] = states

CAPTURE.write_text(json.dumps(captured, indent=2))
'''


def _run(cwd, *args, env=None):
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    return result


@pytest.mark.slow
@pytest.mark.parametrize(
    ("lakehouse", "mode", "needle", "old_str", "new_str"),
    [
        # old_str/new_str are the edit the stand-in agent stages. render_diff
        # requires old_str to be unique in the file, so these carry enough
        # surrounding context to distinguish the code line from the comment
        # above it -- the same constraint propose_patch puts on a real agent.
        (
            True,
            "schema_drift",
            "customer_name",
            '"customer_name": "example"',
            '"name": "example"',
        ),
        (
            False,
            "missing_config",
            "output_file",
            'output_setting="output_file"',
            'output_setting="output_path"',
        ),
    ],
)
def test_demo_failure_triggers_the_healer_and_lands_a_patch(
    cookies, lakehouse, mode, needle, old_str, new_str
):
    result = bake_with(
        cookies,
        include_batch=True,
        include_streaming=False,
        include_lakehouse=lakehouse,
        include_dbt=False,
        include_agentic=True,
    )
    assert result.exit_code == 0
    project = result.project_path
    slug = result.context["project_slug"]

    _run(project, "uv", "sync")
    (project / "drive_heal.py").write_text(
        HEAL_DRIVER.format(
            slug=slug, mode=mode, old_str=repr(old_str), new_str=repr(new_str)
        )
    )

    env = {
        **os.environ,
        "AIRFLOW_HOME": str(project / ".airflow"),
        # dag.test() creates a real DagRun, which Airflow 3 refuses to do for
        # an unserialized DAG -- so the DAG has to be reachable from the
        # configured dags folder, not merely imported.
        "AIRFLOW__CORE__DAGS_FOLDER": str(project / "src" / slug / "batch" / "dags"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    }
    # Offline and keyless by construction: nothing here may reach the API.
    env.pop("ANTHROPIC_API_KEY", None)

    _run(project, "uv", "run", "airflow", "db", "migrate", env=env)
    _run(project, "uv", "run", "python", "drive_heal.py", env=env)

    captured = json.loads((project / "heal_capture.json").read_text())

    # The demo failure actually fires, and ALL_FAILED lets the healer through.
    assert captured["states"]["load"] == "failed"
    assert captured["states"]["heal_on_failure"] == "success"

    ctx = captured["ctx"]
    # The healer describes the task that failed, not itself.
    assert ctx["task_id"] == "load"
    # The real exception reached it, via the on_failure_callback XCom.
    assert needle in ctx["exception_message"]
    assert ctx["exception_type"] != "UnknownError"
    # The log path matches what Airflow's own FileTaskHandler would render.
    assert ctx["log_path"] == captured["canonical_log_path"]
    # And the artifacts landed under the project root, at the documented path.
    assert ctx["project_root"] == str(project)
    patches = list((project / "artifacts").glob("*.patch"))
    assert len(patches) == 1
    assert new_str in patches[0].read_text()
    assert len(list((project / "artifacts").glob("*.json"))) == 1


@pytest.mark.slow
def test_agentic_bake_generated_tests_pass(cookies):
    result = bake_with(
        cookies,
        include_batch=True,
        include_streaming=False,
        include_lakehouse=True,
        include_dbt=False,
        include_agentic=True,
    )
    assert result.exit_code == 0

    from conftest import run_cmd

    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")


@pytest.mark.slow
def test_agentic_without_lakehouse_generated_tests_pass(cookies):
    result = bake_with(
        cookies,
        include_batch=True,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=False,
        include_agentic=True,
    )
    assert result.exit_code == 0

    from conftest import run_cmd

    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")
