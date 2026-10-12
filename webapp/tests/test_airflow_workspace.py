"""Exercise staged edits and deployment without modifying the real demo platform."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))
from airflow_workspace import AirflowWorkspace  # noqa: E402

DAG = "customer_revenue_daily"
DAG_FILE = f"src/schema_therapy/batch/dags/{DAG}.py"


@pytest.fixture
def workspace(tmp_path):
    project = tmp_path / "project"
    path = project / DAG_FILE
    path.parent.mkdir(parents=True)
    path.write_text('"""Demo DAG."""\nvalue = "old"\n')
    (project / "transformation/models").mkdir(parents=True)
    commands = []

    def runner(args, **kwargs):
        commands.append((args, kwargs))
        if "-c" in args:
            payload = json.loads(kwargs["input"])
            if payload["mode"] == "validate":
                data = {"valid": True, "dag_id": DAG, "tasks": ["ingest_supabase", "transform_with_dbt"], "dbt_parse": "passed"}
            else:
                data = {"file_hashes": {name: hashlib.sha256((project / name).read_bytes()).hexdigest() for name in payload["files"]}}
            return subprocess.CompletedProcess(args, 0, json.dumps(data), "")
        return subprocess.CompletedProcess(args, 0, "Serialized DAGs", "")

    return AirflowWorkspace(project, tmp_path / "state", runner=runner), project, commands


def stage(workspace):
    tools, _project, _commands = workspace
    current = tools.read_airflow_file(DAG_FILE)
    return tools.edit_airflow_file(DAG_FILE, current["sha256"], '"old"', '"new"')


def test_edits_are_staged_and_diff_is_reviewable(workspace):
    tools, project, _commands = workspace
    result = stage(workspace)
    assert '"old"' in (project / DAG_FILE).read_text()
    assert '"new"' in tools.read_airflow_file(DAG_FILE)["content"]
    assert '-value = "old"' in result["diff"]
    assert '+value = "new"' in result["diff"]
    assert result["deployed"] is False


@pytest.mark.parametrize("path", ["../.env", ".env", "docker-compose.yml", "supabase/migrations/change.sql", "/tmp/dag.py", "src/schema_therapy/source_seed.py"])
def test_source_credentials_and_outside_paths_are_not_editable(workspace, path):
    tools, _project, _commands = workspace
    with pytest.raises(ValueError, match="editable downstream"):
        tools.read_airflow_file(path)


def test_symlink_escape_is_rejected(workspace, tmp_path):
    tools, project, _commands = workspace
    external = tmp_path / "secret.py"
    external.write_text("secret = 1")
    (project / DAG_FILE).unlink()
    (project / DAG_FILE).symlink_to(external)
    with pytest.raises(ValueError, match="symlink"):
        tools.read_airflow_file(DAG_FILE)


def test_edit_rejects_stale_or_ambiguous_text_and_invalid_python(workspace):
    tools, _project, _commands = workspace
    current = tools.read_airflow_file(DAG_FILE)
    with pytest.raises(ValueError, match="changed"):
        tools.edit_airflow_file(DAG_FILE, "0" * 64, "old", "new")
    with pytest.raises(ValueError, match="exactly once"):
        tools.edit_airflow_file(DAG_FILE, current["sha256"], "missing", "new")
    with pytest.raises(SyntaxError):
        tools.edit_airflow_file(DAG_FILE, current["sha256"], '"old"', '"unclosed')
    assert tools.read_airflow_file(DAG_FILE)["staged"] is False


def test_deploy_revalidates_then_publishes_and_refreshes_airflow(workspace):
    tools, project, commands = workspace
    staged = stage(workspace)
    result = tools.deploy_airflow_changes(DAG, staged["workspace_revision"])
    assert result["deployed"] is True
    assert '"new"' in (project / DAG_FILE).read_text()
    assert tools.read_airflow_file(DAG_FILE)["staged"] is False
    assert any(args[-3:] == ["dags", "reserialize", "--bundle-name=dags-folder"] for args, _kwargs in commands)
    assert result["runtime_files_match"] is True


def test_deploy_refuses_revision_drift_or_presenter_changes(workspace):
    tools, project, commands = workspace
    staged = stage(workspace)
    with pytest.raises(ValueError, match="revision"):
        tools.deploy_airflow_changes(DAG, "0" * 64)
    (project / DAG_FILE).write_text("presenter_change = True\n")
    with pytest.raises(ValueError, match="changed since"):
        tools.deploy_airflow_changes(DAG, staged["workspace_revision"])
    assert commands == []


def test_test_file_edits_publish_locally_without_requiring_a_docker_test_mount(workspace):
    tools, project, commands = workspace
    path = project / "tests/test_batch_dag.py"
    path.parent.mkdir()
    path.write_text('value = "old"\n')
    current = tools.read_airflow_file("tests/test_batch_dag.py")
    staged = tools.edit_airflow_file("tests/test_batch_dag.py", current["sha256"], '"old"', '"new"')
    tools.deploy_airflow_changes(DAG, staged["workspace_revision"])
    assert '"new"' in path.read_text()
    status_calls = [json.loads(kwargs["input"]) for _args, kwargs in commands if kwargs.get("input") and json.loads(kwargs["input"])["mode"] == "status"]
    assert status_calls[0]["files"] == []


def test_failed_validation_keeps_live_files_untouched(workspace):
    tools, project, _commands = workspace
    staged = stage(workspace)
    tools.runner = lambda args, **kwargs: subprocess.CompletedProcess(args, 1, "", "DAG import failed")
    with pytest.raises(RuntimeError, match="DAG import failed"):
        tools.deploy_airflow_changes(DAG, staged["workspace_revision"])
    assert '"old"' in (project / DAG_FILE).read_text()


def test_discard_can_recover_a_stale_patch_without_touching_presenter_files(workspace):
    tools, project, _commands = workspace
    staged = stage(workspace)
    (project / DAG_FILE).write_text("presenter_change = True\n")
    tools.discard_airflow_changes(DAG, staged["workspace_revision"])
    assert tools.read_airflow_file(DAG_FILE)["content"] == "presenter_change = True\n"
    assert tools.list_airflow_files(DAG)["staged_files"] == []


def test_serialization_failure_restores_live_files_and_keeps_staged_patch(workspace):
    tools, project, _commands = workspace
    staged = stage(workspace)
    original = tools.runner

    def fail_serialization(args, **kwargs):
        if "reserialize" in args:
            return subprocess.CompletedProcess(args, 1, "", "Serialization failed")
        return original(args, **kwargs)

    tools.runner = fail_serialization
    with pytest.raises(RuntimeError, match="restored"):
        tools.deploy_airflow_changes(DAG, staged["workspace_revision"])
    assert '"old"' in (project / DAG_FILE).read_text()
    assert tools.read_airflow_file(DAG_FILE)["staged"] is True
