import os

from conftest import bake_with


def test_batch_dag_present_when_enabled(cookies):
    result = bake_with(cookies, include_batch=True)
    slug = result.context["project_slug"]
    dag_path = result.project_path / "src" / slug / "batch" / "dags" / "example_dag.py"
    assert dag_path.is_file()
    assert "events" in dag_path.read_text()


def test_env_file_pins_airflow_uid_to_the_baking_host_uid(cookies):
    # compose's airflow-init service is what keeps the bind mounts writable;
    # this file only decides *which* uid they end up owned by. Pinning the
    # baking host's uid makes init's chown a no-op, so the logs and patches
    # stay readable and deletable as the host user rather than as uid 50000.
    result = bake_with(cookies, include_batch=True)
    env_path = result.project_path / ".env"
    assert env_path.is_file()
    assert f"AIRFLOW_UID={os.getuid()}" in env_path.read_text().splitlines()


def test_compose_chowns_the_bind_mounts_before_airflow_starts(cookies):
    # Without this, a fresh clone has no .env, AIRFLOW_UID falls back to
    # 50000, and on Linux that uid cannot write to the host-owned ./logs --
    # so the demo's failing task produces no log for the agent to read. The
    # init service must run as root and must complete before airflow starts,
    # or the chown races the task that depends on it.
    import yaml

    result = bake_with(cookies, include_batch=True)
    compose = yaml.safe_load((result.project_path / "docker-compose.yml").read_text())

    init = compose["services"]["airflow-init"]
    assert init["user"] == "0:0"
    assert "./logs:/sources/logs" in init["volumes"]

    depends = compose["services"]["airflow-standalone"]["depends_on"]
    assert depends["airflow-init"]["condition"] == "service_completed_successfully"


def test_compose_has_no_init_service_without_batch(cookies):
    import yaml

    result = bake_with(cookies, include_batch=False, include_agentic=False)
    compose = yaml.safe_load((result.project_path / "docker-compose.yml").read_text())
    assert "airflow-init" not in compose["services"]


def test_env_file_absent_when_batch_disabled(cookies):
    # No airflow service means no uid to pin.
    result = bake_with(cookies, include_batch=False, include_agentic=False)
    assert not (result.project_path / ".env").exists()


def test_env_file_is_gitignored(cookies):
    # The pinned uid is a fact about one host; committing it would hand the
    # next machine a stale value that silently breaks the same mounts.
    result = bake_with(cookies, include_batch=True)
    assert ".env" in (result.project_path / ".gitignore").read_text().splitlines()
