import os
import uuid

from conftest import bake_with


def test_default_bake_succeeds(cookies):
    result = cookies.bake()
    assert result.exit_code == 0
    assert result.exception is None
    assert result.project_path.is_dir()
    assert (result.project_path / "README.md").is_file()
    assert (result.project_path / ".gitignore").is_file()


def test_all_patterns_off_removes_pattern_dirs(cookies):
    result = bake_with(
        cookies,
        include_batch=False,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=False,
    )
    assert result.exit_code == 0
    project = result.project_path
    slug = result.context["project_slug"]
    assert not (project / "src" / slug / "batch").exists()
    assert not (project / "src" / slug / "streaming").exists()
    assert not (project / "src" / slug / "lakehouse").exists()
    assert not (project / "transformation").exists()


def test_all_patterns_on_keeps_pattern_dirs(cookies):
    result = bake_with(cookies)
    assert result.exit_code == 0
    project = result.project_path
    slug = result.context["project_slug"]
    assert (project / "src" / slug / "batch").is_dir()
    assert (project / "src" / slug / "streaming").is_dir()
    assert (project / "src" / slug / "lakehouse").is_dir()
    assert (project / "transformation").is_dir()


def test_pyproject_declares_only_enabled_pattern_deps(cookies):
    result = bake_with(
        cookies,
        include_batch=True,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=False,
    )
    pyproject = (result.project_path / "pyproject.toml").read_text()
    assert "apache-airflow" in pyproject
    assert "confluent-kafka" not in pyproject
    assert "pyiceberg" not in pyproject
    assert "dbt-duckdb" not in pyproject
    assert 'requires-python = ">=3.12"' in pyproject
    assert "[build-system]" in pyproject


def test_precommit_includes_sqlfluff_only_when_dbt_enabled(cookies):
    with_dbt = bake_with(cookies, include_dbt=True)
    without_dbt = bake_with(cookies, include_dbt=False)

    with_dbt_config = (with_dbt.project_path / ".pre-commit-config.yaml").read_text()
    without_dbt_config = (without_dbt.project_path / ".pre-commit-config.yaml").read_text()

    assert "sqlfluff" in with_dbt_config
    assert "sqlfluff" not in without_dbt_config
    assert "ruff-pre-commit" in with_dbt_config
    assert "ruff-pre-commit" in without_dbt_config
    assert "sqlfluff-templater-dbt==3.1.0" in with_dbt_config
    assert "dbt-duckdb>=1.8" in with_dbt_config


def test_ci_includes_dbt_test_step_only_when_dbt_enabled(cookies):
    with_dbt = bake_with(cookies, include_dbt=True)
    without_dbt = bake_with(cookies, include_dbt=False)

    with_dbt_ci = (with_dbt.project_path / ".github" / "workflows" / "ci.yml").read_text()
    without_dbt_ci = (without_dbt.project_path / ".github" / "workflows" / "ci.yml").read_text()

    assert "dbt test" in with_dbt_ci
    assert "dbt test" not in without_dbt_ci
    assert "ruff check" in with_dbt_ci
    assert "pytest" in with_dbt_ci


def test_duckdb_scoped_to_lakehouse_pyiceberg_removed(cookies):
    with_lakehouse = bake_with(cookies, include_lakehouse=True)
    without_lakehouse = bake_with(cookies, include_lakehouse=False, include_dbt=True)
    with_pyproject = (with_lakehouse.project_path / "pyproject.toml").read_text()
    without_pyproject = (without_lakehouse.project_path / "pyproject.toml").read_text()

    assert '"duckdb>=1.0"' in with_pyproject
    assert '"duckdb>=1.0"' not in without_pyproject


def test_airflow_version_pin_matches_across_files(cookies):
    result = bake_with(cookies, include_batch=True)

    airflow_version = result.context["airflow_version"]

    pyproject = (result.project_path / "pyproject.toml").read_text()
    compose = (result.project_path / "docker-compose.yml").read_text()

    assert f"apache-airflow>={airflow_version}" in pyproject
    assert f"apache/airflow:{airflow_version}" in compose


def test_bake_fails_fast_for_invalid_project_slug(cookies):
    result = cookies.bake(extra_context={"project_name": "3D Data!"})
    assert result.exit_code != 0
    assert result.exception is not None


def test_bake_succeeds_for_project_name_with_spaces_and_hyphens(cookies):
    result = cookies.bake(extra_context={"project_name": "My Cool-Project"})
    assert result.exit_code == 0
    assert result.context["project_slug"] == "my_cool_project"


def test_hooks_do_not_execute_code_injected_via_project_name(cookies):
    marker = f"/tmp/cookiecutter_injection_marker_{uuid.uuid4().hex}"
    malicious_name = 'x";open(' + repr(marker) + ',"w").close();y="'
    try:
        cookies.bake(extra_context={"project_name": malicious_name})
        assert not os.path.exists(marker), (
            "hooks/*_gen_project.py executed code injected via project_name "
            "instead of treating cookiecutter.project_slug as an inert string"
        )
    finally:
        if os.path.exists(marker):
            os.remove(marker)
