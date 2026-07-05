def test_default_bake_succeeds(cookies):
    result = cookies.bake()
    assert result.exit_code == 0
    assert result.exception is None
    assert result.project_path.is_dir()
    assert (result.project_path / "README.md").is_file()
    assert (result.project_path / ".gitignore").is_file()


def test_all_patterns_off_removes_pattern_dirs(cookies):
    result = cookies.bake(extra_context={
        "include_batch": False,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": False,
    })
    assert result.exit_code == 0
    project = result.project_path
    slug = result.context["project_slug"]
    assert not (project / "src" / slug / "batch").exists()
    assert not (project / "src" / slug / "streaming").exists()
    assert not (project / "src" / slug / "lakehouse").exists()
    assert not (project / "transformation").exists()


def test_all_patterns_on_keeps_pattern_dirs(cookies):
    result = cookies.bake(extra_context={
        "include_batch": True,
        "include_streaming": True,
        "include_lakehouse": True,
        "include_dbt": True,
    })
    assert result.exit_code == 0
    project = result.project_path
    slug = result.context["project_slug"]
    assert (project / "src" / slug / "batch").is_dir()
    assert (project / "src" / slug / "streaming").is_dir()
    assert (project / "src" / slug / "lakehouse").is_dir()
    assert (project / "transformation").is_dir()


def test_pyproject_declares_only_enabled_pattern_deps(cookies):
    result = cookies.bake(extra_context={
        "include_batch": True,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": False,
    })
    pyproject = (result.project_path / "pyproject.toml").read_text()
    assert "apache-airflow" in pyproject
    assert "confluent-kafka" not in pyproject
    assert "pyiceberg" not in pyproject
    assert "dbt-duckdb" not in pyproject
    assert 'requires-python = ">=3.12"' in pyproject
    assert "[build-system]" in pyproject


def test_precommit_includes_sqlfluff_only_when_dbt_enabled(cookies):
    with_dbt = cookies.bake(extra_context={"include_dbt": True})
    without_dbt = cookies.bake(extra_context={"include_dbt": False})

    with_dbt_config = (with_dbt.project_path / ".pre-commit-config.yaml").read_text()
    without_dbt_config = (without_dbt.project_path / ".pre-commit-config.yaml").read_text()

    assert "sqlfluff" in with_dbt_config
    assert "sqlfluff" not in without_dbt_config
    assert "ruff-pre-commit" in with_dbt_config
    assert "ruff-pre-commit" in without_dbt_config


def test_ci_includes_dbt_test_step_only_when_dbt_enabled(cookies):
    with_dbt = cookies.bake(extra_context={"include_dbt": True})
    without_dbt = cookies.bake(extra_context={"include_dbt": False})

    with_dbt_ci = (with_dbt.project_path / ".github" / "workflows" / "ci.yml").read_text()
    without_dbt_ci = (without_dbt.project_path / ".github" / "workflows" / "ci.yml").read_text()

    assert "dbt test" in with_dbt_ci
    assert "dbt test" not in without_dbt_ci
    assert "ruff check" in with_dbt_ci
    assert "pytest" in with_dbt_ci


def test_duckdb_scoped_to_lakehouse_pyiceberg_removed(cookies):
    with_lakehouse = cookies.bake(extra_context={"include_lakehouse": True})
    without_lakehouse = cookies.bake(extra_context={
        "include_lakehouse": False,
        "include_dbt": True,
    })
    with_pyproject = (with_lakehouse.project_path / "pyproject.toml").read_text()
    without_pyproject = (without_lakehouse.project_path / "pyproject.toml").read_text()

    assert '"duckdb>=1.0"' in with_pyproject
    assert '"duckdb>=1.0"' not in without_pyproject
    assert '"dbt-duckdb>=1.8"' in without_pyproject
    assert "pyiceberg" not in with_pyproject
    assert "pyiceberg" not in without_pyproject
