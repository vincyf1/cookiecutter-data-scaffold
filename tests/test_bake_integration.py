import pytest

from conftest import run_cmd


def test_batch_sink_uses_lakehouse_writer_when_both_enabled(cookies):
    result = cookies.bake(extra_context={"include_batch": True, "include_lakehouse": True})
    slug = result.context["project_slug"]
    sinks = (result.project_path / "src" / slug / "batch" / "sinks.py").read_text()
    assert "lakehouse.writer" in sinks
    assert "pyarrow" not in sinks


def test_batch_sink_uses_parquet_when_lakehouse_disabled(cookies):
    result = cookies.bake(extra_context={"include_batch": True, "include_lakehouse": False})
    slug = result.context["project_slug"]
    sinks = (result.project_path / "src" / slug / "batch" / "sinks.py").read_text()
    assert "lakehouse.writer" not in sinks
    assert "pyarrow" in sinks


def test_streaming_sink_uses_lakehouse_writer_when_both_enabled(cookies):
    result = cookies.bake(extra_context={"include_streaming": True, "include_lakehouse": True})
    slug = result.context["project_slug"]
    sinks = (result.project_path / "src" / slug / "streaming" / "sinks.py").read_text()
    assert "lakehouse.writer" in sinks


def test_dbt_sources_reference_lakehouse_when_both_enabled(cookies):
    result = cookies.bake(extra_context={"include_dbt": True, "include_lakehouse": True})
    staging_dir = result.project_path / "transformation" / "models" / "staging"
    assert (staging_dir / "sources.yml").is_file()
    sources = (staging_dir / "sources.yml").read_text()
    assert "name: lakehouse" in sources
    assert "name: events" in sources


def test_dbt_sources_absent_when_lakehouse_disabled(cookies):
    result = cookies.bake(extra_context={"include_dbt": True, "include_lakehouse": False})
    staging_dir = result.project_path / "transformation" / "models" / "staging"
    assert not (staging_dir / "sources.yml").exists()


def test_docker_compose_has_airflow_service_only_when_batch_enabled(cookies):
    with_batch = cookies.bake(extra_context={"include_batch": True, "include_streaming": False})
    without_batch = cookies.bake(extra_context={"include_batch": False, "include_streaming": False})

    with_batch_compose = (with_batch.project_path / "docker-compose.yml").read_text()
    without_batch_compose = (without_batch.project_path / "docker-compose.yml").read_text()

    assert "airflow-webserver" in with_batch_compose
    assert "airflow-webserver" not in without_batch_compose


def test_docker_compose_has_kafka_service_only_when_streaming_enabled(cookies):
    result = cookies.bake(extra_context={"include_streaming": True})
    compose = (result.project_path / "docker-compose.yml").read_text()
    assert "kafka:" in compose


def test_docker_compose_is_valid_yaml_mapping_when_no_services_enabled(cookies):
    import yaml

    result = cookies.bake(extra_context={"include_batch": False, "include_streaming": False})
    compose = yaml.safe_load((result.project_path / "docker-compose.yml").read_text())
    assert compose["services"] == {}


def test_full_bake_all_patterns_lints_clean(cookies):
    result = cookies.bake(extra_context={
        "include_batch": True,
        "include_streaming": True,
        "include_lakehouse": True,
        "include_dbt": True,
    })
    assert result.exit_code == 0

    run_cmd(result.project_path, "uvx", "ruff", "check", ".")


@pytest.mark.parametrize(
    "flag",
    ["include_batch", "include_streaming", "include_lakehouse", "include_dbt"],
)
def test_single_pattern_alone_lints_clean(cookies, flag):
    # For include_dbt, this only checks that the bake+prune succeeds and
    # ruff has nothing to complain about (the only Python file left is an
    # empty src/{slug}/__init__.py) — it is not a meaningful lint gate on
    # transformation/'s SQL/YAML content. Real dbt-content verification for
    # the dbt-only combination is test_dbt_only_bake_dbt_build_and_test_pass.
    extra_context = {
        "include_batch": False,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": False,
    }
    extra_context[flag] = True

    result = cookies.bake(extra_context=extra_context)
    assert result.exit_code == 0

    run_cmd(result.project_path, "uvx", "ruff", "check", ".")


def test_dbt_only_bake_ci_tolerates_no_tests_collected(cookies):
    result = cookies.bake(extra_context={
        "include_batch": False,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": True,
    })
    assert result.exit_code == 0
    ci = (result.project_path / ".github" / "workflows" / "ci.yml").read_text()
    assert "uv run pytest || [ $? -eq 5 ]" in ci


def test_full_bake_all_patterns_off_lints_clean(cookies):
    result = cookies.bake(extra_context={
        "include_batch": False,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": False,
    })
    assert result.exit_code == 0

    run_cmd(result.project_path, "uvx", "ruff", "check", ".")


@pytest.mark.slow
def test_streaming_only_bake_generated_tests_pass(cookies):
    result = cookies.bake(extra_context={
        "include_batch": False,
        "include_streaming": True,
        "include_lakehouse": False,
        "include_dbt": False,
    })
    assert result.exit_code == 0

    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")


@pytest.mark.slow
def test_lakehouse_only_bake_generated_tests_pass(cookies):
    result = cookies.bake(extra_context={
        "include_batch": False,
        "include_streaming": False,
        "include_lakehouse": True,
        "include_dbt": False,
    })
    assert result.exit_code == 0

    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")


@pytest.mark.slow
def test_batch_only_bake_generated_tests_pass(cookies):
    result = cookies.bake(extra_context={
        "include_batch": True,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": False,
    })
    assert result.exit_code == 0

    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")


@pytest.mark.slow
def test_dbt_only_bake_dbt_build_and_test_pass(cookies):
    result = cookies.bake(extra_context={
        "include_batch": False,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": True,
    })
    assert result.exit_code == 0

    run_cmd(result.project_path, "uv", "sync")

    transformation_dir = result.project_path / "transformation"
    run_cmd(transformation_dir, "uv", "run", "--project", str(result.project_path), "dbt", "deps")
    run_cmd(transformation_dir, "uv", "run", "--project", str(result.project_path), "dbt", "build")
    run_cmd(transformation_dir, "uv", "run", "--project", str(result.project_path), "dbt", "test")


@pytest.mark.slow
def test_dbt_and_lakehouse_both_enabled_dbt_build_and_test_pass(cookies):
    result = cookies.bake(extra_context={
        "include_batch": False,
        "include_streaming": False,
        "include_lakehouse": True,
        "include_dbt": True,
    })
    assert result.exit_code == 0

    run_cmd(result.project_path, "uv", "sync")

    transformation_dir = result.project_path / "transformation"
    run_cmd(transformation_dir, "uv", "run", "--project", str(result.project_path), "dbt", "deps")
    run_cmd(transformation_dir, "uv", "run", "--project", str(result.project_path), "dbt", "build")
    run_cmd(transformation_dir, "uv", "run", "--project", str(result.project_path), "dbt", "test")


@pytest.mark.parametrize(
    "include_batch,include_streaming",
    [(True, False), (False, True), (True, True), (False, False)],
)
def test_docker_compose_is_valid_yaml_for_all_batch_streaming_combinations(
    cookies, include_batch, include_streaming
):
    import yaml

    result = cookies.bake(extra_context={
        "include_batch": include_batch,
        "include_streaming": include_streaming,
    })
    compose = yaml.safe_load((result.project_path / "docker-compose.yml").read_text())
    assert isinstance(compose["services"], dict)
    assert ("airflow-webserver" in compose["services"]) == include_batch
    assert ("kafka" in compose["services"]) == include_streaming
