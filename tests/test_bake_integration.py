import pytest
from conftest import bake_with, lint_project, run_cmd


def test_shared_sink_uses_lakehouse_writer_when_enabled(cookies):
    result = bake_with(cookies, include_lakehouse=True)
    slug = result.context["project_slug"]
    sinks = (result.project_path / "src" / slug / "sinks.py").read_text()
    assert "lakehouse.writer" in sinks
    assert "pyarrow" not in sinks


def test_shared_sink_uses_parquet_when_lakehouse_disabled(cookies):
    result = bake_with(cookies, include_lakehouse=False)
    slug = result.context["project_slug"]
    sinks = (result.project_path / "src" / slug / "sinks.py").read_text()
    assert "lakehouse.writer" not in sinks
    assert "pyarrow" in sinks


def test_shared_sink_exposes_one_interface_in_each_storage_variant(cookies):
    for lakehouse in (True, False):
        result = bake_with(cookies, include_lakehouse=lakehouse)
        slug = result.context["project_slug"]
        sinks = (result.project_path / "src" / slug / "sinks.py").read_text()
        assert "class EventSink(Protocol)" in sinks
        assert "def create_event_sink(destination: Path) -> EventSink" in sinks


def test_shared_sink_removed_when_batch_and_streaming_both_disabled(cookies):
    result = bake_with(cookies, include_batch=False, include_streaming=False, include_agentic=False)
    slug = result.context["project_slug"]
    assert not (result.project_path / "src" / slug / "sinks.py").exists()


def test_dbt_sources_reference_lakehouse_when_both_enabled(cookies):
    result = bake_with(cookies, include_dbt=True, include_lakehouse=True)
    staging_dir = result.project_path / "transformation" / "models" / "staging"
    assert (staging_dir / "sources.yml").is_file()
    sources = (staging_dir / "sources.yml").read_text()
    assert "name: lakehouse" in sources
    assert "name: events" in sources


def test_dbt_sources_absent_when_lakehouse_disabled(cookies):
    result = bake_with(cookies, include_dbt=True, include_lakehouse=False)
    staging_dir = result.project_path / "transformation" / "models" / "staging"
    assert not (staging_dir / "sources.yml").exists()


def test_docker_compose_has_airflow_service_only_when_batch_enabled(cookies):
    with_batch = bake_with(cookies, include_batch=True, include_streaming=False)
    without_batch = bake_with(
        cookies, include_batch=False, include_streaming=False, include_agentic=False
    )

    with_batch_compose = (with_batch.project_path / "docker-compose.yml").read_text()
    without_batch_compose = (without_batch.project_path / "docker-compose.yml").read_text()

    assert "airflow-standalone" in with_batch_compose
    assert "airflow-standalone" not in without_batch_compose


def test_docker_compose_has_kafka_service_only_when_streaming_enabled(cookies):
    result = bake_with(cookies, include_streaming=True)
    compose = (result.project_path / "docker-compose.yml").read_text()
    assert "kafka:" in compose


def test_docker_compose_is_valid_yaml_mapping_when_no_services_enabled(cookies):
    import yaml

    result = bake_with(cookies, include_batch=False, include_streaming=False, include_agentic=False)
    compose = yaml.safe_load((result.project_path / "docker-compose.yml").read_text())
    assert compose["services"] == {}


def test_full_bake_all_patterns_lints_clean(cookies):
    result = bake_with(cookies)
    assert result.exit_code == 0

    lint_project(result.project_path)


@pytest.mark.parametrize(
    "flag",
    ["include_batch", "include_streaming", "include_lakehouse", "include_dbt", "include_agentic"],
)
def test_single_pattern_alone_lints_clean(cookies, flag):
    # For include_dbt, this only checks that the bake+prune succeeds and
    # ruff has nothing to complain about (the only Python file left is an
    # empty src/{slug}/__init__.py) — it is not a meaningful lint gate on
    # transformation/'s SQL/YAML content. Real dbt-content verification for
    # the dbt-only combination is test_dbt_only_bake_dbt_build_and_test_pass.
    # include_agentic cannot stand alone: pre_gen_project.py rejects it
    # without include_batch, so that case is agentic+batch.
    all_off = {
        "include_batch": False,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": False,
        "include_agentic": False,
    }
    all_off[flag] = True
    if flag == "include_agentic":
        all_off["include_batch"] = True

    result = bake_with(cookies, **all_off)
    assert result.exit_code == 0

    lint_project(result.project_path)


def test_dbt_only_bake_ci_tolerates_no_tests_collected(cookies):
    result = bake_with(
        cookies,
        include_batch=False,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=True,
        include_agentic=False,
    )
    assert result.exit_code == 0
    ci = (result.project_path / ".github" / "workflows" / "ci.yml").read_text()
    assert "uv run pytest || [ $? -eq 5 ]" in ci


def test_full_bake_all_patterns_off_lints_clean(cookies):
    result = bake_with(
        cookies,
        include_batch=False,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=False,
        include_agentic=False,
    )
    assert result.exit_code == 0

    lint_project(result.project_path)


@pytest.mark.slow
def test_streaming_only_bake_generated_tests_pass(cookies):
    result = bake_with(
        cookies,
        include_batch=False,
        include_streaming=True,
        include_lakehouse=False,
        include_dbt=False,
        include_agentic=False,
    )
    assert result.exit_code == 0

    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")


@pytest.mark.slow
def test_lakehouse_only_bake_generated_tests_pass(cookies):
    result = bake_with(
        cookies,
        include_batch=False,
        include_streaming=False,
        include_lakehouse=True,
        include_dbt=False,
        include_agentic=False,
    )
    assert result.exit_code == 0

    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")


@pytest.mark.slow
def test_batch_only_bake_generated_tests_pass(cookies):
    result = bake_with(
        cookies,
        include_batch=True,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=False,
    )
    assert result.exit_code == 0

    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")


@pytest.mark.slow
def test_dbt_only_bake_dbt_build_and_test_pass(cookies):
    result = bake_with(
        cookies,
        include_batch=False,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=True,
        include_agentic=False,
    )
    assert result.exit_code == 0

    run_cmd(result.project_path, "uv", "sync")

    transformation_dir = result.project_path / "transformation"
    run_cmd(transformation_dir, "uv", "run", "--project", str(result.project_path), "dbt", "deps")
    run_cmd(transformation_dir, "uv", "run", "--project", str(result.project_path), "dbt", "build")
    run_cmd(transformation_dir, "uv", "run", "--project", str(result.project_path), "dbt", "test")


@pytest.mark.slow
def test_dbt_and_lakehouse_both_enabled_dbt_build_and_test_pass(cookies):
    result = bake_with(
        cookies,
        include_batch=False,
        include_streaming=False,
        include_lakehouse=True,
        include_dbt=True,
        include_agentic=False,
    )
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

    result = bake_with(
        cookies,
        include_batch=include_batch,
        include_streaming=include_streaming,
        include_agentic=include_batch,
    )
    compose = yaml.safe_load((result.project_path / "docker-compose.yml").read_text())
    assert isinstance(compose["services"], dict)
    assert ("airflow-standalone" in compose["services"]) == include_batch
    assert ("kafka" in compose["services"]) == include_streaming


@pytest.mark.parametrize(
    "include_lakehouse,include_agentic",
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_docker_compose_pip_requirements_for_all_lakehouse_agentic_combinations(
    cookies, include_lakehouse, include_agentic
):
    # _PIP_ADDITIONAL_REQUIREMENTS is a single quoted YAML string built from
    # two independent inline `{% if %}` fragments (lakehouse's, agentic's).
    # yaml.safe_load succeeding only proves the line is well-formed YAML --
    # "pyarrow>=17.0duckdb>=1.0" parses perfectly and is still a broken
    # requirements string. Assert the exact rendered value instead, so a
    # missing separator or a stray leading/trailing space fails here.
    import yaml

    # include_agentic=True requires include_batch=True.
    result = bake_with(
        cookies,
        include_batch=True,
        include_lakehouse=include_lakehouse,
        include_agentic=include_agentic,
    )
    compose = yaml.safe_load((result.project_path / "docker-compose.yml").read_text())
    env = compose["services"]["airflow-standalone"]["environment"]

    expected = "pyarrow>=17.0"
    if include_lakehouse:
        expected += " duckdb>=1.0"
    if include_agentic:
        expected += " anthropic>=0.40 pydantic>=2.7"

    assert env["_PIP_ADDITIONAL_REQUIREMENTS"] == expected
    assert ("ANTHROPIC_API_KEY" in env) == include_agentic


def test_agentic_bake_lints_clean(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    assert result.exit_code == 0
    # Pinned ruff, but pinned *forward* -- see RUFF_VERSION in conftest.py.
    # A pin below what the generated project resolves lets this pass while
    # real bakes fail.
    lint_project(result.project_path)


@pytest.mark.slow
def test_default_bake_generated_tests_pass(cookies):
    result = bake_with(cookies)
    assert result.exit_code == 0
    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")
