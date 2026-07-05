def test_dbt_project_present_when_enabled(cookies):
    result = cookies.bake(extra_context={"include_dbt": True, "include_lakehouse": False})
    project = result.project_path
    assert (project / "transformation" / "profiles.yml").is_file()
    assert (project / "transformation" / "models" / "staging" / "stg_events.sql").is_file()
    schema = (project / "transformation" / "models" / "staging" / "schema.yml").read_text()
    assert "not_null" in schema
    assert "unique" in schema
    stg_events = (project / "transformation" / "models" / "staging" / "stg_events.sql").read_text()
    assert "{{ ref('events_seed') }}" in stg_events


def test_sqlfluff_config_present_only_when_dbt_enabled(cookies):
    with_dbt = cookies.bake(extra_context={"include_dbt": True})
    without_dbt = cookies.bake(extra_context={"include_dbt": False})

    assert (with_dbt.project_path / ".sqlfluff").is_file()
    assert not (without_dbt.project_path / ".sqlfluff").exists()

    config = (with_dbt.project_path / ".sqlfluff").read_text()
    assert "dialect = duckdb" in config
    assert "templater = dbt" in config
    assert "project_dir = transformation" in config
