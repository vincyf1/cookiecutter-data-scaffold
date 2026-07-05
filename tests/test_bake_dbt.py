def test_dbt_project_present_when_enabled(cookies):
    result = cookies.bake(extra_context={"include_dbt": True, "include_lakehouse": False})
    project = result.project_path
    assert (project / "transformation" / "profiles.yml").is_file()
    assert (project / "transformation" / "models" / "staging" / "stg_events.sql").is_file()
    schema = (project / "transformation" / "models" / "staging" / "schema.yml").read_text()
    assert "not_null" in schema
    assert "unique" in schema
