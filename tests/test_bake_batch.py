def test_batch_dag_present_when_enabled(cookies):
    result = cookies.bake(extra_context={"include_batch": True})
    slug = result.context["project_slug"]
    dag_path = result.project_path / "src" / slug / "batch" / "dags" / "example_dag.py"
    assert dag_path.is_file()
    assert "events" in dag_path.read_text()
