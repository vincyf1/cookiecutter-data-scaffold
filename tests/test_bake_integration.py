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
