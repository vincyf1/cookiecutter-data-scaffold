def test_default_bake_succeeds(cookies):
    result = cookies.bake()
    assert result.exit_code == 0
    assert result.exception is None
    assert result.project_path.is_dir()
    assert (result.project_path / "README.md").is_file()


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
