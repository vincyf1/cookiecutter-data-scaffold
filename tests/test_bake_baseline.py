def test_default_bake_succeeds(cookies):
    result = cookies.bake()
    assert result.exit_code == 0
    assert result.exception is None
    assert result.project_path.is_dir()
    assert (result.project_path / "README.md").is_file()
