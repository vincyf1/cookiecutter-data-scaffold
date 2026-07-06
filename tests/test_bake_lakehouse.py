from conftest import bake_with


def test_lakehouse_writer_present_when_enabled(cookies):
    result = bake_with(cookies, include_lakehouse=True)
    slug = result.context["project_slug"]
    writer_path = result.project_path / "src" / slug / "lakehouse" / "writer.py"
    assert writer_path.is_file()
    assert "def write_events(" in writer_path.read_text()
