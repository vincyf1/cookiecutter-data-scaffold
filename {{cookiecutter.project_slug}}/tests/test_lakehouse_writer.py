from {{cookiecutter.project_slug}}.lakehouse.writer import get_connection, write_events


def test_write_events_inserts_rows(tmp_path):
    con = get_connection(str(tmp_path / "test.duckdb"))
    write_events(con, [{"id": 1, "name": "example"}])
    result = con.execute("SELECT id, name FROM events").fetchall()
    assert result == [(1, "example")]
