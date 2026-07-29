import pytest

from {{ cookiecutter.project_slug }}.lakehouse.writer import (
    SchemaDriftError,
    declared_columns,
    get_connection,
    write_events,
)


def test_write_events_inserts_rows(tmp_path):
    con = get_connection(str(tmp_path / "test.duckdb"))
    write_events(con, [{"id": 1, "name": "example"}])
    result = con.execute("SELECT id, name FROM events").fetchall()
    assert result == [(1, "example")]


def test_declared_columns_are_parsed_from_the_ddl():
    assert declared_columns() == ["id", "name", "ingested_at"]


def test_write_events_rejects_an_undeclared_field(tmp_path):
    con = get_connection(str(tmp_path / "lakehouse.duckdb"))

    with pytest.raises(SchemaDriftError) as excinfo:
        write_events(con, [{"id": 1, "customer_name": "example"}])

    message = str(excinfo.value)
    assert "customer_name" in message
    assert "name" in message


def test_write_events_accepts_a_conforming_record(tmp_path):
    con = get_connection(str(tmp_path / "lakehouse.duckdb"))
    write_events(con, [{"id": 1, "name": "example"}])
    assert con.execute("SELECT count(*) FROM events").fetchone()[0] == 1
