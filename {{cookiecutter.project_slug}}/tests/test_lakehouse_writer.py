import pytest

from {{ cookiecutter.project_slug }}.lakehouse import writer
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
    # "name" alone is a substring of the offending "customer_name" field, so
    # it would pass even if the declared-columns list were dropped from the
    # message entirely. Assert columns that can only come from the DDL side.
    assert "id" in message
    assert "ingested_at" in message


def test_write_events_accepts_a_conforming_record(tmp_path):
    con = get_connection(str(tmp_path / "lakehouse.duckdb"))
    write_events(con, [{"id": 1, "name": "example"}])
    assert con.execute("SELECT count(*) FROM events").fetchone()[0] == 1


def test_declared_columns_ignores_a_table_level_constraint(tmp_path, monkeypatch):
    ddl = tmp_path / "create_tables.sql"
    ddl.write_text(
        "CREATE TABLE IF NOT EXISTS events (\n"
        "    id BIGINT,\n"
        "    name VARCHAR,\n"
        "    PRIMARY KEY (id)\n"
        ");\n"
    )
    monkeypatch.setattr(writer, "DDL_PATH", ddl)
    assert writer.declared_columns() == ["id", "name"]


def test_declared_columns_raises_loudly_on_an_unparseable_fragment(tmp_path, monkeypatch):
    ddl = tmp_path / "create_tables.sql"
    ddl.write_text("CREATE TABLE IF NOT EXISTS events (\n    ???not_a_column???\n);\n")
    monkeypatch.setattr(writer, "DDL_PATH", ddl)
    with pytest.raises(ValueError, match="cannot parse a column definition"):
        writer.declared_columns()
