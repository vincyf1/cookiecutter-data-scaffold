import duckdb
import pytest

from {{ cookiecutter.project_slug }}.events import Event
from {{ cookiecutter.project_slug }}.lakehouse.writer import DuckDBEventSink


def test_duckdb_sink_inserts_one_batch_transactionally(tmp_path):
    database = tmp_path / "test.duckdb"
    with DuckDBEventSink(database) as sink:
        sink.write([Event(id=1, name="example"), Event(id=2, name="second")])
        rows = sink.connection.execute("SELECT id, name FROM events ORDER BY id").fetchall()
    assert rows == [(1, "example"), (2, "second")]


def test_duckdb_sink_closes_connection_on_context_exit(tmp_path):
    sink = DuckDBEventSink(tmp_path / "test.duckdb")
    with sink:
        pass
    with pytest.raises(duckdb.ConnectionException):
        sink.connection.execute("SELECT 1")


def test_duckdb_sink_rolls_back_whole_batch_on_database_failure(tmp_path):
    database = tmp_path / "test.duckdb"
    with DuckDBEventSink(database) as sink:
        sink.connection.execute("CREATE UNIQUE INDEX events_id ON events(id)")
        with pytest.raises(duckdb.ConstraintException):
            sink.write([Event(id=1, name="first"), Event(id=1, name="duplicate")])
        rows = sink.connection.execute("SELECT id, name FROM events").fetchall()
    assert rows == []


def test_empty_batch_is_a_noop(tmp_path):
    with DuckDBEventSink(tmp_path / "test.duckdb") as sink:
        sink.write([])
        assert sink.connection.execute("SELECT count(*) FROM events").fetchone() == (0,)


def test_failed_initialization_closes_connection(tmp_path, monkeypatch):
    connection = duckdb.connect(str(tmp_path / "test.duckdb"))
    monkeypatch.setattr(duckdb, "connect", lambda destination: connection)
    monkeypatch.setattr("{{ cookiecutter.project_slug }}.lakehouse.writer.DDL_PATH", tmp_path / "missing")
    with pytest.raises(FileNotFoundError), DuckDBEventSink(tmp_path / "test.duckdb"):
        pass
    with pytest.raises(duckdb.ConnectionException):
        connection.execute("SELECT 1")
