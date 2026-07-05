from pathlib import Path

import duckdb

DDL_PATH = Path(__file__).parent / "create_tables.sql"


def get_connection(db_path: str = "lakehouse.duckdb") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
    con.execute(DDL_PATH.read_text())
    return con


def write_events(con: duckdb.DuckDBPyConnection, records: list[dict]) -> None:
    for record in records:
        con.execute(
            "INSERT INTO events (id, name) VALUES (?, ?)",
            [record["id"], record["name"]],
        )
