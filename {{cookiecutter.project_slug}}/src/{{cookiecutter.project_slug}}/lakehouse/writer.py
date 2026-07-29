import re
from pathlib import Path

import duckdb

DDL_PATH = Path(__file__).parent / "create_tables.sql"

# create_tables.sql is the schema's only hand-authored source (the post-gen
# hook also inlines it into dbt's on-run-start). Deriving the validation
# from it means the runtime error and the eval's ground truth cannot drift.
_COLUMN_RE = re.compile(r"^\s*(\w+)\s+\w+", re.MULTILINE)


class SchemaDriftError(Exception):
    """A record carries a field the events table does not declare."""


def declared_columns() -> list[str]:
    body = DDL_PATH.read_text().split("(", 1)[1].rsplit(")", 1)[0]
    return [m.group(1) for m in _COLUMN_RE.finditer(body)]


def get_connection(db_path: str = "lakehouse.duckdb") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
    con.execute(DDL_PATH.read_text())
    return con


def validate(records: list[dict]) -> None:
    columns = declared_columns()
    for record in records:
        unexpected = [key for key in record if key not in columns]
        if unexpected:
            raise SchemaDriftError(
                f"record carries undeclared field(s) {unexpected}; "
                f"the events table declares columns {columns}. "
                "Either the producer renamed a field or the DDL is out of date."
            )


def write_events(con: duckdb.DuckDBPyConnection, records: list[dict]) -> None:
    validate(records)
    for record in records:
        con.execute(
            "INSERT INTO events (id, name) VALUES (?, ?)",
            [record["id"], record["name"]],
        )
