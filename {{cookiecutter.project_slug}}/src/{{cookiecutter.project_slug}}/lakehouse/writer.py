import re
from pathlib import Path

import duckdb

DDL_PATH = Path(__file__).parent / "create_tables.sql"

# create_tables.sql is the schema's only hand-authored source (the post-gen
# hook also inlines it into dbt's on-run-start). Deriving the validation
# from it means the runtime error and the eval's ground truth cannot drift.
_COLUMN_DEF_RE = re.compile(r"^(\w+)\s+\w+")

# Table-level constraints share the column list's comma-separated syntax
# (e.g. `PRIMARY KEY (id)`) but are not columns. Recognized starters are
# skipped rather than mistaken for a column name; anything else that
# doesn't look like `<name> <type>` is a parse failure we raise loudly on,
# rather than silently inventing a bogus column.
_CONSTRAINT_KEYWORDS = {"PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT", "KEY", "INDEX"}


class SchemaDriftError(Exception):
    """A record carries a field the events table does not declare."""


def _split_top_level(body: str) -> list[str]:
    """Split a DDL column list on commas that aren't nested inside parens.

    A plain line-split breaks on multi-token constructs like
    `ingested_at TIMESTAMP DEFAULT now()` only by luck (no comma inside the
    parens); this makes that safe by construction and also lets a
    parenthesized constraint like `PRIMARY KEY (id)` survive as one item.
    """
    items = []
    depth = 0
    current: list[str] = []
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            items.append("".join(current))
            current = []
        else:
            current.append(char)
    if current:
        items.append("".join(current))
    return [item.strip() for item in items if item.strip()]


def declared_columns() -> list[str]:
    # Assumes the table's closing paren is the last ")" in the file — true
    # of create_tables.sql today; a trailing comment containing parens
    # would break this.
    body = DDL_PATH.read_text().split("(", 1)[1].rsplit(")", 1)[0]
    columns = []
    for item in _split_top_level(body):
        first_word = item.split(None, 1)[0].upper()
        if first_word in _CONSTRAINT_KEYWORDS:
            continue
        match = _COLUMN_DEF_RE.match(item)
        if not match:
            raise ValueError(
                f"cannot parse a column definition from DDL fragment {item!r} "
                f"in {DDL_PATH}; declared_columns() only understands "
                "`<name> <type> ...` column lines and table-level "
                f"constraints starting with {sorted(_CONSTRAINT_KEYWORDS)}."
            )
        columns.append(match.group(1))
    return columns


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
