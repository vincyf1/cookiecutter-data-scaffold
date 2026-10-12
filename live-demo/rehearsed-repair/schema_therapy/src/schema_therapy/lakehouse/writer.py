import os
from pathlib import Path

import duckdb

DDL_PATH = Path(__file__).parent / "create_tables.sql"


def get_connection(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    path = Path(db_path or os.environ.get("LAKEHOUSE_DB_PATH", "data/lakehouse.duckdb"))
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(DDL_PATH.read_text())
    migrate_raw_customers_schema(con)
    return con


def migrate_raw_customers_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Upgrade the historical customer snapshot without discarding its rows."""
    columns = {
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = 'raw_customers'"
        ).fetchall()
    }
    if "customer_segment" in columns and "customer_type" not in columns:
        con.begin()
        try:
            con.execute(
                "ALTER TABLE raw_customers RENAME COLUMN customer_segment TO customer_type"
            )
            con.commit()
        except Exception:
            con.rollback()
            raise


def write_events(con: duckdb.DuckDBPyConnection, records: list[dict]) -> None:
    for record in records:
        con.execute(
            "INSERT INTO events (id, name) VALUES (?, ?)",
            [record["id"], record["name"]],
        )


def replace_source_tables(
    con: duckdb.DuckDBPyConnection,
    customers: list[tuple],
    transactions: list[tuple],
) -> None:
    """Replace both raw tables as one snapshot, preserving the old rows on failure."""
    con.begin()
    try:
        con.execute("DELETE FROM raw_transactions")
        con.execute("DELETE FROM raw_customers")
        if customers:
            con.executemany(
                """INSERT INTO raw_customers
                   (customer_id, full_name, email, region, signed_up_at, customer_type)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                customers,
            )
        if transactions:
            con.executemany(
                """INSERT INTO raw_transactions
                   (transaction_id, customer_id, occurred_at, amount_aud, status)
                   VALUES (?, ?, ?, ?, ?)""",
                transactions,
            )
        con.commit()
    except Exception:
        con.rollback()
        raise
