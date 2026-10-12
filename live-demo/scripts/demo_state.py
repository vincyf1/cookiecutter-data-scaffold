"""Inspect and reset the live demo's data state: the Supabase source and the DuckDB lakehouse.

Run with the schema_therapy virtualenv (it has psycopg and duckdb):

    schema_therapy/.venv/bin/python scripts/demo_state.py status
    schema_therapy/.venv/bin/python scripts/demo_state.py reset-source
    schema_therapy/.venv/bin/python scripts/demo_state.py reset-lakehouse

The baseline contract is `customer_segment`; the demo renames it to `customer_type`.
"""

import argparse
import os
import sys
from pathlib import Path

import duckdb
import psycopg

PROJECT = Path(__file__).resolve().parents[1] / "schema_therapy"
LAKEHOUSE = Path(os.environ.get("LAKEHOUSE_DB_PATH", PROJECT / "data/lakehouse.duckdb"))
BASELINE, RENAMED = "customer_segment", "customer_type"


def env_value(key: str) -> str:
    if os.environ.get(key):
        return os.environ[key]
    env_file = PROJECT / ".env"
    if env_file.exists():
        for line in reversed(env_file.read_text().splitlines()):
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip().strip("'\"")
    return ""


def source_url() -> str:
    url = env_value("SUPABASE_DB_URL")
    if not url:
        sys.exit("SUPABASE_DB_URL is not set; run schema_therapy/scripts/setup_supabase_db_url.sh")
    return url


def source_columns(con: psycopg.Connection) -> set[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'demo_source' AND table_name = 'customers'"
    ).fetchall()
    return {row[0] for row in rows}


def lakehouse_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'main' AND table_name = 'raw_customers'"
    ).fetchall()
    return {row[0] for row in rows}


def contract(columns: set[str]) -> str:
    if not columns:
        return "missing"
    if BASELINE in columns and RENAMED not in columns:
        return BASELINE
    if RENAMED in columns and BASELINE not in columns:
        return RENAMED
    return "ambiguous"


def status() -> None:
    with psycopg.connect(source_url(), sslmode="require", connect_timeout=10) as con:
        con.read_only = True
        columns = source_columns(con)
        customers = con.execute("SELECT count(*) FROM demo_source.customers").fetchone()[0]
        transactions = con.execute("SELECT count(*) FROM demo_source.transactions").fetchone()[0]
    print(f"supabase   demo_source.customers: {contract(columns)} "
          f"({customers} customers, {transactions} transactions)")

    if not LAKEHOUSE.exists():
        print(f"lakehouse  {LAKEHOUSE}: missing (the first DAG run creates it)")
        return
    con = duckdb.connect(str(LAKEHOUSE), read_only=True)
    try:
        print(f"lakehouse  raw_customers: {contract(lakehouse_columns(con))}")
    finally:
        con.close()


def reset_source() -> None:
    """Undo the presenter's rename so the next demo run fails on stage again."""
    with psycopg.connect(source_url(), sslmode="require", connect_timeout=10) as con:
        current = contract(source_columns(con))
        if current == BASELINE:
            print(f"supabase   already on {BASELINE}")
            return
        if current != RENAMED:
            sys.exit(f"supabase   customers contract is {current}; run the setup SQL in "
                     "schema_therapy/README.md before resetting")
        con.execute(f"ALTER TABLE demo_source.customers RENAME COLUMN {RENAMED} TO {BASELINE}")
        con.commit()
    print(f"supabase   renamed {RENAMED} -> {BASELINE}")


def reset_lakehouse() -> None:
    """Rename the raw column back in place; the baseline DAG run then rebuilds the dbt models."""
    if not LAKEHOUSE.exists():
        print("lakehouse  missing; the baseline DAG run will create it")
        return
    con = duckdb.connect(str(LAKEHOUSE))
    try:
        current = contract(lakehouse_columns(con))
        if current in (BASELINE, "missing"):
            print(f"lakehouse  raw_customers already {current}")
            return
        if current != RENAMED:
            sys.exit(f"lakehouse  raw_customers contract is {current}; inspect {LAKEHOUSE}")
        con.execute(f"ALTER TABLE raw_customers RENAME COLUMN {RENAMED} TO {BASELINE}")
    finally:
        con.close()
    print(f"lakehouse  renamed raw_customers.{RENAMED} -> {BASELINE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["status", "reset-source", "reset-lakehouse"])
    {"status": status, "reset-source": reset_source, "reset-lakehouse": reset_lakehouse}[
        parser.parse_args().command
    ]()
