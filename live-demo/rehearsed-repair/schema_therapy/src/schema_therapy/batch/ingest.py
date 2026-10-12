"""Copy the Supabase source snapshot into DuckDB."""

import logging
import os

import psycopg

from schema_therapy.lakehouse.writer import get_connection, replace_source_tables

LOGGER = logging.getLogger(__name__)


def read_source_tables(source_url: str, sslmode: str = "require") -> tuple[list[tuple], list[tuple]]:
    """Read customers and transactions from one consistent Postgres snapshot."""
    with psycopg.connect(source_url, sslmode=sslmode) as source:
        source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        customers = source.execute(
            """SELECT customer_id, full_name, email, region, signed_up_at, customer_type
               FROM demo_source.customers ORDER BY customer_id"""
        ).fetchall()
        transactions = source.execute(
            """SELECT transaction_id, customer_id, occurred_at, amount_aud, status
               FROM demo_source.transactions ORDER BY transaction_id"""
        ).fetchall()
    return customers, transactions


def ingest_supabase_to_duckdb() -> None:
    source_url = os.environ.get("SUPABASE_DB_URL")
    if not source_url:
        raise RuntimeError("Set SUPABASE_DB_URL to the Supabase Postgres connection string")

    customers, transactions = read_source_tables(source_url)
    target = get_connection()
    try:
        replace_source_tables(target, customers, transactions)
    finally:
        target.close()
    LOGGER.info("Loaded %d customers and %d transactions", len(customers), len(transactions))
