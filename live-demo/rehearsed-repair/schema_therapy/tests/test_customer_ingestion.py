from datetime import UTC, datetime
from decimal import Decimal

import duckdb
import pytest

from schema_therapy.lakehouse import writer


def test_source_refresh_replaces_rows_and_customer_types(tmp_path):
    con = writer.get_connection(str(tmp_path / "revenue.duckdb"))
    created_at = datetime(2026, 9, 1, tzinfo=UTC)

    writer.replace_source_tables(
        con,
        customers=[(1, "Demo Customer 001", "customer001@example.test", "NSW", created_at, "bronze")],
        transactions=[(10, 1, created_at, Decimal("12.50"), "completed")],
    )
    writer.replace_source_tables(
        con,
        customers=[(1, "Demo Customer 001", "customer001@example.test", "NSW", created_at, "gold")],
        transactions=[(10, 1, created_at, Decimal("20.00"), "completed")],
    )

    assert con.execute("SELECT customer_id, customer_type FROM raw_customers").fetchall() == [
        (1, "gold")
    ]
    assert con.execute("SELECT transaction_id, amount_aud FROM raw_transactions").fetchall() == [
        (10, Decimal("20.00"))
    ]
    con.close()


def test_existing_customer_segment_snapshot_is_migrated_without_data_loss(tmp_path):
    db_path = tmp_path / "revenue.duckdb"
    con = writer.get_connection(str(db_path))
    con.execute("DROP TABLE raw_customers")
    con.execute(
        """CREATE TABLE raw_customers (
            customer_id BIGINT PRIMARY KEY,
            full_name VARCHAR NOT NULL,
            email VARCHAR NOT NULL,
            region VARCHAR NOT NULL,
            signed_up_at TIMESTAMPTZ NOT NULL,
            customer_segment VARCHAR NOT NULL
        )"""
    )
    con.execute(
        """INSERT INTO raw_customers VALUES
           (1, 'Demo Customer 001', 'customer001@example.test', 'NSW',
            TIMESTAMPTZ '2026-09-01 00:00:00+00', 'gold')"""
    )
    con.close()

    con = writer.get_connection(str(db_path))
    assert con.execute("SELECT customer_id, customer_type FROM raw_customers").fetchall() == [
        (1, "gold")
    ]
    con.close()


def test_failed_source_refresh_keeps_previous_snapshot(tmp_path):
    con = writer.get_connection(str(tmp_path / "revenue.duckdb"))
    created_at = datetime(2026, 9, 1, tzinfo=UTC)
    customer = (1, "Demo Customer 001", "customer001@example.test", "NSW", created_at, "bronze")
    transaction = (10, 1, created_at, Decimal("12.50"), "completed")
    writer.replace_source_tables(con, [customer], [transaction])

    with pytest.raises(duckdb.ConstraintException):
        writer.replace_source_tables(
            con,
            [customer],
            [transaction, transaction],
        )

    assert con.execute("SELECT transaction_id, customer_id FROM raw_transactions").fetchall() == [
        (10, 1)
    ]
    con.close()
