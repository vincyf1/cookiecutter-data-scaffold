from datetime import UTC, datetime
from decimal import Decimal

import duckdb
import pytest

from schema_therapy.lakehouse import writer


def test_source_refresh_replaces_rows_and_customer_segments(tmp_path):
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

    assert con.execute("SELECT customer_id, customer_segment FROM raw_customers").fetchall() == [
        (1, "gold")
    ]
    assert con.execute("SELECT transaction_id, amount_aud FROM raw_transactions").fetchall() == [
        (10, Decimal("20.00"))
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
