"""Append synthetic source records for repeatable live demos."""

from datetime import UTC, datetime
from decimal import Decimal

import psycopg

REGIONS = ("NSW", "VIC", "QLD", "WA")


def build_rows(
    last_customer_id: int,
    last_transaction_id: int,
    existing_customer_ids: list[int],
    customer_count: int,
    transaction_count: int,
    created_at: datetime,
) -> tuple[list[tuple], list[tuple]]:
    """Build new rows with IDs above the current source maximums."""
    if customer_count < 0 or transaction_count < 0:
        raise ValueError("Row counts must be nonnegative")
    if customer_count == 0 and transaction_count == 0:
        raise ValueError("Request at least one customer or transaction")

    customer_ids = list(range(last_customer_id + 1, last_customer_id + customer_count + 1))
    eligible_customer_ids = customer_ids + existing_customer_ids
    if transaction_count and not eligible_customer_ids:
        raise ValueError("Transactions require at least one customer")

    customers = []
    for customer_id in customer_ids:
        segment = (
            "gold" if customer_id % 10 == 0
            else "silver" if customer_id % 10 in (1, 2, 3)
            else "bronze"
        )
        customers.append(
            (
                customer_id,
                f"Demo Customer {customer_id:03d}",
                f"customer{customer_id:03d}@example.test",
                REGIONS[(customer_id - 1) % len(REGIONS)],
                created_at,
                segment,
            )
        )

    transactions = []
    for index in range(transaction_count):
        transaction_id = last_transaction_id + index + 1
        amount_cents = 500 + (transaction_id * 97) % 49500
        transactions.append(
            (
                transaction_id,
                eligible_customer_ids[index % len(eligible_customer_ids)],
                created_at,
                Decimal(amount_cents) / 100,
                "failed" if transaction_id % 11 == 0 else "completed",
            )
        )
    return customers, transactions


def append_synthetic_data(
    conn: psycopg.Connection, customer_count: int, transaction_count: int
) -> tuple[int, int]:
    """Append one batch atomically; concurrent runs of this script serialize."""
    with conn.transaction():
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext('schema_therapy'), hashtext('demo_source_seed'))"
        )
        last_customer_id = conn.execute(
            "SELECT COALESCE(MAX(customer_id), 0) FROM demo_source.customers"
        ).fetchone()[0]
        last_transaction_id = conn.execute(
            "SELECT COALESCE(MAX(transaction_id), 0) FROM demo_source.transactions"
        ).fetchone()[0]
        existing_customer_ids = [
            row[0]
            for row in conn.execute(
                "SELECT customer_id FROM demo_source.customers ORDER BY customer_id"
            ).fetchall()
        ]
        customers, transactions = build_rows(
            last_customer_id,
            last_transaction_id,
            existing_customer_ids,
            customer_count,
            transaction_count,
            datetime.now(UTC),
        )
        with conn.cursor() as cursor:
            if customers:
                cursor.executemany(
                    """INSERT INTO demo_source.customers
                       (customer_id, full_name, email, region, signed_up_at, customer_segment)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    customers,
                )
            if transactions:
                cursor.executemany(
                    """INSERT INTO demo_source.transactions
                       (transaction_id, customer_id, occurred_at, amount_aud, status)
                       VALUES (%s, %s, %s, %s, %s)""",
                    transactions,
                )
    return len(customers), len(transactions)
