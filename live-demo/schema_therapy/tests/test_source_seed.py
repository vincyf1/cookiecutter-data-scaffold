from datetime import UTC, datetime
from decimal import Decimal
from importlib import import_module

import pytest


def test_new_batch_uses_fresh_ids_and_links_transactions_to_new_customers():
    source_seed = import_module("schema_therapy.source_seed")
    created_at = datetime(2026, 9, 27, 2, 0, tzinfo=UTC)

    customers, transactions = source_seed.build_rows(
        last_customer_id=208,
        last_transaction_id=4000,
        existing_customer_ids=[1, 2],
        customer_count=3,
        transaction_count=5,
        created_at=created_at,
    )

    assert [(row[0], row[5]) for row in customers] == [
        (209, "bronze"),
        (210, "gold"),
        (211, "silver"),
    ]
    assert [row[0] for row in transactions] == [4001, 4002, 4003, 4004, 4005]
    assert [row[1] for row in transactions] == [209, 210, 211, 1, 2]
    assert all(row[2] == created_at and row[3] >= Decimal("5.00") for row in transactions)
    assert all(row[4] in {"completed", "failed"} for row in transactions)


def test_transactions_require_at_least_one_customer():
    source_seed = import_module("schema_therapy.source_seed")

    with pytest.raises(ValueError, match="at least one customer"):
        source_seed.build_rows(
            last_customer_id=0,
            last_transaction_id=0,
            existing_customer_ids=[],
            customer_count=0,
            transaction_count=1,
            created_at=datetime(2026, 9, 27, tzinfo=UTC),
        )
