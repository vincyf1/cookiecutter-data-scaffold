"""Verify bounded, read-only live source inspection."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))
from supabase_source import inspect_source  # noqa: E402


class FakeConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.last = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, query: str, _params=None):
        self.last = query
        self.queries.append(query)
        return self

    def fetchall(self):
        if "information_schema.columns" in self.last:
            return [("customer_id", "bigint", "NO"), ("customer_type", "text", "NO")]
        if "GROUP BY customer_type" in self.last:
            return [("bronze", 3), ("gold", 1)]
        raise AssertionError(f"Unexpected query: {self.last}")

    def fetchone(self):
        if "count(*) FROM demo_source.customers" in self.last:
            return (4,)
        raise AssertionError(f"Unexpected query: {self.last}")


def test_live_source_summary_detects_renamed_column_without_writes() -> None:
    conn = FakeConnection()
    with patch("supabase_source.source_url", return_value="postgresql://example"), patch(
        "supabase_source.psycopg.connect", return_value=conn
    ):
        result = inspect_source("customers", include_stats=True)

    assert result["source"] == "supabase_postgres_live"
    assert result["category_column"] == "customer_type"
    assert result["row_count"] == 4
    assert result["categories"] == [
        {"value": "bronze", "count": 3}, {"value": "gold", "count": 1}
    ]
    assert conn.queries[0] == "SET TRANSACTION READ ONLY"
    assert all(query.startswith("SELECT") or query.startswith("SET TRANSACTION READ ONLY") for query in conn.queries)


def test_live_source_rejects_other_tables() -> None:
    with pytest.raises(ValueError, match="demo customers and transactions"):
        inspect_source("users")
