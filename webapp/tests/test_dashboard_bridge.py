"""Verify the dashboard reads and aggregates the DuckDB mart directly."""

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))
from dashboard_bridge import dashboard_snapshot  # noqa: E402


@pytest.mark.parametrize("column", ["customer_segment", "customer_type"])
def test_dashboard_period_and_schema_name(tmp_path: Path, column: str) -> None:
    db_path = tmp_path / "lakehouse.duckdb"
    with duckdb.connect(str(db_path)) as db:
        db.execute(
            f"""CREATE TABLE customer_revenue_daily (
                customer_id INTEGER, revenue_date DATE, {column} TEXT,
                region TEXT, completed_transaction_count INTEGER, revenue_aud DECIMAL(12, 2)
            )"""
        )
        db.execute(
            "INSERT INTO customer_revenue_daily VALUES "
            "(1, '2026-09-01', 'bronze', 'NSW', 1, 10.00), "
            "(1, '2026-09-09', 'bronze', 'NSW', 2, 20.00), "
            "(2, '2026-09-10', 'gold', 'VIC', 1, 30.00)"
        )

    recent = dashboard_snapshot(db_path, "7")
    assert recent["customerTypeColumn"] == column
    assert recent["summary"] == {
        "revenueAud": 50.0,
        "completedTransactions": 3,
        "purchasingCustomers": 2,
        "customerDays": 2,
    }
    assert [day["date"] for day in recent["daily"]] == ["2026-09-09", "2026-09-10"]
    assert sum(group["revenueAud"] for group in recent["customerTypes"]) == 50.0
    assert sum(group["revenueAud"] for group in recent["regions"]) == 50.0

    all_dates = dashboard_snapshot(db_path, "all")
    assert all_dates["summary"]["revenueAud"] == 60.0
    assert all_dates["summary"]["purchasingCustomers"] == 2


def test_dashboard_waits_for_mart(tmp_path: Path) -> None:
    assert dashboard_snapshot(tmp_path / "missing.duckdb", "30")["available"] is False
