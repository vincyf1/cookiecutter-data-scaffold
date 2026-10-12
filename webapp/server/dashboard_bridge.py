"""Read dashboard aggregates directly from the local DuckDB revenue mart."""

import json
import os
import sys
from datetime import timedelta
from pathlib import Path

import duckdb

PROJECT = Path(__file__).resolve().parents[2] / "live-demo/schema_therapy"
PERIODS = {"7": 7, "30": 30, "90": 90, "all": None}


def dashboard_snapshot(db_path: Path, period: str) -> dict:
    if period not in PERIODS:
        raise ValueError("Period must be 7, 30, 90, or all")
    if not db_path.exists():
        return {"available": False, "message": "DuckDB has not been created. Run the customer_revenue_daily DAG first."}

    with duckdb.connect(str(db_path), read_only=True) as db:
        tables = {row[0] for row in db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()}
        if "customer_revenue_daily" not in tables:
            return {"available": False, "message": "The revenue mart is not built yet. Run the customer_revenue_daily DAG first."}

        columns = {row[0] for row in db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = 'customer_revenue_daily'"
        ).fetchall()}
        customer_type_column = next(
            (name for name in ("customer_type", "customer_segment") if name in columns), None
        )
        latest = db.execute("SELECT max(revenue_date) FROM customer_revenue_daily").fetchone()[0]
        if latest is None:
            return {"available": False, "message": "The revenue mart has no rows yet."}

        days = PERIODS[period]
        cutoff = latest - timedelta(days=days - 1) if days else None
        where = "WHERE revenue_date >= ?" if cutoff else ""
        params = [cutoff] if cutoff else []

        total = db.execute(
            f"""SELECT coalesce(sum(revenue_aud), 0),
                       coalesce(sum(completed_transaction_count), 0),
                       count(DISTINCT customer_id), count(*), min(revenue_date)
                FROM customer_revenue_daily {where}""",
            params,
        ).fetchone()
        daily = db.execute(
            f"""SELECT revenue_date, sum(revenue_aud),
                       sum(completed_transaction_count), count(DISTINCT customer_id)
                FROM customer_revenue_daily {where}
                GROUP BY revenue_date ORDER BY revenue_date""",
            params,
        ).fetchall()
        regions = db.execute(
            f"""SELECT region, sum(revenue_aud), count(DISTINCT customer_id)
                FROM customer_revenue_daily {where}
                GROUP BY region ORDER BY sum(revenue_aud) DESC""",
            params,
        ).fetchall()
        customer_types = (
            db.execute(
                f"""SELECT {customer_type_column}, sum(revenue_aud), count(DISTINCT customer_id)
                    FROM customer_revenue_daily {where}
                    GROUP BY {customer_type_column} ORDER BY sum(revenue_aud) DESC""",
                params,
            ).fetchall()
            if customer_type_column else []
        )

    return {
        "available": True,
        "period": period,
        "latestDate": latest.isoformat(),
        "firstDate": total[4].isoformat(),
        "customerTypeColumn": customer_type_column,
        "summary": {
            "revenueAud": float(total[0]),
            "completedTransactions": int(total[1]),
            "purchasingCustomers": int(total[2]),
            "customerDays": int(total[3]),
        },
        "daily": [
            {"date": date.isoformat(), "revenueAud": float(revenue),
             "completedTransactions": int(transactions), "purchasingCustomers": int(customers)}
            for date, revenue, transactions, customers in daily
        ],
        "regions": [
            {"name": name, "revenueAud": float(revenue), "purchasingCustomers": int(customers)}
            for name, revenue, customers in regions
        ],
        "customerTypes": [
            {"name": name, "revenueAud": float(revenue), "purchasingCustomers": int(customers)}
            for name, revenue, customers in customer_types
        ],
    }


def main() -> None:
    try:
        period = sys.argv[1] if len(sys.argv) > 1 else "30"
        db_path = Path(os.environ.get("LAKEHOUSE_DB_PATH", PROJECT / "data/lakehouse.duckdb"))
        if not db_path.is_absolute():
            db_path = PROJECT / db_path
        print(json.dumps(dashboard_snapshot(db_path, period)))
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
