"""Read-only inspection of the live Supabase demo source."""

import os
from datetime import UTC, datetime
from pathlib import Path

import psycopg

PROJECT = Path(__file__).resolve().parents[2] / "live-demo/schema_therapy"
DATASETS = {"customers", "transactions"}


def source_url() -> str:
    configured = os.environ.get("SUPABASE_DB_URL")
    if configured:
        return configured
    env_path = PROJECT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("SUPABASE_DB_URL="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise RuntimeError("SUPABASE_DB_URL is not configured for live source inspection")


def inspect_source(dataset: str, include_stats: bool = False) -> dict:
    if dataset not in DATASETS:
        raise ValueError("Only the demo customers and transactions tables can be inspected")

    with psycopg.connect(source_url(), sslmode="require", connect_timeout=8) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        rows = conn.execute(
            """SELECT column_name, data_type, is_nullable
               FROM information_schema.columns
               WHERE table_schema = %s AND table_name = %s
               ORDER BY ordinal_position""",
            ("demo_source", dataset),
        ).fetchall()
        columns = [
            {"name": name, "type": data_type, "nullable": nullable == "YES"}
            for name, data_type, nullable in rows
        ]
        result = {
            "source": "supabase_postgres_live",
            "table": f"demo_source.{dataset}",
            "observed_at": datetime.now(UTC).isoformat(),
            "columns": columns,
        }
        if not include_stats:
            return result

        if dataset == "customers":
            result["row_count"] = conn.execute("SELECT count(*) FROM demo_source.customers").fetchone()[0]
            category = next(
                (name for name in ("customer_type", "customer_segment")
                 if any(column["name"] == name for column in columns)),
                None,
            )
            result["category_column"] = category
            result["categories"] = (
                [{"value": value, "count": count} for value, count in conn.execute(
                    f"SELECT {category}, count(*) FROM demo_source.customers "
                    f"GROUP BY {category} ORDER BY count(*) DESC"
                ).fetchall()]
                if category else []
            )
        else:
            result["row_count"] = conn.execute("SELECT count(*) FROM demo_source.transactions").fetchone()[0]
            result["statuses"] = [
                {"status": status, "count": count}
                for status, count in conn.execute(
                    "SELECT status, count(*) FROM demo_source.transactions "
                    "GROUP BY status ORDER BY count(*) DESC"
                ).fetchall()
            ]
        return result
