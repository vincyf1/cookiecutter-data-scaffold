"""Append more customers and transactions to the Supabase demo source."""

import argparse
import os
from pathlib import Path

import psycopg

from schema_therapy.source_seed import append_synthetic_data


def source_url() -> str:
    url = os.environ.get("SUPABASE_DB_URL")
    if url:
        return url
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("SUPABASE_DB_URL="):
                return line.split("=", 1)[1]
    raise RuntimeError("SUPABASE_DB_URL is missing; run scripts/setup_supabase_db_url.sh")


def main() -> None:
    parser = argparse.ArgumentParser(description="Append synthetic Supabase source rows")
    parser.add_argument("--customers", type=int, default=10)
    parser.add_argument("--transactions", type=int, default=100)
    args = parser.parse_args()

    with psycopg.connect(source_url(), sslmode="require") as conn:
        customers_added, transactions_added = append_synthetic_data(
            conn, args.customers, args.transactions
        )
    print(f"Added {customers_added} customers and {transactions_added} transactions")


if __name__ == "__main__":
    main()
