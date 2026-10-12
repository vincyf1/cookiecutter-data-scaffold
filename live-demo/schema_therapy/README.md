# schema_therapy

Data engineering live demo. The Supabase Postgres source for the planned
`customer_revenue_daily` DAG is defined in [`supabase/migrations/20260927000000_create_demo_source.sql`](supabase/migrations/20260927000000_create_demo_source.sql).

## Set up the Supabase source

Project: `https://vfrioamroonmasjrzthx.supabase.co`

1. Open the project's **SQL Editor** in the Supabase Dashboard.
2. Run `supabase/migrations/20260927000000_create_demo_source.sql`.
3. Run `supabase/migrations/20260927000001_add_customer_segment.sql`.
4. Run `supabase/seed.sql`.
5. Run `supabase/verify.sql`. On an empty schema, expect 200 customers,
   4,000 transactions, zero orphan or pre-signup transactions, and 120
   transaction dates. Customer segments should be bronze 120, silver 60,
   and gold 20.

The seed can be run again without duplicating rows. All names and addresses
are synthetic (`example.test`). Customers have signup times preceding their
transactions. The segment tier is synthetic and stable for each customer ID;
rerunning the seed also fills it for customers seeded before the column was
added. Transactions have positive AUD amounts and either `completed`
or `failed` status; only completed transactions contribute to revenue.
The schema is `demo_source`, separate from Supabase's public API schema.

For the live schema-change demo, first confirm a successful baseline Airflow
run. When the demo begins, run
`supabase/migrations/20260927000002_rename_customer_segment_to_customer_type.sql`
in the Supabase SQL Editor, then trigger a new `customer_revenue_daily` run.
The migration keeps the existing values and constraints while changing the
source contract. Ingestion should fail until the downstream pipeline adopts
`customer_type`. Run this migration only once; the initial seed and
`supabase/verify.sql` still use `customer_segment` and are for pre-demo setup.

For a direct database connection, get the Postgres connection string from the
Supabase Dashboard's **Connect** panel. Keep credentials out of this repository.
The source tables for the later DAG are `demo_source.customers` and
`demo_source.transactions`; `supabase/verify.sql` contains a sample daily
revenue aggregation.

## Add more synthetic source data

Run this command from the project directory whenever you want more rows:

```sh
uv run python scripts/seed_more.py
```

Each run adds 10 customers and 100 transactions by default. Choose counts with
`--customers` and `--transactions`, for example:

```sh
uv run python scripts/seed_more.py --customers 5 --transactions 50
```

The script reads `SUPABASE_DB_URL` from the environment or the local `.env`
file. It allocates new IDs above the current maximums and commits customers
and transactions together. New transactions are linked to valid customers,
including the newly created ones. It does not rerun the original
`supabase/seed.sql`, which is intended only for the initial 200/4,000 rows.
The daily Airflow DAG will ingest the added rows on its next run; trigger it in
the Airflow UI to refresh DuckDB immediately.

## Ingest into DuckDB with Airflow

From this directory, run the setup wizard. It opens the Supabase project and
asks you to copy the **Session pooler** URI template from **Connect**, then
enter the database password in a hidden prompt:

```sh
./scripts/setup_supabase_db_url.sh
```

The wizard percent-encodes the password, writes `SUPABASE_DB_URL` to the
gitignored `.env` file, and offers to start or recreate Airflow. If you skip
that step, run `docker compose up -d --force-recreate` yourself. Do not paste
the password into the repository or chat.

For the local Airflow admin login, set `AIRFLOW_ADMIN_PASSWORD` in this
directory's gitignored `.env` file. The Compose startup script writes that
value to Airflow 3's Simple Auth Manager password file before `standalone`
starts, so recreating the container keeps the same `admin` password. Set
`AIRFLOW_USERNAME=admin` and the same value as `AIRFLOW_PASSWORD` in
`../../webapp/.env`, then recreate Airflow and restart the webapp server.

The `customer_revenue_daily` DAG runs daily and can also be triggered in the
Airflow UI at `http://localhost:8080`. Its `ingest_supabase` task reads both
`demo_source` tables over SSL and replaces `raw_customers` and
`raw_transactions` in one DuckDB transaction. `transform_with_dbt` then builds
and tests the `customer_revenue_daily` table and the daily time spine. A retry
does not duplicate source rows, and a failed replacement leaves the previous
snapshot intact. The DuckDB file is `data/lakehouse.duckdb`; the dbt profile
points to the same file. Credentials stay in the environment rather than in
the repository.

## Revenue semantics for agents

The dbt semantic model in
`transformation/models/marts/customer_revenue_daily.yml` is the source of truth
for metric names, dimensions, and definitions. Its grain is one customer per
UTC day with a completed transaction. `completed_revenue_aud` sums completed
sales in AUD; `completed_transactions` counts those sales; and
`purchasing_customers` counts distinct customers in the queried period. Failed
transactions are excluded. Refunds, taxes, and currency conversion are not
modeled. Days with no completed sales have no row in the mart. Customer segment
and region are the values at the latest dbt build.

`transformation/models/marts/saved_queries.yml` defines a reusable daily
revenue query by segment. `dbt parse` writes the machine-readable
`transformation/target/semantic_manifest.json` for tools that consume dbt
metadata. The dbt models and YAML remain authoritative when the manifest is
regenerated.

To explore the metrics through MetricFlow locally, install the optional
semantic dependency and run the saved query from the dbt project directory:

```sh
uv sync --extra semantic
cd transformation
../.venv/bin/mf list metrics
../.venv/bin/mf query --saved-query daily_revenue_by_segment --limit 5
```

Agents can use the semantic manifest to discover the metric definitions and
the MetricFlow CLI to query DuckDB using those definitions. This setup runs
locally and does not provision a hosted semantic service.

## Pipeline agent

The [`agent/`](agent/) directory contains an OpenAI SDK script with tools for
Airflow status and logs, schema changes, dbt lineage and models, Git changes,
patch proposals, dbt tests, and a selected task rerun. See
[`agent/README.md`](agent/README.md) for setup and commands.
