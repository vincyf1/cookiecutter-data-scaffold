# Pipeline agent

This local Python agent uses the OpenAI Responses API with nine function tools.
It reads the existing Airflow container, dbt manifest, SQL models, DuckDB file,
Supabase migration files, and local Git history. It does not contact Supabase.

From the `schema_therapy` directory:

```sh
uv sync --extra agent
export OPENAI_API_KEY=...  # use your own key; do not commit it
export OPENAI_MODEL=...    # a Responses API model available to your account
uv run --extra agent python -m agent.run "What is the customer_revenue_daily DAG status?"
```

Examples:

```sh
uv run --extra agent python -m agent.run "Explain the lineage and metric definition of customer_revenue_daily."
uv run --extra agent python -m agent.run "Run dbt tests for customer_revenue_daily."
uv run --extra agent python -m agent.run --allow-rerun "Rerun transform_with_dbt in customer_revenue_daily."
```

| Tool | Behavior |
| --- | --- |
| `get_dag_status(dag_id)` | Latest and recent Airflow DAG runs. |
| `get_task_logs(dag_id, task_id)` | Latest task attempt, last 200 lines. |
| `get_recent_schema_changes(dataset)` | Migration SQL and current DuckDB columns. This checkout has no Git or schema snapshot history, so it cannot date live schema changes. |
| `get_lineage(model)` | Direct upstream and downstream nodes from dbt's manifest. |
| `get_recent_git_changes(repo)` | Last ten commits when the local demo directory is a Git repository; otherwise reports that no repository exists. |
| `read_dbt_model(model)` | SQL source for a named dbt model. |
| `propose_patch(model)` | Returns a unified diff based on the current request; does not apply it. |
| `run_dbt_tests(model)` | Runs dbt tests for a named model against DuckDB. |
| `rerun_airflow_task(dag_id, task_id)` | Clears only that task in the latest DAG run for scheduler rerun; requires `--allow-rerun`. |

Start Airflow with `docker compose up -d` before using its tools. Run `dbt parse`
from `transformation/` if `target/manifest.json` is missing. The Git tool
reports `not_a_git_repository` in the current checkout. The agent has no
general shell or file write tool; patch proposals require human review.
