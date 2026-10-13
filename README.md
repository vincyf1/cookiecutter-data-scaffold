# Data Engineering in the Agentic Era — live demo

This branch is an archive of the talk's live demo. It shares no history with
`main` and is never merged into it. The template itself lives on `main`.

The demo shows agents operating a small data platform: a voice host hands
requests to an investigator model that reads Airflow, dbt and the Supabase
source, stages downstream fixes, validates them inside the Airflow image,
deploys them, and reruns the DAG to prove recovery.

| Path | What it is |
| --- | --- |
| `live-demo/schema_therapy/` | The demo data platform (Supabase → Airflow → DuckDB → dbt), generated from this repository's template and then extended. `agent/` holds the original command-line agent. |
| `live-demo/baseline/`, `live-demo/rehearsed-repair/` | The pre-demo reset point and a rehearsed repair, used by `setup.sh` and `teardown.sh`. |
| `live-demo/setup.sh`, `live-demo/teardown.sh` | Reset, start, and stop the demo. See `live-demo/README.md`. |
| `webapp/` | The presenter dashboard: React UI, Express API, voice session, delegated investigator, and the staged-edit and deployment tools. See `webapp/README.md`. |

## Run the demo

### Prerequisites

- Docker Desktop, running
- `uv`, Node.js 22+ with `npm`, `curl`, `jq` and `lsof` (`setup.sh` checks for each)
- Your own Supabase project. The URL in `live-demo/schema_therapy/README.md`
  is the presenter's and will not accept your credentials.
- An OpenAI API key with access to the voice and investigator models named in
  `webapp/.env.example`

### One-time setup

1. **Create the source data.** In your Supabase project's SQL Editor, run
   these files from `live-demo/schema_therapy/supabase/` in order:
   - `migrations/20260927000000_create_demo_source.sql`
   - `migrations/20260927000001_add_customer_segment.sql`
   - `seed.sql`
   - `verify.sql`

   `verify.sql` should report 200 customers and 4,000 transactions.
   **Do not run** `…000002_rename_customer_segment_to_customer_type.sql` yet.
   That migration is the live failure.
2. **Configure the data platform.** Copy `live-demo/schema_therapy/.env.example`
   to `.env`. Run `live-demo/schema_therapy/scripts/setup_supabase_db_url.sh` to
   write `SUPABASE_DB_URL`. Then set `AIRFLOW_ADMIN_PASSWORD` and `OPENAI_API_KEY`.
3. **Configure the web app.** Copy `webapp/.env.example` to `webapp/.env`. Set:
   - `AIRFLOW_PASSWORD`, to the same value as `AIRFLOW_ADMIN_PASSWORD`
   - `OPENAI_API_KEY`
   - optionally `SUPABASE_URL` and `SUPABASE_PUBLISHABLE_KEY`, for the
     broadcast badge
4. Run `npm install` in `webapp/`.

### Before each run

```sh
cd live-demo
./setup.sh --check   # read-only: reports what is missing or out of place
./setup.sh           # reset to the baseline, start Airflow, prove a green run, start the web app
```

`setup.sh` is safe to rerun from any state. When it finishes, open
`http://localhost:5173`. The Airflow UI is at `http://localhost:8080` (user
`admin`).

### The demo

1. In the web app, select **Inspect pipeline** to show a green baseline.
2. In the Supabase SQL Editor, run
   `migrations/20260927000002_rename_customer_segment_to_customer_type.sql`.
   This is the upstream schema change.
3. Trigger `customer_revenue_daily` in Airflow. `ingest_supabase` fails.
4. Select **Start voice session**, or type into the question field, and ask the
   agent to investigate. For example:

   > Keep the source as customer_type. Fix the downstream ingestion and dbt
   > contract, deploy it, and run the full DAG to validate. Talk us through the
   > evidence.

   The agent then works through these stages, each showing up in
   **Investigation** as it happens:
   - reads the logs and the schema
   - stages edits
   - validates them inside the Airflow image
   - deploys
   - reruns the DAG
5. Open the **Revenue dashboard** tab to show the data flowing again.

If the live agent stalls, `live-demo/rehearsed-repair/` holds a verified
repair. Apply it by hand, then trigger a fresh run:

```sh
cp -R live-demo/rehearsed-repair/schema_therapy/. live-demo/schema_therapy/
cd live-demo/schema_therapy && docker compose exec airflow-standalone airflow dags reserialize
```

### Afterwards

```sh
./teardown.sh          # stop the web app and Airflow, keep the repaired state
./teardown.sh --reset  # also rewind code, DuckDB and the Supabase column to the baseline
```

### Security caveats

This is presenter tooling for one trusted laptop. Do not run it on a shared
machine or network. A review of this archive found:

- **The web app's local API has no authentication.** It listens on
  `127.0.0.1:3001` and can edit, deploy and run pipeline code. A malicious web
  page open in the same browser could reach it through DNS rebinding. Close
  other tabs while the web app is running, and stop it when you finish.
- **Deployment validation runs edited code** inside the Airflow image, where
  the runtime credentials (`SUPABASE_DB_URL`, the Airflow password) are
  visible. Treat any edit the agent stages as code that can read those
  secrets.
- Use a Supabase project and an OpenAI key created for the demo, never
  production ones.

## How it relates to the template

The template's `include_agentic` Pattern is the production-shaped counterpart.
There, the agent diagnoses a failure and proposes a patch for a person to
review; it never deploys. This demo deliberately goes further, editing,
deploying and rerunning on its own, because a stage demo needs to show a fix
land. Those steps are safe on a presenter's laptop and are not template
defaults.

## Not included

- The slide deck.
- Generated or local files: `node_modules/`, `.venv/`, `dist/`, DuckDB files,
  dbt `target/`, logs, and the demo's runtime state.
- Credentials. Copy `webapp/.env.example` and
  `live-demo/schema_therapy/.env.example` to `.env` and fill them in locally.
- `webapp/DEMO_SCRIPT.md`, which `live-demo/README.md` refers to; it was not
  part of the archived source. "The demo" above replaces it.
