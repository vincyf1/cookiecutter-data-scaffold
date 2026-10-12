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
  part of the archived source.
