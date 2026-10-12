# Live demo

The talk's demo is `schema_therapy/` (Supabase → Airflow → DuckDB → dbt) driven
from `../webapp`. Follow `../webapp/DEMO_SCRIPT.md` on stage.

```sh
./setup.sh             # before the talk or a rehearsal: reset, start, prove a green run
./setup.sh --check     # read-only preflight and state report
./teardown.sh          # after the talk: stop the webapp and Airflow, keep the outcome
./teardown.sh --reset  # also rewind code, DuckDB and Supabase to the baseline
```

`setup.sh` is safe to rerun from any state. It restores the downstream code,
clears the webapp's staged agent edits, renames `customer_type` back to
`customer_segment` in DuckDB and Supabase when needed, regenerates the dbt
manifest, starts Airflow, reserializes the DAG and waits for a successful
baseline run, then starts the webapp in the background (log in `.demo/`).
The only step left to the presenter is running the rename migration live.

| Path | Purpose |
| --- | --- |
| `baseline/schema_therapy/` | Pre-demo (`customer_segment`) copies of every file the webapp delegate may edit. The reset point; edit these if the baseline pipeline changes. |
| `rehearsed-repair/schema_therapy/` | The agent's verified 28 Sep repair, the fallback if the live agent stalls. |
| `.demo/last-repair/` | Whatever the most recent reset overwrote. |
| `scripts/` | Shared shell helpers and `demo_state.py` (Supabase/DuckDB status and reset). |
