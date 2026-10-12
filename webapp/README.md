# Schema Therapy Agentic Data Platform

A local live-demo dashboard for the `schema_therapy` pipeline. The right pane keeps the agent, Airflow task states, voice controls, and activity stream visible. The left pane begins empty and displays evidence as agent tools return results. A clearly labeled rehearsal preview is available when you want to show the incident layout without live services.

Investigation results offer **Readable** and **JSON** views, with syntax colours,
line numbers, and 18px monospace text for projection. Airflow log envelopes are
decoded into separate entries; warnings and errors have distinct highlights.
Long lines scroll horizontally by default. Use **Wrap**, **A− / A+**, and **Copy**
to adjust the view or copy formatted content. The JSON view expands embedded
log payloads for display. Long tool arguments are collapsed until opened.

The left pane also has a **Revenue dashboard** tab. It reads the local
`data/lakehouse.duckdb` `customer_revenue_daily` mart directly through a
read-only API endpoint, showing completed revenue over time, transactions,
distinct purchasing customers, and revenue by customer type and region. The
7, 30, and 90 day filters count back from the latest date in the mart; **All**
shows its full history. Refresh the tab after a successful DAG run to see the
newly built data. During the schema-change demo, the dashboard labels the
customer field according to the column currently present in DuckDB.
The API uses `LAKEHOUSE_DB_PATH` when set; otherwise it reads
`live-demo/schema_therapy/data/lakehouse.duckdb`. It never writes to the file.

## Run

1. From `webapp/`, run `npm install`.
2. Copy `.env.example` to `.env` and fill in the credentials you need. Keep `.env` local.
3. Start the schema therapy Airflow container from `../live-demo/schema_therapy/` with `docker compose up -d`.
4. Run `npm run dev` from `webapp/` and open `http://localhost:5173`.

The app uses Vite for the React frontend and a local Express API on `127.0.0.1:3001`. `npm run build` creates a production frontend in `dist/`; `npm run server` serves it and the API together. Run `npm test` to check voice delegation behavior.

## Connections

| Service | Configuration | Use |
| --- | --- | --- |
| Airflow 3 REST API | `AIRFLOW_API_URL`, plus `AIRFLOW_USERNAME` and `AIRFLOW_PASSWORD` or `AIRFLOW_TOKEN` | Latest DAG run, task states, task logs |
| dbt CLI and manifest | Existing `../live-demo/schema_therapy/.venv` and `transformation/target/manifest.json` | Lineage, model SQL, schema history, tests, patch proposal |
| OpenAI GPT-Live API | `OPENAI_API_KEY`, optional `OPENAI_LIVE_MODEL`, `OPENAI_VOICE` | Browser microphone and speaker audio over WebRTC |
| OpenAI delegated worker | `OPENAI_API_KEY` | `gpt-5.6-terra` with `reasoning.effort: medium` investigates via the Responses API and returns tool evidence |
| Live Supabase inspection | `SUPABASE_DB_URL` in `live-demo/schema_therapy/.env` (or the webapp process environment) | Read-only schema and summary tools for `demo_source.customers` and `demo_source.transactions` |
| Supabase Realtime Broadcast | `SUPABASE_URL`, `SUPABASE_PUBLISHABLE_KEY`, `SUPABASE_BROADCAST_TOPIC` | Live event notifications and status refresh |

The server requests an Airflow JWT from `/auth/token` and calls `/api/v2`. If your Airflow auth manager does not permit token creation without credentials, set its username and password or provide an existing token. The OpenAI key never goes to the browser. The browser posts its SDP offer to the local server, which creates the GPT-Live session.

The local Airflow Compose stack seeds a stable `admin` password from `AIRFLOW_ADMIN_PASSWORD` in `../live-demo/schema_therapy/.env`. Put that same value in this directory's gitignored `.env` as `AIRFLOW_PASSWORD` (with `AIRFLOW_USERNAME=admin`). Recreate Airflow after changing it and restart this server so it reloads `.env`.

The voice model defaults to `gpt-live-1` and uses the Live sessions API. `OPENAI_LIVE_MODEL` selects a GPT-Live variant; the older `OPENAI_REALTIME_MODEL` setting is accepted as a migration fallback. The delegated investigation model remains `gpt-5.6-terra`.

The three badges at the top right report Airflow API reachability, Supabase channel subscription, and OpenAI model availability. OpenAI changes to **ACTIVE** while voice is connected. **UNSET** means the corresponding local credential has not been supplied.

The existing Python `agent.tools.DemoTools` implementation supplies dbt and schema tools. Install its optional agent dependency for patch proposals: `cd ../live-demo/schema_therapy && uv sync --extra agent`. The data platform's `OPENAI_MODEL` controls patch proposals separately from `OPENAI_LIVE_MODEL`. A patch is displayed for review and is never applied automatically.

## Broadcast event

Subscribe on the topic named by `SUPABASE_BROADCAST_TOPIC` and send event `platform-event`. A simple payload is:

```json
{ "message": "transform_with_dbt failed", "type": "task_failed" }
```

The dashboard appends the message to the activity stream and refreshes Airflow status. To display external evidence, include `tool_name`, `args`, and `result` in the payload. The app does not create a database trigger or publish customer data. Configure broadcast authorization in Supabase for the channel you use.

## Demo controls

- **Start voice session** asks for microphone permission and connects GPT-Live over WebRTC. The agent listens during the presentation and distinguishes narration for the audience from requests directed to it. No wake word is required. GPT-Live decides when a turn needs the `gpt-5.6-terra` investigator and delegates it.
- The delegated worker can inspect the last five Airflow runs, current DAG and task status, task logs, and the live Supabase source schema and safe table summaries. Supabase queries use the configured Postgres connection in a read-only transaction; they do not expose customer names or email addresses to the model.
- **Inspect pipeline** fetches the latest Airflow DAG status without voice.
- **Preview incident** inserts one labeled rehearsal log result. It does not claim to be a live incident.
- **Airflow recovery tools:** the voice delegate can call `trigger_airflow_dag` to start a fresh run from ingestion, or `rerun_airflow_task` to clear the failed task and its downstream tasks in the latest failed run. Use a fresh run after downstream schema migration. Ask “Run the full DAG to validate the downstream fix” or “Retry the failed ingestion task.” Active runs block repeat requests; status and logs verify completion.
- **Rerun failed task** targets the actual failed task through the same Airflow REST action and refreshes status.
- **Downstream repair tools:** `list_airflow_files`, `read_airflow_file`, and `edit_airflow_file` inspect and stage edits to the customer revenue DAG, ingestion, DuckDB writer/DDL, dbt model SQL/YAML, and relevant tests. Edits use the file SHA256 and one exact text replacement; their diffs appear in Investigation. Source migrations, seed code, credentials, and Docker configuration are excluded.
- **Deployment tools:** `validate_airflow_deployment` checks an isolated copy inside the Airflow image (Python syntax, DAG import/task dependencies, dbt parse). `deploy_airflow_changes` revalidates the supplied workspace revision, publishes through existing Docker bind mounts, verifies mounted hashes, and runs `airflow dags reserialize` for the local bundle. It refuses active runs and stale file contents and restores live files after a failed deployment. `get_airflow_deployment_status` checks hashes and serialized DAG metadata. Validation does not run database migrations or establish recovery; start a fresh DAG and check logs/tests afterward.
- **Source changes** remain exclusively presenter-controlled. Supabase tools are read-only. The delegate is instructed to preserve the source contract and implement migrations downstream, including idempotent migration of existing DuckDB tables when needed.

`discard_airflow_changes` drops staged edits for a specified revision without changing live files; use it before restaging if a presenter edit makes the patch stale. Test-file changes publish locally because tests are not mounted into Docker.

Staged changes and the last deployment record persist locally in `.airflow-workspace/` (ignored by Git). Docker must already be running the configured `airflow-standalone` service; deployment uses its mounts and does not rebuild images, restart containers, or install dependencies.

Example voice request: “Keep the source as customer_type. Fix the downstream ingestion and dbt contract, deploy it, and run the full DAG to validate. Talk us through the evidence.”

Text entered in the question field is treated as a direct request. While voice is off, it can inspect status, logs, lineage, or schema evidence. With voice on, it sends the request to Terra and passes the verified answer to GPT-Live for speech.

The browser groups transcript fragments after a brief quiet interval. The speaker pauses briefly while the presenter talks; GPT-Live is instructed to avoid backchannels and wait through audience narration. Test representative narration, direct requests, and short follow-ups before presenting live.

## Keeping the room engaged during investigations

`POST /api/delegate` streams NDJSON: a `step` line before each Terra tool call, an `evidence` line after it, then one `result` or `error` line. Evidence appears in Investigation as each tool returns, not only at the end. Each step becomes one sentence describing what is being checked and why it matters (`src/progress.ts`), and the voice session forwards it to GPT-Live:

- **Spoken** (`session.commentary.append`) when the room has been quiet for 4 seconds, with neither the presenter nor GPT-Live speaking.
- **Silent context** (`session.thinking.append`) otherwise, so later answers still know the progress.
- After **9 seconds of silence** with work still running, GPT-Live gets a `session.instructions.append` asking for one short, varied line to the audience. It never states a result. A delegation gets at most six of these.

The verified result ends the updates. Nothing is spoken over the presenter: any presenter speech restarts the silence clock. Typed requests made while voice is on get the same treatment. Tune the timings at the top of `src/voice.ts`.

## Voice connection recovery

Voice stays active until you stop it. A brief WebRTC interruption gets a five-second grace period; a failed peer or closed data channel starts automatic reconnection with up to five attempts. Replacement GPT-Live sessions receive bounded recent text history and current backend task state. The app keeps the microphone stream, buffers backend results while reconnecting, and routes old-session results as session-wide updates; it does not restart the delegated workflow. Recent history is kept in memory and is lost on page reload.

The activity stream records the session ID, connection age, API error codes, and `session.closed` reason/usage when available. If the channel closes before that event, the cause is reported as unconfirmed. Intentional closure, microphone loss, or content termination ends voice; exhausted retries restore the Start voice control. Reload the page after updating the voice code and start a new session.

Speaker playback pauses when presenter speech arrives and resumes after a short quiet gap. There is no fixed 15-second playback mute timer. Startup failures and connection loss are separate from backend errors; backend work can continue while voice reconnects. This follows [OpenAI's session recovery guidance](https://developers.openai.com/api/docs/guides/live-conversations).

## Source references

- [OpenAI WebRTC connection guide](https://developers.openai.com/api/docs/guides/voice-webrtc)
- [GPT-Live delegation](https://developers.openai.com/api/docs/guides/live-delegation)
- [GPT-Live session events](https://developers.openai.com/api/docs/guides/live-conversations)
- [GPT-5.6 Terra model](https://developers.openai.com/api/docs/models/gpt-5.6-terra)
- [Airflow REST API](https://airflow.apache.org/docs/apache-airflow/3.2.2/stable-rest-api-ref.html)
- [Supabase Broadcast](https://supabase.com/docs/guides/realtime/broadcast)
