#!/usr/bin/env bash
#
# Prepare the Schema Therapy live demo from any prior state (fresh machine,
# finished rehearsal, or a previous talk):
#
#   1. preflight: tools, Docker, credentials
#   2. reset: downstream code to the customer_segment baseline, staged agent
#      edits, the DuckDB raw table, and the Supabase source column
#   3. start Airflow, reserialize the DAG, and prove a green baseline run
#   4. start the webapp at http://localhost:5173
#
# Afterwards the stage is set for webapp/DEMO_SCRIPT.md: the presenter runs the
# rename migration in the Supabase SQL Editor and the next ingestion fails.
#
# Usage: ./setup.sh [--check] [--no-webapp] [--skip-run]
#   --check      preflight and read-only status only; change nothing
#   --no-webapp  don't start the webapp (run `npm run dev` in webapp/ yourself)
#   --skip-run   don't trigger the baseline DAG run

set -euo pipefail
source "$(dirname "$0")/scripts/lib.sh"

CHECK_ONLY=0; START_WEBAPP=1; BASELINE_RUN=1
for arg in "$@"; do
  case "$arg" in
    --check) CHECK_ONLY=1 ;;
    --no-webapp) START_WEBAPP=0 ;;
    --skip-run) BASELINE_RUN=0 ;;
    -h|--help) sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown option: $arg (see --help)" ;;
  esac
done

# ── 1. Preflight ──────────────────────────────────────────────────────────
section "Preflight"
for tool in docker uv node npm curl jq lsof; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool is not installed"
done
ok "docker, uv, node, npm, curl, jq available"

docker info >/dev/null 2>&1 || die "Docker is not running; start Docker Desktop and retry"
ok "Docker is running"

if [[ ! -x "$PYTHON" ]]; then
  (( CHECK_ONLY )) && die "schema_therapy/.venv is missing; run ./setup.sh to create it"
  info "creating schema_therapy/.venv"
  (cd "$PROJECT_DIR" && uv sync --extra agent --quiet)
fi
ok "schema_therapy virtualenv ready"

[[ -n "$(env_get "$PROJECT_DIR/.env" SUPABASE_DB_URL)" ]] ||
  die "SUPABASE_DB_URL missing in schema_therapy/.env; run schema_therapy/scripts/setup_supabase_db_url.sh"
ADMIN_PASSWORD=$(env_get "$PROJECT_DIR/.env" AIRFLOW_ADMIN_PASSWORD)
[[ -n "$ADMIN_PASSWORD" ]] || die "AIRFLOW_ADMIN_PASSWORD missing in schema_therapy/.env"
ok "schema_therapy/.env has SUPABASE_DB_URL and AIRFLOW_ADMIN_PASSWORD"

[[ -f "$WEBAPP_DIR/.env" ]] || die "webapp/.env missing; copy webapp/.env.example and fill it in"
[[ "$(env_get "$WEBAPP_DIR/.env" AIRFLOW_PASSWORD)" == "$ADMIN_PASSWORD" ]] ||
  die "webapp/.env AIRFLOW_PASSWORD must equal schema_therapy/.env AIRFLOW_ADMIN_PASSWORD"
[[ -n "$(env_get "$WEBAPP_DIR/.env" OPENAI_API_KEY)" ]] ||
  warn "webapp/.env OPENAI_API_KEY is empty; voice and the delegate will be unavailable"
ok "webapp/.env matches the Airflow admin password"

section "Current state"
demo_state status
if (( CHECK_ONLY )); then
  drift=$(baseline_drift | wc -l | tr -d ' ')
  (( drift )) && info "downstream code: $drift file(s) differ from the baseline (setup restores them)" \
              || info "downstream code matches the baseline"
  compose ps --status running --services 2>/dev/null | grep -qx "$SERVICE" &&
    info "Airflow container running" || info "Airflow container stopped"
  webapp_running && info "webapp running (pid $(cat "$WEBAPP_PID_FILE"))" || info "webapp not running from setup.sh"
  printf '\n%sCheck only; nothing changed.%s\n' "$DIM" "$RESET"
  exit 0
fi

# ── 2. Reset to the pre-demo baseline ─────────────────────────────────────
section "Reset to baseline"
stop_webapp  # a server from an earlier run would keep serving stale workspace state
restore_baseline
clear_workspace
demo_state reset-lakehouse
demo_state reset-source

(cd "$PROJECT_DIR/transformation" && ../.venv/bin/dbt parse --profiles-dir . --quiet)
ok "dbt manifest regenerated for the agent's lineage tools"

# ── 3. Airflow and a green baseline run ───────────────────────────────────
section "Airflow"
compose up -d --quiet-pull
info "waiting for the Airflow API (first start installs packages, allow a few minutes)"
for i in $(seq 1 120); do
  curl -fsS "$AIRFLOW_URL/api/v2/monitor/health" 2>/dev/null |
    jq -e '.scheduler.status == "healthy"' >/dev/null 2>&1 && break
  (( i == 120 )) && die "Airflow did not become healthy; check: docker compose logs $SERVICE"
  sleep 5
done
ok "Airflow API healthy at $AIRFLOW_URL"

compose exec -T "$SERVICE" airflow dags reserialize >/dev/null 2>&1 ||
  die "DAG reserialize failed; check: docker compose exec $SERVICE airflow dags list-import-errors"
AIRFLOW_TOKEN=$(airflow_token) || die "Airflow rejected the admin login"
for i in $(seq 1 24); do
  airflow_api GET "/dags/$DAG_ID" >/dev/null 2>&1 && break
  (( i == 24 )) && die "$DAG_ID is not registered; check DAG import errors in the Airflow UI"
  sleep 5
done
airflow_api PATCH "/dags/$DAG_ID" '{"is_paused": false}' >/dev/null
ok "$DAG_ID reserialized from the baseline code and unpaused"

if (( BASELINE_RUN )); then
  # Wait out a scheduled run the unpause may have started, since max_active_runs=1.
  for _ in $(seq 1 60); do
    state=$(airflow_api GET "/dags/$DAG_ID/dagRuns?limit=1&order_by=-start_date" |
      jq -r '.dag_runs[0].state // "none"')
    [[ "$state" == queued || "$state" == running ]] || break
    sleep 5
  done
  RUN_ID=$(airflow_api POST "/dags/$DAG_ID/dagRuns" '{"logical_date": null, "conf": {}}' | jq -r '.dag_run_id')
  info "triggered baseline run $RUN_ID"
  for i in $(seq 1 120); do
    state=$(airflow_api GET "/dags/$DAG_ID/dagRuns/$(jq -rn --arg r "$RUN_ID" '$r|@uri')" | jq -r '.state')
    [[ "$state" == success || "$state" == failed ]] && break
    (( i % 12 == 0 )) && AIRFLOW_TOKEN=$(airflow_token)  # tokens are short-lived
    sleep 5
  done
  [[ "$state" == success ]] ||
    die "baseline run ended '$state'; open $AIRFLOW_URL and fix it before the audience arrives"
  ok "baseline run succeeded (ingest_supabase → transform_with_dbt)"
fi

# ── 4. Webapp ─────────────────────────────────────────────────────────────
if (( START_WEBAPP )); then
  section "Webapp"
  [[ -d "$WEBAPP_DIR/node_modules" ]] || (cd "$WEBAPP_DIR" && npm install --silent)
  if lsof -nP -t -iTCP:3001 -iTCP:5173 -sTCP:LISTEN >/dev/null 2>&1; then
    warn "ports 3001/5173 are already in use; restart that webapp so it reloads .env"
  else
    mkdir -p "$STATE_DIR"
    set -m  # own process group, so teardown can stop vite and the server together
    (cd "$WEBAPP_DIR" && exec nohup npm run dev >"$WEBAPP_LOG" 2>&1) &
    echo $! >"$WEBAPP_PID_FILE"
    set +m
    for i in $(seq 1 30); do
      curl -fsS http://127.0.0.1:5173 >/dev/null 2>&1 && break
      (( i == 30 )) && die "webapp did not start; see live-demo/.demo/webapp.log"
      sleep 1
    done
    ok "webapp at http://localhost:5173 (log: live-demo/.demo/webapp.log)"
  fi
fi

section "Ready"
demo_state status
cat <<EOF

  ${BOLD}Stage is set.${RESET} Before the audience arrives (webapp/DEMO_SCRIPT.md):
    • Test the microphone and start the voice session in the webapp.
    • Keep the Supabase SQL Editor open with
      schema_therapy/supabase/migrations/20260927000002_rename_customer_segment_to_customer_type.sql
  When the talk is over: ${BOLD}./teardown.sh${RESET}   (add --reset to rewind for another rehearsal)

EOF
