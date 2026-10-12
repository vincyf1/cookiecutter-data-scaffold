#!/usr/bin/env bash
#
# Shut down the Schema Therapy live demo.
#
# By default this stops the webapp and the Airflow container and leaves the
# demo's outcome in place: Supabase stays on customer_type and the downstream
# code keeps the agent's repair (see "After the demo" in webapp/DEMO_SCRIPT.md).
# Removing the container also discards its local Airflow run history.
#
# Usage: ./teardown.sh [--reset] [--keep-airflow]
#   --reset         also rewind everything to the pre-demo baseline: downstream
#                   code, staged agent edits, the DuckDB raw table, and the
#                   Supabase column (customer_type -> customer_segment)
#   --keep-airflow  leave the Airflow container running

set -euo pipefail
source "$(dirname "$0")/scripts/lib.sh"

RESET_STATE=0; STOP_AIRFLOW=1
for arg in "$@"; do
  case "$arg" in
    --reset) RESET_STATE=1 ;;
    --keep-airflow) STOP_AIRFLOW=0 ;;
    -h|--help) sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown option: $arg (see --help)" ;;
  esac
done

section "Webapp"
stop_webapp

section "Airflow"
if (( STOP_AIRFLOW )); then
  if docker info >/dev/null 2>&1; then
    compose down --remove-orphans >/dev/null 2>&1
    ok "Airflow container removed"
  else
    warn "Docker is not running; nothing to stop"
  fi
else
  ok "left Airflow running (--keep-airflow)"
fi

if (( RESET_STATE )); then
  section "Reset to baseline"
  [[ -x "$PYTHON" ]] || die "schema_therapy/.venv is missing; run ./setup.sh first"
  if (( ! STOP_AIRFLOW )); then
    state=$(AIRFLOW_TOKEN=$(airflow_token 2>/dev/null) &&
      airflow_api GET "/dags/$DAG_ID/dagRuns?limit=1&order_by=-start_date" 2>/dev/null |
      jq -r '.dag_runs[0].state // "none"') || state=unknown
    [[ "$state" == queued || "$state" == running ]] &&
      die "a $DAG_ID run is $state and may hold the DuckDB file; wait for it, then retry"
  fi
  restore_baseline
  clear_workspace
  demo_state reset-lakehouse
  demo_state reset-source
  (cd "$PROJECT_DIR/transformation" && ../.venv/bin/dbt parse --profiles-dir . --quiet)
  ok "dbt manifest regenerated"
  (( STOP_AIRFLOW )) || warn "Airflow still serves the old DAG bundle; run ./setup.sh to reserialize it"
fi

section "Done"
if (( RESET_STATE )); then
  info "Everything is back on customer_segment. Run ./setup.sh before the next rehearsal."
else
  info "Demo outcome left as-is (code, DuckDB, Supabase). Run ./setup.sh to rewind and start again."
fi
printf '\n'
