# Shared helpers for live-demo/setup.sh and live-demo/teardown.sh. Source, don't execute.

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="$DEMO_DIR/schema_therapy"
BASELINE_DIR="$DEMO_DIR/baseline/schema_therapy"
WEBAPP_DIR="$(cd "$DEMO_DIR/../webapp" && pwd)"
STATE_DIR="$DEMO_DIR/.demo"
WEBAPP_PID_FILE="$STATE_DIR/webapp.pid"
WEBAPP_LOG="$STATE_DIR/webapp.log"
PYTHON="$PROJECT_DIR/.venv/bin/python"

DAG_ID="customer_revenue_daily"
AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8080}"
SERVICE="airflow-standalone"

if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; RESET=$'\033[0m'
  GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RED=$'\033[31m'; BLUE=$'\033[34m'
else
  BOLD=""; DIM=""; RESET=""; GREEN=""; YELLOW=""; RED=""; BLUE=""
fi

section() { printf '\n%s%s▸ %s%s\n' "$BOLD" "$BLUE" "$1" "$RESET"; }
ok()      { printf '  %s✓%s %s\n' "$GREEN" "$RESET" "$1"; }
info()    { printf '  %s%s%s\n' "$DIM" "$1" "$RESET"; }
warn()    { printf '  %s⚠ %s%s\n' "$YELLOW" "$1" "$RESET"; }
die()     { printf '  %s✗ %s%s\n' "$RED" "$1" "$RESET" >&2; exit 1; }

# env_get FILE KEY prints KEY's value from a dotenv file without sourcing it.
env_get() {
  [[ -f "$1" ]] || return 0
  local line
  line=$(grep -E "^$2=" "$1" | tail -n1) || return 0
  line="${line#*=}"; line="${line%\"}"; line="${line#\"}"; line="${line%\'}"; line="${line#\'}"
  printf '%s' "$line"
}

compose() { docker compose --project-directory "$PROJECT_DIR" -f "$PROJECT_DIR/docker-compose.yml" "$@"; }

demo_state() { "$PYTHON" "$DEMO_DIR/scripts/demo_state.py" "$@"; }

# baseline_drift prints "changed <path>" or "extra <path>" for each downstream
# file that differs from baseline/. The baseline covers exactly what the webapp
# delegate may edit: the DAG, ingestion, writer/DDL, their tests, and dbt models.
baseline_drift() {
  local rel
  while IFS= read -r rel; do
    cmp -s "$BASELINE_DIR/$rel" "$PROJECT_DIR/$rel" || echo "changed $rel"
  done < <(cd "$BASELINE_DIR" && find . -type f ! -name .DS_Store | sed 's|^\./||' | sort)
  while IFS= read -r rel; do
    [[ -f "$BASELINE_DIR/$rel" ]] || echo "extra $rel"
  done < <(cd "$PROJECT_DIR" && find transformation/models -type f ! -name .DS_Store | sort)
}

# restore_baseline rewinds the agent's downstream repair.
restore_baseline() {
  local kind rel drift
  drift=$(baseline_drift)
  if [[ -z "$drift" ]]; then
    ok "downstream code already matches the baseline"
    return
  fi
  # Keep the previous run's repair: it is the rehearsed fallback if the live agent stalls.
  local backup="$STATE_DIR/last-repair"
  rm -rf "$backup"
  while read -r kind rel; do
    if [[ -f "$PROJECT_DIR/$rel" ]]; then
      mkdir -p "$(dirname "$backup/$rel")"
      cp "$PROJECT_DIR/$rel" "$backup/$rel"
    fi
    if [[ "$kind" == changed ]]; then
      mkdir -p "$(dirname "$PROJECT_DIR/$rel")"
      cp "$BASELINE_DIR/$rel" "$PROJECT_DIR/$rel"
      info "restored $rel"
    else
      rm "$PROJECT_DIR/$rel"
      info "removed $rel (not in baseline)"
    fi
  done <<<"$drift"
  ok "downstream code restored to the customer_segment baseline"
  info "previous versions saved in live-demo/.demo/last-repair/"
}

# clear_workspace drops the webapp's staged edits and last-deployment record,
# which describe the previous rehearsal's revision.
clear_workspace() {
  if [[ -f "$WEBAPP_DIR/.airflow-workspace/workspace.json" ]]; then
    rm "$WEBAPP_DIR/.airflow-workspace/workspace.json"
    ok "cleared staged agent edits (webapp/.airflow-workspace)"
  else
    ok "no staged agent edits"
  fi
}

airflow_token() {
  local password
  password=$(env_get "$PROJECT_DIR/.env" AIRFLOW_ADMIN_PASSWORD)
  jq -n --arg p "$password" '{username: "admin", password: $p}' |
    curl -fsS -X POST "$AIRFLOW_URL/auth/token" -H 'Content-Type: application/json' -d @- |
    jq -r '.access_token'
}

# airflow_api METHOD PATH [JSON_BODY] calls the Airflow 3 REST API with a fresh token.
airflow_api() {
  local args=(-fsS -X "$1" "$AIRFLOW_URL/api/v2$2" -H "Authorization: Bearer $AIRFLOW_TOKEN")
  [[ -n "${3:-}" ]] && args+=(-H 'Content-Type: application/json' -d "$3")
  curl "${args[@]}"
}

webapp_running() {
  [[ -f "$WEBAPP_PID_FILE" ]] && kill -0 "$(cat "$WEBAPP_PID_FILE")" 2>/dev/null
}

stop_webapp() {
  if webapp_running; then
    local pid; pid=$(cat "$WEBAPP_PID_FILE")
    # setup.sh starts the webapp in its own process group; signal the whole group
    # so vite and the node server stop with concurrently.
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    for _ in {1..20}; do kill -0 "$pid" 2>/dev/null || break; sleep 0.25; done
    kill -KILL -- "-$pid" 2>/dev/null || true
    ok "stopped the webapp (pid $pid)"
  else
    ok "webapp not running from setup.sh"
  fi
  rm -f "$WEBAPP_PID_FILE"
  local leftover
  leftover=$( (lsof -nP -t -iTCP:3001 -iTCP:5173 -sTCP:LISTEN 2>/dev/null || true) | sort -u | tr '\n' ' ')
  [[ -z "$leftover" ]] || warn "ports 3001/5173 are still held by pid(s) $leftover (started outside setup.sh)"
}
