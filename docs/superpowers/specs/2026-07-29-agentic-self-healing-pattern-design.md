# Agentic Self-Healing Pattern — Design

**Date:** 2026-07-29
**Status:** Approved, ready for implementation planning

## Summary

Add a fifth composable Pattern, `include_agentic`, to `cookiecutter-data-scaffold`. When a
batch DAG task fails, an in-DAG healing task runs a bounded AI agent that reads the failure
logs, diagnoses the root cause, and produces a patch as a reviewable artifact. The entire
agent run is captured as a JSON array and persisted. A four-tier eval suite — including an
LLM grader — scores agent behavior, with the deterministic tiers gating CI.

## Goals

1. A demo DAG with a reproducible, diagnosable failure.
2. An agent that reads the failing run's logs and repository source.
3. A patch surfaced for human review, without credentials in the Airflow worker.
4. End-to-end capture of the agent run as structured, queryable telemetry.
5. An eval scaffold with graded feedback, extensible by adding data files.
6. Deterministic behavior: `uv run pytest` passes offline, with no API key, every time.

## Non-goals

- Applying patches automatically. The agent proposes; humans merge.
- Opening real pull requests by default. `GitHubPublisher` ships but is opt-in.
- An OpenTelemetry exporter or a dbt model over traces. Both are additive later.
- Healing any DAG other than the scaffolded demo. Multi-DAG healing is a wiring change.

## Architecture

`include_agentic` scaffolds `src/<slug>/agentic/` with six modules:

| Module | Responsibility | Depends on |
|---|---|---|
| `contracts.py` | Typed records: `FailureContext`, `AgentResult`, `TraceElement`, `Trace`. No logic. | — |
| `runner.py` | `AgentRunner` protocol; `FakeRunner`, `ReplayRunner`, `ClaudeRunner` | `contracts` |
| `tools.py` | The three allowlisted agent tools | `contracts` |
| `tracing.py` | `TraceRecorder`, `TraceSink` protocol; `DuckDBSink`, `JsonlSink` | `contracts` |
| `publisher.py` | `PRPublisher` protocol; `LocalPatchPublisher`, `GitHubPublisher` | `contracts` |
| `dags/healing_dag.py` | Wires the above into Airflow. The only Airflow-aware module. | all |

### Data flow

```
example_events_dag (task fails, failure_mode=schema_drift)
        │  trigger_rule=ALL_FAILED
        ▼
heal task ──1──▶ build FailureContext from Airflow task context
          ──2──▶ AgentRunner.run(ctx) ──▶ tools: read_task_log / read_repo_file / propose_patch
          ──3──▶ AgentResult{diagnosis, edits, elements}
          ──4──▶ TraceSink.write(elements)   ← non-blocking; never fails the task
          ──5──▶ PRPublisher.publish(result) ← artifacts/<trace_id>.patch + metadata
```

### Design boundaries

Two seams carry the design:

**`AgentRunner`** isolates all non-determinism. Above it, every module is ordinary Python
testable with plain pytest. `FakeRunner` returns a fixture; `ReplayRunner` replays a recorded
HTTP transport; `ClaudeRunner` calls the live API. Nothing downstream knows which ran.

**`AgentResult`** is a single artifact with three consumers — the publisher renders `edits`
into a diff, the trace sink persists `elements`, and evals assert on all three fields. One
producer, three consumers, no back-channels.

Only `healing_dag.py` imports Airflow, so the rest of the Pattern is unit-testable without a
scheduler. This mirrors how `test_lakehouse_writer.py` and `test_streaming_sinks.py` already
test the existing Patterns.

## The failure scenario

`example_dag.py` gains a param, defaulting to off, so the current demo behavior is unchanged:

```python
@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False,
     params={"failure_mode": Param("none", enum=["none", "schema_drift"])})
```

With `failure_mode="schema_drift"`, `extract()` emits `{"id": 1, "customer_name": "example"}`.
`write_events` validates record keys against the columns parsed from
`lakehouse/create_tables.sql` and raises `SchemaDriftError` naming the offending field and the
expected columns.

The DDL is already described in `hooks/post_gen_project.py` as "the schema's only hand-authored
source," and the post-gen hook copies it into dbt's `on-run-start`. Deriving both the runtime
error and the eval's ground truth from that same file means there is no third place for the
schema to drift.

When `include_lakehouse=false` there is no DDL to validate against. In that configuration the
demo ships a `missing_config` failure mode instead, and traces fall back to JSONL.

## Trigger mechanism

The healing task lives in the same DAG, with `trigger_rule=ALL_FAILED`:

```python
heal = heal_on_failure.override(trigger_rule=TriggerRule.ALL_FAILED)(load_result)
```

Same DAG run, same worker, same log directory. `read_task_log` is therefore a local file read
rather than an API call, the whole loop is exercisable via `dag.test()` in pytest, and no
credentials are required anywhere.

Rejected alternatives:

- **`on_failure_callback` → REST API → separate healing DAG.** Closer to a real multi-pipeline
  deployment, but requires API credentials and a live scheduler to observe. Untestable in CI.
- **Asset/Dataset-driven.** Airflow 3 assets fire on success, not failure. Wrong primitive.

The healing logic lives in `agentic/`, not in the DAG file, so promoting it to a standalone
healing DAG later is a wiring change rather than a rewrite.

`FailureContext` carries `dag_id`, `dag_run_id`, `task_id`, `try_number`, exception type and
message, and the log file path.

## Agent contract

### Tool allowlist

Three tools. No bash, no write, no glob.

| Tool | Input | Returns | Rationale |
|---|---|---|---|
| `read_task_log` | `task_id`, optional `tail_lines` (capped) | log text | The trace records whether the agent actually read it — evidence it diagnosed rather than pattern-matched |
| `read_repo_file` | `path` | file text | Path confinement is enforceable at a typed boundary; it is not if the agent has bash |
| `propose_patch` | `path`, `old_str`, `new_str`, `rationale` | accepted / rejected | Produces a diff without touching the filesystem |

`propose_patch` uses `str_replace` semantics but **stages** rather than applies: it verifies
`old_str` occurs exactly once and records the edit. The healing task renders accumulated edits
into a unified diff at the end. Nothing on disk changes during the agent run, so a bad run
costs an unused `.patch` file — and an eval can score a patch that was never applied.

`read_repo_file` resolves each path to canonical form and rejects anything outside the project
root (`..`, symlinks, absolute escapes, encoded traversal). The input is model output; this is
load-bearing.

The tool loop has a hard iteration cap (default 12).

### Structured output

The final turn is schema-constrained, so parsing cannot fail:

```python
class AgentDiagnosis(BaseModel):
    root_cause: str
    offending_field: str | None
    confidence: Literal["high", "medium", "low"]
    patch_summary: str
```

### Model configuration

The model is a cookiecutter variable, `agentic_model`, defaulting to `claude-haiku-4-5`.

Request parameters differ by model tier, so `runner.py` builds them through a
`_model_kwargs(model) -> dict` helper: a short lookup table with a conservative fallback for
unrecognized IDs (no `effort`, no `thinking`, no sampling parameters — the intersection valid
on every model). Three entries and a comment, not a compatibility matrix.

For the `claude-haiku-4-5` default:

```python
runner = client.beta.messages.tool_runner(
    model=MODEL,
    max_tokens=8000,
    temperature=0,
    tools=[read_task_log, read_repo_file, propose_patch],
    system=SYSTEM_PROMPT,
    messages=[{"role": "user", "content": render_context(ctx)}],
)
```

Tier-specific constraints the helper encodes:

- `output_config.effort` errors on Haiku 4.5; it is valid on Opus 4.5+ and Sonnet 4.6+.
- Haiku 4.5 predates adaptive thinking. If thinking is wanted there it is
  `{"type": "enabled", "budget_tokens": N}`; the default omits thinking entirely.
- `temperature` is accepted on Haiku 4.5 and rejected with a 400 on `claude-opus-5`.
- Haiku 4.5 caps at 200K context and 64K output, which is why `read_task_log` bounds
  `tail_lines`.

`temperature=0` does not guarantee identical output. The `FakeRunner` / `ReplayRunner` seam is
the actual determinism mechanism.

The system prompt lives in `agentic/prompts/diagnose.md` as a file, so users can edit it
without touching code and evals can pin a prompt version.

## Observability

### Trace format

The entire agent run is captured as **one JSON document per failure event**, whose top level is
an array of elements in emission order:

```json
[
  {"seq": 0, "kind": "agent_run", "name": "heal", "status": "running",
   "started_at": "...", "dag_id": "...", "dag_run_id": "...", "task_id": "load"},
  {"seq": 1, "kind": "llm_turn",  "name": "turn_1", "input_tokens": 1842, "output_tokens": 96},
  {"seq": 2, "kind": "tool_call", "name": "read_task_log",  "status": "ok", "duration_ms": 12},
  {"seq": 3, "kind": "tool_call", "name": "read_repo_file", "status": "ok",
   "input_summary": {"path": "..."}},
  {"seq": 4, "kind": "llm_turn",  "name": "turn_2"},
  {"seq": 5, "kind": "tool_call", "name": "propose_patch", "status": "ok"},
  {"seq": 6, "kind": "result",    "diagnosis": {}, "patch_path": "..."},
  {"seq": 7, "kind": "agent_run", "name": "heal", "status": "ok", "duration_ms": 4210}
]
```

Element kinds: `agent_run`, `llm_turn`, `tool_call`, `result`. Common fields are `seq`, `kind`,
`name`, `status`, `started_at`, `ended_at`, `duration_ms`, and `error`. `input_summary` and
`output_summary` are truncated; full payloads are never persisted. Token counts come from
`response.usage` and are null under `FakeRunner`.

A JSON array is ordered, so `seq` replaces a parent/child span tree. In a single-threaded agent
loop, emission order *is* causal order.

`TraceRecorder` accumulates elements in memory and appends the terminal element in a `finally`,
so a crashed run still produces a well-formed array ending in `status: "error"`.

### Storage

The array is the unit of write. Atomicity is structural: the document is written or it is not.

`TraceSink` has one method, `write(trace: Trace) -> None`, with two implementations selected
exactly as `sinks.py` already selects its backend — Lakehouse if enabled, JSONL otherwise:

- **`DuckDBSink`** writes one row per failure event through the existing `lakehouse/writer.py`.
- **`JsonlSink`** writes `artifacts/traces/<trace_id>.json`.

Both persist byte-identical documents, so an eval asserting on a trace does not care which sink
ran.

`agent_traces` DDL, added to `create_tables.sql`:

| Column | Type |
|---|---|
| `trace_id` | `VARCHAR` primary key |
| `dag_id`, `dag_run_id`, `task_id` | `VARCHAR` |
| `try_number` | `INTEGER` |
| `occurred_at` | `TIMESTAMP` |
| `status` | `VARCHAR` |
| `elements` | `JSON` |

DuckDB's native JSON support keeps aggregate queries first-class:
`unnest(json_extract(elements, '$[*]'))` recovers a row-per-element view without the write path
paying for that shape.

### Failure isolation

The sink call is wrapped in a swallow-and-log: a trace write that raises is caught, logged as a
warning, and discarded.

This is a deliberate exception suppression. The healing task exists to fix a schema failure; a
schema failure in the trace table taking down the fixer is a circular failure worth designing
out. The cost is silent trace loss, mitigated by the warning landing in the Airflow task log.

## Evals

### Tiers

| Tier | Marker | Runner | Gates CI | Proves |
|---|---|---|---|---|
| Unit | *(none)* | `FakeRunner` | Yes | Plumbing: trace well-formed, patch renders, publisher writes, sink parity |
| Replay | *(none)* | `ReplayRunner` | Yes | The real loop: tool dispatch, schema validation, parsing |
| Live | `live` | `ClaudeRunner` | No | Agent quality against the live model |
| Graded | `graded` | `ClaudeRunner` + `ClaudeGrader` | No | Scored, per-criterion feedback |

`uv run pytest` runs tiers 1–2: deterministic, no key, no network. `uv run pytest -m live` and
`-m graded` run the rest, and **skip themselves when `ANTHROPIC_API_KEY` is unset** rather than
failing, so a fresh clone never breaks mysteriously.

The generated `pyproject.toml` gains `[tool.pytest.ini_options]` with `live` and `graded`
markers, following the existing `slow` marker idiom in the template repo's own `pyproject.toml`.

### ReplayRunner

`ReplayRunner` mocks the **HTTP transport**, not the runner. `tool_runner` genuinely loops,
tools genuinely execute, and structured output is genuinely parsed; only model responses are
canned. Cassettes live in `evals/cassettes/*.json` and are recorded by running tier 3 with
`--record`. Re-record when the prompt changes.

### Cases as data

`evals/cases/schema_drift.yaml`:

```yaml
id: schema_drift
failure_mode: schema_drift
expect:
  tools_called: [read_task_log, read_repo_file, propose_patch]   # order-sensitive
  diagnosis_mentions: [customer_name, name]
  patch_targets: "src/<slug>/batch/dags/example_dag.py"
  min_confidence: medium
score:
  required: [tools_called, patch_targets]
  advisory: [diagnosis_mentions, min_confidence]
```

A single parametrized test collects every YAML in `cases/` and scores the trace array against
it. Adding a case means adding a file, not writing a test — which matters in a template, since
a generated project's failure cases will differ from the ones shipped here.

The `required` / `advisory` split is explicit in the data. `patch_targets` is objectively
checkable and gates. `diagnosis_mentions` is keyword matching on prose: useful signal, poor
gate. Encoding the distinction prevents a later "fix" to a flaky eval from loosening the
load-bearing criterion.

Ordered `tools_called` is what proves the agent read the log *before* proposing a patch — an
assertion the JSON array makes a one-liner.

### Grader

`evals/grader.py` defines a `Grader` protocol with `FakeGrader` (fixture verdict) and
`ClaudeGrader`, mirroring the `AgentRunner` seam so the eval suite does not acquire a
non-deterministic dependency in the layer that exists to be deterministic. It lives under
`evals/` rather than `agentic/` because it is test infrastructure, not runtime code — the
healing DAG never imports it. `GradeReport` and `CriterionVerdict` are defined there too.

```python
class CriterionVerdict(BaseModel):
    criterion: str
    score: int          # 0-5
    passed: bool
    feedback: str

class GradeReport(BaseModel):
    overall: int                        # 0-5
    verdicts: list[CriterionVerdict]
    summary: str
    suggested_rubric_gaps: list[str]
```

The grader receives the case's rubric YAML, the full trace array, and the rendered patch. It
does **not** receive the agent's system prompt — grading against the same instructions the
agent was given measures compliance, not correctness.

Per-criterion scores rather than a single number: a bare score says a regression happened;
per-criterion scores say which criterion moved.

`suggested_rubric_gaps` is advisory output for a human, never an assertion. The grader noticing
that the agent skipped a step no criterion covers is how the rubric improves.

Reports are written to `evals/reports/<case_id>-<timestamp>.json` plus a Markdown summary
table. Feedback that only appears in pytest output gets ignored.

Two guardrails:

1. The grader never gates the default test run. CI stays green or red on tiers 1–2 alone.
2. Grader calls are cassette-recordable. `FakeGrader` plus a recorded grader cassette covers
   the report-rendering path in tier 1, so a bug in the Markdown summary does not require an
   API key to catch.

The grader uses a separate cookiecutter variable, `agentic_grader_model`, defaulting to the
same value as `agentic_model`. Grading is a judgment task and may warrant a stronger model than
diagnosis, without paying for it on every heal.

## Publishing

`PRPublisher` protocol with two implementations:

- **`LocalPatchPublisher`** (default) writes `artifacts/<trace_id>.patch` plus a metadata JSON
  carrying the proposed PR title and body. No credentials required.
- **`GitHubPublisher`** (opt-in, config-gated) branches, commits, pushes, and opens a PR.

The default matters for out-of-box experience: a freshly generated scaffold that tries to push
to a remote on first run is a bad first impression, and most generated projects have no remote
yet. The patch artifact is also exactly what evals score, so the diff is the assertable output
either way. A documented one-liner turns an artifact into a PR.

## Template integration

### cookiecutter.json

```json
"include_agentic": true,
"agentic_model": "claude-haiku-4-5",
"agentic_grader_model": "claude-haiku-4-5"
```

### Pattern manifest

`hooks/post_gen_project.py` gains an entry. `tests/test_pattern_manifest.py` cross-checks
`gated` against every `cookiecutter.include_agentic` site in the template tree, so a missed
Jinja gate fails a test rather than drifting silently.

```python
{
    "flag_name": "include_agentic",
    "flag": {{ cookiecutter.include_agentic }},
    "remove": [
        f"src/{SLUG}/agentic",
        "tests/test_agentic_contracts.py",
        "tests/test_agentic_tracing.py",
        "tests/test_agentic_publisher.py",
        "evals",
    ],
    "gated": [
        "docker-compose.yml",
        "pyproject.toml",
        ".github/workflows/ci.yml",
        f"src/{SLUG}/lakehouse/create_tables.sql",
        f"src/{SLUG}/batch/dags/example_dag.py",
    ],
}
```

### Pre-generation validation

`hooks/pre_gen_project.py` rejects `include_agentic=true` with `include_batch=false`: there is
no DAG to heal. A clear error beats a silent degradation.

This is the first Pattern with a cross-Pattern dependency. The existing four are mutually
independent; Lakehouse is a storage layer that Batch and Streaming write into, whereas Agentic
is a control layer that observes Batch. The pre-gen check is where that asymmetry is enforced.

### Dependencies

`pyproject.toml` gains `anthropic` and `pyyaml` under `include_agentic`. The generated CI
workflow runs `uv run pytest`, unchanged — tiers 3 and 4 are never invoked in CI.

## Testing strategy

| Test | Tier | Covers |
|---|---|---|
| `test_agentic_contracts.py` | unit | Record shapes, serialization round-trip |
| `test_agentic_tools.py` | unit | Path confinement, `propose_patch` single-match rule, diff rendering |
| `test_agentic_tracing.py` | unit | Array well-formedness, terminal element on crash, DuckDB/JSONL parity, sink-failure suppression |
| `test_agentic_publisher.py` | unit | Patch artifact contents, metadata |
| `test_agentic_dag.py` | unit | `dag.test()` end-to-end with `FakeRunner`; heal task fires on failure and is skipped on success |
| `test_agentic_replay.py` | replay | Full loop against cassettes |
| `evals/test_cases.py` | live / graded | Parametrized over `evals/cases/*.yaml` |
| `tests/test_bake_agentic.py` | template repo | Bake with the flag on and off; assert file presence and absence |
| `tests/test_pattern_manifest.py` | template repo | Existing test, now covering the new Pattern |

Both `include_lakehouse` branches need coverage in `test_agentic_tracing.py`, matching how
`test_streaming_sinks.py` is already gated on that flag.

## Open risks

- **Cassette staleness.** Prompt edits invalidate recordings, and a stale cassette passes while
  the live path is broken. Mitigation: `-m live` in a scheduled job, not just on demand.
- **Grader drift.** Grader scores are not comparable across model versions. Mitigation: the
  report records `agentic_grader_model`; treat cross-version comparisons as invalid.
- **Log volume.** `tail_lines` bounds input, but a pathological log could still dominate the
  context on a 200K-context model. Mitigation: hard character cap in `read_task_log`.
