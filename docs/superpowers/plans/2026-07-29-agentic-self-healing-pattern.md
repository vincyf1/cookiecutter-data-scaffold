# Agentic Self-Healing Pattern Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `include_agentic`, a fifth composable Pattern that runs a bounded AI agent when a batch DAG task fails — diagnosing from logs, proposing a patch as a reviewable artifact, capturing the run as a JSON array, and scoring behavior through a four-tier eval suite.

**Architecture:** Six modules under `src/<slug>/agentic/` behind two protocol seams — `AgentRunner` (isolates all non-determinism) and `TraceSink` (mirrors the existing `sinks.py` Lakehouse/fallback idiom). Only `dags/healing_dag.py` imports Airflow, so every other module is unit-testable with plain pytest. The healing task runs in the same DAG via `trigger_rule=ALL_FAILED`.

**Tech Stack:** Python 3.12+, cookiecutter + Jinja2, Airflow 3.2 (`airflow.sdk`), DuckDB, `anthropic` SDK, pytest, pyyaml, ruff.

**Spec:** `docs/superpowers/specs/2026-07-29-agentic-self-healing-pattern-design.md`

---

## Global Constraints

Every task's requirements implicitly include this section.

**Template mechanics**

- Everything under `{{cookiecutter.project_slug}}/` is **Jinja-rendered before it is Python**. Never write a literal `{{` or `{%` in those files. This forbids nested dict literals written as `{{...}}`, f-strings like `f"{{x}}"`, and `.format()` templates with escaped braces. Single braces are fine.
- Imports inside template files use `{{ cookiecutter.project_slug }}` as the package name.
- `hooks/` and `tests/` in the template repo are **not** Jinja-rendered at bake time except `hooks/*.py`, which are rendered. `tests/` in the repo root is plain Python.
- `[tool.ruff] extend-exclude` already excludes `hooks` and the template dir, so repo-root `ruff check .` does not lint generated code. The generated project has its own ruff config.

**Manifest invariant — every commit must satisfy this**

`tests/test_pattern_manifest.py` asserts that the `gated` list for `include_agentic` in `hooks/post_gen_project.py` **exactly equals** the set of files under `{{cookiecutter.project_slug}}/` containing the string `cookiecutter.include_agentic`. Any task that adds or removes a Jinja gate must update `gated` in the same commit. Any task that adds a file removed by the flag must update `remove` in the same commit.

**Pattern dependency**

`include_agentic=true` requires `include_batch=true`. Enforced in `hooks/pre_gen_project.py`.

**Model configuration**

- Default `agentic_model`: `claude-haiku-4-5`. Default `agentic_grader_model`: `claude-haiku-4-5`.
- Request parameters are tier-dependent and must go through `_model_kwargs()`. `temperature` is valid on Haiku 4.5 and returns 400 on `claude-opus-5`. `output_config.effort` is the reverse. Never pass either unconditionally.
- Never hardcode a model ID outside `runner.py` / `grader.py`; read the cookiecutter variable.

**Determinism**

- `uv run pytest` in a generated project must pass with no network and no `ANTHROPIC_API_KEY`.
- Tests marked `live` or `graded` must **skip** (not fail) when `ANTHROPIC_API_KEY` is unset.

**Deviation from spec, already decided**

The spec says the `agent_traces` DDL is "added to `create_tables.sql`". **Do not do this.** `hooks/post_gen_project.py::sync_lakehouse_ddl` collapses that entire file into a single whitespace-normalized statement and inlines it into dbt's `on-run-start` hook. A second `CREATE TABLE` there would produce one malformed concatenated statement in `dbt_project.yml`. The traces DDL therefore lives in its own file, `src/<slug>/agentic/create_traces_table.sql`, owned by the agentic Pattern. This also keeps `create_tables.sql` as the single hand-authored source for the *events* schema, which the schema-drift failure depends on.

**Dev loop**

Template-level tests run from the repo root:

```bash
uv run pytest -m "not slow" -v
```

Generated-code logic cannot be exercised that way. Bake once and iterate inside the baked project:

```bash
rm -rf /tmp/scaffold-dev && mkdir -p /tmp/scaffold-dev
uv run cookiecutter . --no-input --output-dir /tmp/scaffold-dev
cd /tmp/scaffold-dev/my_data_project && uv sync && uv run pytest -v
```

Re-bake after every template edit — the baked copy does not track the template.

---

## File Structure

**Template repo (not rendered):**

| File | Responsibility | Task |
|---|---|---|
| `cookiecutter.json` | Add `include_agentic`, `agentic_model`, `agentic_grader_model` | 1 |
| `hooks/pre_gen_project.py` | Reject `agentic && !batch` | 1 |
| `hooks/post_gen_project.py` | `PATTERNS` entry for `include_agentic` | 1, grown per task |
| `tests/conftest.py` | `include_agentic` in `DEFAULT_FLAGS` | 1 |
| `tests/test_bake_agentic.py` | Bake-level presence/absence assertions | 1, grown per task |
| `tests/test_bake_integration.py` | Fix all-off and single-pattern cases | 1 |

**Generated project (Jinja-rendered), all under `{{cookiecutter.project_slug}}/`:**

| File | Responsibility | Task |
|---|---|---|
| `src/<slug>/agentic/__init__.py` | Package marker | 2 |
| `src/<slug>/agentic/contracts.py` | `FailureContext`, `ProposedEdit`, `AgentDiagnosis`, `AgentResult`, `TraceElement`, `Trace`. No logic. | 2 |
| `src/<slug>/agentic/tracing.py` | `TraceRecorder`, `TraceSink`, `DuckDBSink`, `JsonlSink`, `write_trace_safely` | 3 |
| `src/<slug>/agentic/create_traces_table.sql` | `agent_traces` DDL | 3 |
| `src/<slug>/agentic/tools.py` | `ToolContext`, `read_task_log`, `read_repo_file`, `propose_patch`, `render_diff` | 4 |
| `src/<slug>/agentic/runner.py` | `AgentRunner`, `FakeRunner`, `ClaudeRunner`, `_model_kwargs` | 5 |
| `src/<slug>/agentic/prompts/diagnose.md` | System prompt as data | 5 |
| `src/<slug>/agentic/publisher.py` | `PRPublisher`, `LocalPatchPublisher`, `GitHubPublisher` | 6 |
| `src/<slug>/batch/dags/example_dag.py` | `failure_mode` param + heal task wiring | 7, 8 |
| `src/<slug>/lakehouse/writer.py` | `SchemaDriftError`, DDL-derived column validation | 7 |
| `src/<slug>/agentic/dags/healing_dag.py` | `build_failure_context`, `heal` task factory | 8 |
| `evals/cases/schema_drift.yaml` | Rubric as data | 9 |
| `evals/replay.py` | `ReplayRunner` + cassette loading | 9 |
| `evals/test_cases.py` | Parametrized scorer over `cases/*.yaml` | 9 |
| `evals/grader.py` | `Grader`, `FakeGrader`, `ClaudeGrader`, `GradeReport`, report rendering | 10 |
| `tests/test_agentic_*.py` | Unit tests per module | 2–8 |

---

### Task 1: Template plumbing for the new flag

Adds the flag, its cross-Pattern validation, and the manifest entry — with no agentic files yet, so `remove` and `gated` are empty and the manifest test passes. This task exists separately because it also has to repair three existing tests that assume four flags.

**Files:**
- Modify: `cookiecutter.json`
- Modify: `hooks/pre_gen_project.py`
- Modify: `hooks/post_gen_project.py`
- Modify: `tests/conftest.py:6-11`
- Modify: `tests/test_bake_integration.py`
- Create: `tests/test_bake_agentic.py`

**Interfaces:**
- Consumes: nothing
- Produces: cookiecutter context keys `include_agentic` (bool), `agentic_model` (str), `agentic_grader_model` (str); a `PATTERNS` entry with `flag_name="include_agentic"`; `conftest.DEFAULT_FLAGS` including `include_agentic: True`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_bake_agentic.py`:

```python
import pytest

from conftest import bake_with


def test_agentic_flag_is_in_context(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    assert result.exit_code == 0
    assert result.context["include_agentic"] is True
    assert result.context["agentic_model"] == "claude-haiku-4-5"
    assert result.context["agentic_grader_model"] == "claude-haiku-4-5"


def test_agentic_without_batch_is_rejected(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=False)
    assert result.exit_code != 0


def test_agentic_with_batch_bakes_cleanly(cookies):
    result = bake_with(
        cookies,
        include_agentic=True,
        include_batch=True,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=False,
    )
    assert result.exit_code == 0


@pytest.mark.parametrize("agentic", [True, False])
def test_bake_succeeds_with_flag_either_way(cookies, agentic):
    result = bake_with(cookies, include_agentic=agentic, include_batch=True)
    assert result.exit_code == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_bake_agentic.py -v`
Expected: FAIL — `KeyError: 'include_agentic'` on the context assertions, and `test_agentic_without_batch_is_rejected` fails because nothing rejects it yet.

- [ ] **Step 3: Add the cookiecutter variables**

In `cookiecutter.json`, add three keys after `include_dbt`:

```json
{
  "project_name": "My Data Project",
  "project_slug": "{{ cookiecutter.project_name.lower().replace(' ', '_').replace('-', '_') }}",
  "author_name": "Your Name",
  "include_batch": true,
  "include_streaming": true,
  "include_lakehouse": true,
  "include_dbt": true,
  "include_agentic": true,
  "agentic_model": "claude-haiku-4-5",
  "agentic_grader_model": "claude-haiku-4-5",
  "dbt_duckdb_version": "1.8",
  "sqlfluff_version": "3.1.0",
  "airflow_version": "3.2.0"
}
```

- [ ] **Step 4: Add the pre-generation guard**

Append to `hooks/pre_gen_project.py`:

```python
INCLUDE_AGENTIC = {{ cookiecutter.include_agentic }}
INCLUDE_BATCH = {{ cookiecutter.include_batch }}

if INCLUDE_AGENTIC and not INCLUDE_BATCH:
    print(
        "ERROR: include_agentic requires include_batch.\n"
        "The Agentic Pattern heals a failing batch DAG; with no DAG scaffolded "
        "there is nothing for it to observe. Enable include_batch, or set "
        "include_agentic=false."
    )
    sys.exit(1)
```

- [ ] **Step 5: Add the manifest entry**

In `hooks/post_gen_project.py`, append to the `PATTERNS` list, after the `include_dbt` entry:

```python
    {
        "flag_name": "include_agentic",
        "flag": {{ cookiecutter.include_agentic }},
        "remove": [],
        "gated": [],
    },
```

Both lists are empty and correct: no file references `cookiecutter.include_agentic` yet, and no file is agentic-only yet. Later tasks grow them.

- [ ] **Step 6: Update the shared bake defaults**

In `tests/conftest.py`, add to `DEFAULT_FLAGS`:

```python
DEFAULT_FLAGS = {
    "include_batch": True,
    "include_streaming": True,
    "include_lakehouse": True,
    "include_dbt": True,
    "include_agentic": True,
}
```

- [ ] **Step 7: Repair the existing tests that assume four flags**

Three tests in `tests/test_bake_integration.py` now bake an invalid combination, because `DEFAULT_FLAGS` turns agentic on while they turn batch off.

In `test_single_pattern_alone_lints_clean`, replace the `all_off` dict and add agentic to the parametrize list, pairing agentic with batch since it cannot stand alone:

```python
@pytest.mark.parametrize(
    "flag",
    ["include_batch", "include_streaming", "include_lakehouse", "include_dbt", "include_agentic"],
)
def test_single_pattern_alone_lints_clean(cookies, flag):
    # For include_dbt, this only checks that the bake+prune succeeds and
    # ruff has nothing to complain about (the only Python file left is an
    # empty src/{slug}/__init__.py) — it is not a meaningful lint gate on
    # transformation/'s SQL/YAML content. Real dbt-content verification for
    # the dbt-only combination is test_dbt_only_bake_dbt_build_and_test_pass.
    # include_agentic cannot stand alone: pre_gen_project.py rejects it
    # without include_batch, so that case is agentic+batch.
    all_off = {
        "include_batch": False,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": False,
        "include_agentic": False,
    }
    all_off[flag] = True
    if flag == "include_agentic":
        all_off["include_batch"] = True

    result = bake_with(cookies, **all_off)
    assert result.exit_code == 0

    run_cmd(result.project_path, "uvx", "ruff", "check", ".")
```

In `test_full_bake_all_patterns_off_lints_clean`, add the flag:

```python
    result = bake_with(
        cookies,
        include_batch=False,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=False,
        include_agentic=False,
    )
```

In `test_dbt_only_bake_ci_tolerates_no_tests_collected` and both `test_dbt_only_*` / `test_dbt_and_lakehouse_*` slow tests, add `include_agentic=False` to each `bake_with(...)` call that sets `include_batch=False`. Search for `include_batch=False` and add the flag to every such call site in the file.

- [ ] **Step 8: Run the full template suite**

Run: `uv run pytest -m "not slow" -v`
Expected: PASS, including `tests/test_pattern_manifest.py` and all of `tests/test_bake_agentic.py`.

- [ ] **Step 9: Commit**

```bash
git add cookiecutter.json hooks/ tests/
git commit -m "feat: add include_agentic Pattern flag and cross-Pattern validation"
```

---

### Task 2: Typed contracts

Pure data. No behavior, no imports beyond the standard library and Pydantic. Everything downstream depends on these names, so they land first and never change.

**Files:**
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/__init__.py`
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/contracts.py`
- Create: `{{cookiecutter.project_slug}}/tests/test_agentic_contracts.py`
- Modify: `hooks/post_gen_project.py` (`remove` list)
- Modify: `{{cookiecutter.project_slug}}/pyproject.toml` (add `pydantic` under the agentic flag)
- Modify: `tests/test_bake_agentic.py`

**Interfaces:**
- Consumes: cookiecutter context from Task 1
- Produces:
  - `FailureContext(dag_id: str, dag_run_id: str, task_id: str, try_number: int, exception_type: str, exception_message: str, log_path: str, project_root: str)`
  - `ProposedEdit(path: str, old_str: str, new_str: str, rationale: str)`
  - `AgentDiagnosis(root_cause: str, offending_field: str | None, confidence: Literal["high","medium","low"], patch_summary: str)`
  - `AgentResult(diagnosis: AgentDiagnosis, edits: list[ProposedEdit], elements: list[TraceElement])`
  - `TraceElement(seq: int, kind: Literal["agent_run","llm_turn","tool_call","result"], name: str, status: str, started_at: str, ended_at: str | None, duration_ms: int | None, input_summary: dict | None, output_summary: dict | None, input_tokens: int | None, output_tokens: int | None, error: str | None)`
  - `Trace(trace_id: str, dag_id: str, dag_run_id: str, task_id: str, try_number: int, occurred_at: str, status: str, elements: list[TraceElement])` with `.to_json_array() -> str`

- [ ] **Step 1: Write the failing test**

Create `{{cookiecutter.project_slug}}/tests/test_agentic_contracts.py`:

```python
import json

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    AgentResult,
    FailureContext,
    ProposedEdit,
    Trace,
    TraceElement,
)


def _element(seq: int, kind: str, name: str) -> TraceElement:
    return TraceElement(
        seq=seq,
        kind=kind,
        name=name,
        status="ok",
        started_at="2026-07-29T00:00:00Z",
        ended_at="2026-07-29T00:00:01Z",
        duration_ms=1000,
    )


def test_failure_context_round_trips():
    ctx = FailureContext(
        dag_id="example_events_dag",
        dag_run_id="run_1",
        task_id="load",
        try_number=1,
        exception_type="SchemaDriftError",
        exception_message="unexpected field 'customer_name'",
        log_path="/logs/load.log",
        project_root="/project",
    )
    assert FailureContext.model_validate(ctx.model_dump()) == ctx


def test_trace_serializes_to_a_json_array():
    trace = Trace(
        trace_id="t1",
        dag_id="d",
        dag_run_id="r",
        task_id="load",
        try_number=1,
        occurred_at="2026-07-29T00:00:00Z",
        status="ok",
        elements=[_element(0, "agent_run", "heal"), _element(1, "tool_call", "read_task_log")],
    )

    parsed = json.loads(trace.to_json_array())

    assert isinstance(parsed, list)
    assert [e["seq"] for e in parsed] == [0, 1]
    assert parsed[1]["name"] == "read_task_log"


def test_trace_json_array_omits_trace_level_metadata():
    # The array holds elements only; correlation columns live beside it in storage.
    trace = Trace(
        trace_id="t1",
        dag_id="d",
        dag_run_id="r",
        task_id="load",
        try_number=1,
        occurred_at="2026-07-29T00:00:00Z",
        status="ok",
        elements=[_element(0, "agent_run", "heal")],
    )

    parsed = json.loads(trace.to_json_array())

    assert "trace_id" not in parsed[0]


def test_agent_result_holds_diagnosis_edits_and_elements():
    result = AgentResult(
        diagnosis=AgentDiagnosis(
            root_cause="field renamed",
            offending_field="customer_name",
            confidence="high",
            patch_summary="rename customer_name to name",
        ),
        edits=[
            ProposedEdit(
                path="src/pkg/dag.py",
                old_str="customer_name",
                new_str="name",
                rationale="matches DDL column",
            )
        ],
        elements=[_element(0, "result", "done")],
    )

    assert result.diagnosis.confidence == "high"
    assert result.edits[0].new_str == "name"
```

- [ ] **Step 2: Run test to verify it fails**

Bake and run:

```bash
rm -rf /tmp/scaffold-dev && mkdir -p /tmp/scaffold-dev
uv run cookiecutter . --no-input --output-dir /tmp/scaffold-dev
cd /tmp/scaffold-dev/my_data_project && uv sync && uv run pytest tests/test_agentic_contracts.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'my_data_project.agentic'`

- [ ] **Step 3: Create the package and contracts**

Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/__init__.py` as an empty file.

Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/contracts.py`:

```python
"""Typed records shared across the agentic Pattern. No behavior lives here.

AgentResult is produced once per healing run and read by three consumers:
publisher.py renders `edits` into a diff, tracing.py persists `elements`,
and the eval suite asserts on all three fields.
"""

import json
from typing import Literal

from pydantic import BaseModel

ElementKind = Literal["agent_run", "llm_turn", "tool_call", "result"]
Confidence = Literal["high", "medium", "low"]


class FailureContext(BaseModel):
    """Everything the agent is told about the failure, assembled from Airflow."""

    dag_id: str
    dag_run_id: str
    task_id: str
    try_number: int
    exception_type: str
    exception_message: str
    log_path: str
    project_root: str


class ProposedEdit(BaseModel):
    """A staged str_replace. Never applied to disk during the agent run."""

    path: str
    old_str: str
    new_str: str
    rationale: str


class AgentDiagnosis(BaseModel):
    """The schema the model's final turn is constrained to."""

    root_cause: str
    offending_field: str | None = None
    confidence: Confidence
    patch_summary: str


class TraceElement(BaseModel):
    """One entry in the run's JSON array. Order is causal; `seq` replaces a span tree."""

    seq: int
    kind: ElementKind
    name: str
    status: str
    started_at: str
    ended_at: str | None = None
    duration_ms: int | None = None
    input_summary: dict | None = None
    output_summary: dict | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    error: str | None = None


class AgentResult(BaseModel):
    diagnosis: AgentDiagnosis
    edits: list[ProposedEdit]
    elements: list[TraceElement]


class Trace(BaseModel):
    """One document per failure event: correlation metadata plus the element array."""

    trace_id: str
    dag_id: str
    dag_run_id: str
    task_id: str
    try_number: int
    occurred_at: str
    status: str
    elements: list[TraceElement]

    def to_json_array(self) -> str:
        """Serialize the elements as a bare JSON array.

        This is the canonical artifact. Both sinks persist byte-identical
        output, so an eval asserting on a trace does not care which ran.
        """
        return json.dumps([e.model_dump(exclude_none=True) for e in self.elements])
```

- [ ] **Step 4: Add the pydantic dependency**

In `{{cookiecutter.project_slug}}/pyproject.toml`, add inside the `dependencies` list, after the `include_dbt` block:

```
{%- if cookiecutter.include_agentic %}
    "pydantic>=2.7",
{%- endif %}
```

- [ ] **Step 5: Update the manifest**

In `hooks/post_gen_project.py`, the `include_agentic` entry becomes:

```python
    {
        "flag_name": "include_agentic",
        "flag": {{ cookiecutter.include_agentic }},
        "remove": [
            f"src/{SLUG}/agentic",
            "tests/test_agentic_contracts.py",
        ],
        "gated": [
            "pyproject.toml",
        ],
    },
```

- [ ] **Step 6: Add the bake-level assertions**

Append to `tests/test_bake_agentic.py`:

```python
def test_agentic_module_present_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    slug = result.context["project_slug"]
    agentic = result.project_path / "src" / slug / "agentic"
    assert (agentic / "contracts.py").is_file()
    assert (result.project_path / "tests" / "test_agentic_contracts.py").is_file()


def test_agentic_module_absent_when_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    slug = result.context["project_slug"]
    assert not (result.project_path / "src" / slug / "agentic").exists()
    assert not (result.project_path / "tests" / "test_agentic_contracts.py").exists()


def test_pydantic_dependency_tracks_the_flag(cookies):
    on = bake_with(cookies, include_agentic=True, include_batch=True)
    off = bake_with(cookies, include_agentic=False, include_batch=True)
    assert "pydantic" in (on.project_path / "pyproject.toml").read_text()
    assert "pydantic" not in (off.project_path / "pyproject.toml").read_text()
```

- [ ] **Step 7: Run both suites**

```bash
uv run pytest -m "not slow" -v
rm -rf /tmp/scaffold-dev && mkdir -p /tmp/scaffold-dev
uv run cookiecutter . --no-input --output-dir /tmp/scaffold-dev
cd /tmp/scaffold-dev/my_data_project && uv sync && uv run pytest tests/test_agentic_contracts.py -v
```

Expected: both PASS.

- [ ] **Step 8: Commit**

```bash
git add "{{cookiecutter.project_slug}}" hooks/ tests/
git commit -m "feat: add agentic contracts module"
```

---

### Task 3: Trace recording and sinks

**Files:**
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/tracing.py`
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/create_traces_table.sql`
- Create: `{{cookiecutter.project_slug}}/tests/test_agentic_tracing.py`
- Modify: `hooks/post_gen_project.py`
- Modify: `tests/test_bake_agentic.py`

**Interfaces:**
- Consumes: `Trace`, `TraceElement` from Task 2
- Produces:
  - `TraceRecorder(dag_id, dag_run_id, task_id, try_number, trace_id=None)` with `.element(kind, name) -> contextmanager`, `.add(kind, name, **fields) -> None`, `.finish(status) -> Trace`, `.trace_id: str`
  - `TraceSink` protocol with `write(trace: Trace) -> None`
  - `JsonlSink(base_dir: Path)`, `DuckDBSink(db_path: str)` (only when `include_lakehouse`)
  - `default_sink() -> TraceSink`
  - `write_trace_safely(sink: TraceSink, trace: Trace) -> bool`

- [ ] **Step 1: Write the failing test**

Create `{{cookiecutter.project_slug}}/tests/test_agentic_tracing.py`:

```python
import json

import pytest

from {{ cookiecutter.project_slug }}.agentic.tracing import (
    JsonlSink,
    TraceRecorder,
    write_trace_safely,
)


def _recorder() -> TraceRecorder:
    return TraceRecorder(
        dag_id="example_events_dag",
        dag_run_id="run_1",
        task_id="load",
        try_number=1,
    )


def test_recorder_numbers_elements_in_emission_order():
    rec = _recorder()
    with rec.element("tool_call", "read_task_log"):
        pass
    with rec.element("tool_call", "propose_patch"):
        pass

    trace = rec.finish("ok")

    assert [e.seq for e in trace.elements] == [0, 1, 2]
    assert [e.name for e in trace.elements] == ["read_task_log", "propose_patch", "heal"]


def test_recorder_records_duration_and_ok_status():
    rec = _recorder()
    with rec.element("tool_call", "read_repo_file"):
        pass

    trace = rec.finish("ok")
    element = trace.elements[0]

    assert element.status == "ok"
    assert element.ended_at is not None
    assert element.duration_ms is not None and element.duration_ms >= 0


def test_recorder_marks_a_raising_element_as_error_and_reraises():
    rec = _recorder()

    with pytest.raises(ValueError):
        with rec.element("tool_call", "read_repo_file"):
            raise ValueError("path escapes project root")

    trace = rec.finish("error")

    assert trace.elements[0].status == "error"
    assert "escapes project root" in trace.elements[0].error


def test_finish_appends_a_terminal_agent_run_element():
    trace = _recorder().finish("error")

    terminal = trace.elements[-1]
    assert terminal.kind == "agent_run"
    assert terminal.name == "heal"
    assert terminal.status == "error"
    assert trace.status == "error"


def test_jsonl_sink_writes_a_bare_json_array(tmp_path):
    rec = _recorder()
    with rec.element("tool_call", "read_task_log"):
        pass
    trace = rec.finish("ok")

    JsonlSink(tmp_path).write(trace)

    written = (tmp_path / f"{trace.trace_id}.json").read_text()
    parsed = json.loads(written)
    assert isinstance(parsed, list)
    assert parsed[0]["name"] == "read_task_log"


def test_write_trace_safely_swallows_sink_failure(caplog):
    class ExplodingSink:
        def write(self, trace):
            raise RuntimeError("table agent_traces does not exist")

    trace = _recorder().finish("ok")

    assert write_trace_safely(ExplodingSink(), trace) is False
    assert "agent_traces" in caplog.text


def test_write_trace_safely_reports_success(tmp_path):
    trace = _recorder().finish("ok")
    assert write_trace_safely(JsonlSink(tmp_path), trace) is True
```

Add a Lakehouse-gated block at the end of the same file — this mirrors how `tests/test_streaming_sinks.py` is already gated on `include_lakehouse`:

```python
{%- if cookiecutter.include_lakehouse %}


def test_duckdb_sink_stores_one_row_per_failure_event(tmp_path):
    import duckdb

    from {{ cookiecutter.project_slug }}.agentic.tracing import DuckDBSink

    db_path = str(tmp_path / "lakehouse.duckdb")
    rec = _recorder()
    with rec.element("tool_call", "read_task_log"):
        pass
    trace = rec.finish("ok")

    DuckDBSink(db_path).write(trace)

    con = duckdb.connect(db_path)
    rows = con.execute("SELECT trace_id, dag_id, status, elements FROM agent_traces").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == trace.trace_id
    assert rows[0][1] == "example_events_dag"
    assert rows[0][2] == "ok"
    assert json.loads(rows[0][3])[0]["name"] == "read_task_log"


def test_both_sinks_persist_identical_documents(tmp_path):
    import duckdb

    from {{ cookiecutter.project_slug }}.agentic.tracing import DuckDBSink

    rec = _recorder()
    with rec.element("tool_call", "read_task_log"):
        pass
    trace = rec.finish("ok")

    JsonlSink(tmp_path).write(trace)
    db_path = str(tmp_path / "lakehouse.duckdb")
    DuckDBSink(db_path).write(trace)

    from_jsonl = (tmp_path / f"{trace.trace_id}.json").read_text()
    con = duckdb.connect(db_path)
    from_duckdb = con.execute("SELECT elements FROM agent_traces").fetchone()[0]

    assert json.loads(from_jsonl) == json.loads(from_duckdb)
{%- endif %}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
rm -rf /tmp/scaffold-dev && mkdir -p /tmp/scaffold-dev
uv run cookiecutter . --no-input --output-dir /tmp/scaffold-dev
cd /tmp/scaffold-dev/my_data_project && uv sync && uv run pytest tests/test_agentic_tracing.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'my_data_project.agentic.tracing'`

- [ ] **Step 3: Write the traces DDL**

Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/create_traces_table.sql`:

```sql
CREATE TABLE IF NOT EXISTS agent_traces (
    trace_id VARCHAR PRIMARY KEY,
    dag_id VARCHAR,
    dag_run_id VARCHAR,
    task_id VARCHAR,
    try_number INTEGER,
    occurred_at TIMESTAMP,
    status VARCHAR,
    elements JSON
);
```

This deliberately lives outside `lakehouse/create_tables.sql` — see Global Constraints.

- [ ] **Step 4: Write the implementation**

Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/tracing.py`:

```python
"""Capture of a healing run as one JSON array per failure event.

The array is the unit of write, so atomicity is structural rather than
transactional: the document is written or it is not. Element order is
emission order, which in a single-threaded agent loop is causal order --
that is why `seq` replaces a parent/child span tree.
"""

import json
import logging
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from {{ cookiecutter.project_slug }}.agentic.contracts import Trace, TraceElement

logger = logging.getLogger(__name__)

DEFAULT_TRACE_DIR = Path("artifacts/traces")
SUMMARY_MAX_CHARS = 2000


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.isoformat()


def truncate(value: str, limit: int = SUMMARY_MAX_CHARS) -> str:
    """Summaries are bounded; full payloads are never persisted."""
    if len(value) <= limit:
        return value
    return value[:limit] + f"... [truncated {len(value) - limit} chars]"


class TraceRecorder:
    """Accumulates elements in memory; emits a Trace at the end."""

    def __init__(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        trace_id: str | None = None,
    ) -> None:
        self.trace_id = trace_id or uuid.uuid4().hex
        self.dag_id = dag_id
        self.dag_run_id = dag_run_id
        self.task_id = task_id
        self.try_number = try_number
        self._elements: list[TraceElement] = []
        self._started = _now()

    def _next_seq(self) -> int:
        return len(self._elements)

    def add(self, kind: str, name: str, **fields) -> TraceElement:
        """Record a zero-duration element (used for `result` entries)."""
        moment = _now()
        element = TraceElement(
            seq=self._next_seq(),
            kind=kind,
            name=name,
            status=fields.pop("status", "ok"),
            started_at=_iso(moment),
            ended_at=_iso(moment),
            duration_ms=0,
            **fields,
        )
        self._elements.append(element)
        return element

    @contextmanager
    def element(self, kind: str, name: str, input_summary: dict | None = None):
        """Time one element. Marks it `error` and re-raises if the body raises."""
        started = _now()
        seq = self._next_seq()
        placeholder = TraceElement(
            seq=seq,
            kind=kind,
            name=name,
            status="running",
            started_at=_iso(started),
            input_summary=input_summary,
        )
        self._elements.append(placeholder)
        box: dict = {}
        try:
            yield box
        except Exception as exc:
            placeholder.status = "error"
            placeholder.error = f"{type(exc).__name__}: {exc}"
            raise
        else:
            placeholder.status = "ok"
            placeholder.output_summary = box.get("output_summary")
            placeholder.input_tokens = box.get("input_tokens")
            placeholder.output_tokens = box.get("output_tokens")
        finally:
            ended = _now()
            placeholder.ended_at = _iso(ended)
            placeholder.duration_ms = int((ended - started).total_seconds() * 1000)

    def finish(self, status: str) -> Trace:
        """Append the terminal element and freeze the trace.

        Call this from a `finally` so a crashed run still yields a
        well-formed array ending in status "error".
        """
        ended = _now()
        self._elements.append(
            TraceElement(
                seq=self._next_seq(),
                kind="agent_run",
                name="heal",
                status=status,
                started_at=_iso(self._started),
                ended_at=_iso(ended),
                duration_ms=int((ended - self._started).total_seconds() * 1000),
            )
        )
        return Trace(
            trace_id=self.trace_id,
            dag_id=self.dag_id,
            dag_run_id=self.dag_run_id,
            task_id=self.task_id,
            try_number=self.try_number,
            occurred_at=_iso(self._started),
            status=status,
            elements=self._elements,
        )


class TraceSink(Protocol):
    def write(self, trace: Trace) -> None: ...


class JsonlSink:
    """One JSON document per failure event, on local disk."""

    def __init__(self, base_dir: Path = DEFAULT_TRACE_DIR) -> None:
        self.base_dir = Path(base_dir)

    def write(self, trace: Trace) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        (self.base_dir / f"{trace.trace_id}.json").write_text(trace.to_json_array())

{%- if cookiecutter.include_lakehouse %}


DDL_PATH = Path(__file__).parent / "create_traces_table.sql"


class DuckDBSink:
    """One row per failure event; the element array lands in a JSON column."""

    def __init__(self, db_path: str = "lakehouse.duckdb") -> None:
        self.db_path = db_path

    def write(self, trace: Trace) -> None:
        import duckdb

        con = duckdb.connect(self.db_path)
        try:
            con.execute(DDL_PATH.read_text())
            con.execute(
                "INSERT OR REPLACE INTO agent_traces "
                "(trace_id, dag_id, dag_run_id, task_id, try_number, occurred_at, status, elements) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    trace.trace_id,
                    trace.dag_id,
                    trace.dag_run_id,
                    trace.task_id,
                    trace.try_number,
                    trace.occurred_at,
                    trace.status,
                    trace.to_json_array(),
                ],
            )
        finally:
            con.close()


def default_sink() -> TraceSink:
    return DuckDBSink()

{%- else %}


def default_sink() -> TraceSink:
    return JsonlSink()

{%- endif %}


def write_trace_safely(sink: TraceSink, trace: Trace) -> bool:
    """Persist a trace without ever failing the healing task.

    The healing task exists to fix a schema failure; a schema failure in
    the trace table taking down the fixer is a circular failure worth
    designing out. The cost is silent trace loss, mitigated by this warning
    landing in the Airflow task log.
    """
    try:
        sink.write(trace)
        return True
    except Exception as exc:
        logger.warning("failed to persist agent trace %s: %s", trace.trace_id, exc)
        return False
```

Note `json` is imported for symmetry with future use in this module; if ruff flags it as unused, remove the import.

- [ ] **Step 5: Update the manifest**

`remove` gains `"tests/test_agentic_tracing.py"`. `gated` gains `f"src/{SLUG}/agentic/tracing.py"` and `"tests/test_agentic_tracing.py"` **only if** those files contain `cookiecutter.include_lakehouse` — they do, but the manifest's `include_agentic` entry tracks `cookiecutter.include_agentic` sites, not lakehouse ones. So instead:

- `include_agentic` entry: add `"tests/test_agentic_tracing.py"` to `remove`. `gated` is unchanged.
- `include_lakehouse` entry: add `f"src/{SLUG}/agentic/tracing.py"` and `"tests/test_agentic_tracing.py"` to its `gated` list, since both now branch on `cookiecutter.include_lakehouse`.

- [ ] **Step 6: Run both suites**

```bash
uv run pytest -m "not slow" -v
rm -rf /tmp/scaffold-dev && mkdir -p /tmp/scaffold-dev
uv run cookiecutter . --no-input --output-dir /tmp/scaffold-dev
cd /tmp/scaffold-dev/my_data_project && uv sync && uv run pytest tests/test_agentic_tracing.py -v
```

Expected: PASS. If `test_pattern_manifest.py` fails, the message names the exact set difference — reconcile `gated` against it.

- [ ] **Step 7: Verify the lakehouse-off branch**

```bash
rm -rf /tmp/scaffold-nolake && mkdir -p /tmp/scaffold-nolake
uv run cookiecutter . --no-input --output-dir /tmp/scaffold-nolake include_lakehouse=false
cd /tmp/scaffold-nolake/my_data_project && uv sync && uv run pytest tests/test_agentic_tracing.py -v
```

Expected: PASS, with the two DuckDB tests absent from collection.

- [ ] **Step 8: Commit**

```bash
git add "{{cookiecutter.project_slug}}" hooks/
git commit -m "feat: capture agent runs as one JSON array per failure event"
```

---

### Task 4: The tool allowlist

**Files:**
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/tools.py`
- Create: `{{cookiecutter.project_slug}}/tests/test_agentic_tools.py`
- Modify: `hooks/post_gen_project.py`

**Interfaces:**
- Consumes: `ProposedEdit` from Task 2
- Produces:
  - `ToolError(Exception)`
  - `ToolContext(project_root: Path, log_path: Path, max_log_chars: int = 20000)` with `.edits: list[ProposedEdit]`
  - `read_task_log(ctx, task_id: str, tail_lines: int = 200) -> str`
  - `read_repo_file(ctx, path: str) -> str`
  - `propose_patch(ctx, path: str, old_str: str, new_str: str, rationale: str) -> str`
  - `render_diff(ctx) -> str`
  - `TOOL_SPECS: list[dict]` — JSON Schema tool definitions for the API

- [ ] **Step 1: Write the failing test**

Create `{{cookiecutter.project_slug}}/tests/test_agentic_tools.py`:

```python
import pytest

from {{ cookiecutter.project_slug }}.agentic.tools import (
    TOOL_SPECS,
    ToolContext,
    ToolError,
    propose_patch,
    read_repo_file,
    read_task_log,
    render_diff,
)


@pytest.fixture()
def ctx(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "dag.py").write_text('record = {"id": 1, "customer_name": "x"}\n')
    log = tmp_path / "load.log"
    log.write_text("\n".join(f"line {i}" for i in range(500)) + "\n")
    return ToolContext(project_root=tmp_path, log_path=log)


def test_read_task_log_returns_the_tail(ctx):
    out = read_task_log(ctx, task_id="load", tail_lines=3)
    assert out.strip().splitlines() == ["line 497", "line 498", "line 499"]


def test_read_task_log_is_character_capped(tmp_path):
    log = tmp_path / "load.log"
    log.write_text("x" * 100_000)
    ctx = ToolContext(project_root=tmp_path, log_path=log, max_log_chars=1000)
    assert len(read_task_log(ctx, task_id="load")) <= 1100


def test_read_repo_file_reads_within_the_root(ctx):
    assert "customer_name" in read_repo_file(ctx, "src/dag.py")


@pytest.mark.parametrize("bad", ["../etc/passwd", "/etc/passwd", "src/../../etc/passwd"])
def test_read_repo_file_rejects_paths_outside_the_root(ctx, bad):
    with pytest.raises(ToolError, match="outside the project root"):
        read_repo_file(ctx, bad)


def test_read_repo_file_rejects_a_symlink_escape(ctx, tmp_path):
    outside = tmp_path.parent / "outside.txt"
    outside.write_text("secret")
    (tmp_path / "link.txt").symlink_to(outside)
    with pytest.raises(ToolError, match="outside the project root"):
        read_repo_file(ctx, "link.txt")


def test_propose_patch_stages_an_edit_without_touching_disk(ctx):
    before = (ctx.project_root / "src" / "dag.py").read_text()

    propose_patch(ctx, "src/dag.py", "customer_name", "name", "matches DDL column")

    assert (ctx.project_root / "src" / "dag.py").read_text() == before
    assert len(ctx.edits) == 1
    assert ctx.edits[0].new_str == "name"


def test_propose_patch_rejects_a_string_that_does_not_occur(ctx):
    with pytest.raises(ToolError, match="0 times"):
        propose_patch(ctx, "src/dag.py", "nonexistent", "x", "why")


def test_propose_patch_rejects_an_ambiguous_string(ctx):
    (ctx.project_root / "src" / "dup.py").write_text("a = 1\na = 1\n")
    with pytest.raises(ToolError, match="2 times"):
        propose_patch(ctx, "src/dup.py", "a = 1", "a = 2", "why")


def test_render_diff_produces_a_unified_diff(ctx):
    propose_patch(ctx, "src/dag.py", "customer_name", "name", "matches DDL column")

    diff = render_diff(ctx)

    assert "--- a/src/dag.py" in diff
    assert "+++ b/src/dag.py" in diff
    assert "-record" in diff
    assert '+record = {"id": 1, "name": "x"}' in diff


def test_render_diff_is_empty_with_no_edits(ctx):
    assert render_diff(ctx) == ""


def test_tool_specs_expose_exactly_three_tools():
    assert {spec["name"] for spec in TOOL_SPECS} == {
        "read_task_log",
        "read_repo_file",
        "propose_patch",
    }
```

- [ ] **Step 2: Run test to verify it fails**

Re-bake, then: `uv run pytest tests/test_agentic_tools.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'my_data_project.agentic.tools'`

- [ ] **Step 3: Write the implementation**

Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/tools.py`:

```python
"""The agent's entire capability surface: three tools, no bash, no write.

propose_patch uses str_replace semantics but *stages* rather than applies.
Nothing on disk changes during the agent run, so a bad run costs an unused
.patch file -- and an eval can score a patch that was never applied.
"""

import difflib
from dataclasses import dataclass, field
from pathlib import Path

from {{ cookiecutter.project_slug }}.agentic.contracts import ProposedEdit

DEFAULT_TAIL_LINES = 200
DEFAULT_MAX_LOG_CHARS = 20000


class ToolError(Exception):
    """Raised for any rejected tool call. Returned to the model as a tool error."""


@dataclass
class ToolContext:
    project_root: Path
    log_path: Path
    max_log_chars: int = DEFAULT_MAX_LOG_CHARS
    edits: list[ProposedEdit] = field(default_factory=list)

    def resolve(self, path: str) -> Path:
        """Confine a model-supplied path to the project root.

        The input is model output, so this is load-bearing rather than
        defensive: resolve() collapses `..` and follows symlinks, then the
        containment check rejects anything that escaped.
        """
        root = self.project_root.resolve()
        candidate = (root / path).resolve()
        if not candidate.is_relative_to(root):
            raise ToolError(f"path '{path}' resolves outside the project root")
        return candidate


def read_task_log(ctx: ToolContext, task_id: str, tail_lines: int = DEFAULT_TAIL_LINES) -> str:
    """Return the tail of the failing task's log."""
    if not ctx.log_path.exists():
        raise ToolError(f"no log found for task '{task_id}' at {ctx.log_path}")
    lines = ctx.log_path.read_text(errors="replace").splitlines()
    text = "\n".join(lines[-tail_lines:])
    if len(text) > ctx.max_log_chars:
        text = text[-ctx.max_log_chars :]
    return text


def read_repo_file(ctx: ToolContext, path: str) -> str:
    target = ctx.resolve(path)
    if not target.is_file():
        raise ToolError(f"no such file: {path}")
    return target.read_text(errors="replace")


def propose_patch(
    ctx: ToolContext, path: str, old_str: str, new_str: str, rationale: str
) -> str:
    """Stage a single unambiguous replacement. Does not write to disk."""
    target = ctx.resolve(path)
    if not target.is_file():
        raise ToolError(f"no such file: {path}")
    occurrences = target.read_text().count(old_str)
    if occurrences != 1:
        raise ToolError(
            f"old_str occurs {occurrences} times in {path}; it must occur exactly once. "
            "Include more surrounding context to make it unique."
        )
    ctx.edits.append(
        ProposedEdit(path=path, old_str=old_str, new_str=new_str, rationale=rationale)
    )
    return f"staged edit {len(ctx.edits)} for {path}"


def render_diff(ctx: ToolContext) -> str:
    """Render all staged edits as a unified diff, still without writing."""
    chunks: list[str] = []
    by_path: dict[str, list[ProposedEdit]] = {}
    for edit in ctx.edits:
        by_path.setdefault(edit.path, []).append(edit)

    for path, edits in by_path.items():
        original = ctx.resolve(path).read_text()
        patched = original
        for edit in edits:
            patched = patched.replace(edit.old_str, edit.new_str, 1)
        diff = difflib.unified_diff(
            original.splitlines(keepends=True),
            patched.splitlines(keepends=True),
            fromfile=f"a/{path}",
            tofile=f"b/{path}",
        )
        chunks.append("".join(diff))
    return "".join(chunks)


TOOL_SPECS = [
    {
        "name": "read_task_log",
        "description": (
            "Read the tail of the failing Airflow task's log. Call this first -- "
            "the exception message names the offending field."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "The failing task id."},
                "tail_lines": {
                    "type": "integer",
                    "description": "How many trailing lines to return. Defaults to 200.",
                },
            },
            "required": ["task_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "read_repo_file",
        "description": (
            "Read a source file, relative to the project root. Use this to inspect "
            "the DAG that produced the bad record and the DDL that defines the schema."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path relative to the project root."}
            },
            "required": ["path"],
            "additionalProperties": False,
        },
    },
    {
        "name": "propose_patch",
        "description": (
            "Stage a single replacement in a file. old_str must occur exactly once. "
            "Nothing is written to disk; the staged edits become a reviewable diff."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "old_str": {"type": "string", "description": "Exact text to replace, once."},
                "new_str": {"type": "string", "description": "Replacement text."},
                "rationale": {"type": "string", "description": "Why this fixes the failure."},
            },
            "required": ["path", "old_str", "new_str", "rationale"],
            "additionalProperties": False,
        },
    },
]
```

- [ ] **Step 4: Update the manifest**

Add `"tests/test_agentic_tools.py"` to the `include_agentic` entry's `remove` list.

- [ ] **Step 5: Run tests**

Re-bake, then: `uv run pytest tests/test_agentic_tools.py -v`
Expected: PASS (13 tests).

Then from the repo root: `uv run pytest -m "not slow" -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add "{{cookiecutter.project_slug}}" hooks/
git commit -m "feat: add the three-tool agent allowlist with path confinement"
```

---

### Task 5: The agent runner

**Files:**
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/runner.py`
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/prompts/diagnose.md`
- Create: `{{cookiecutter.project_slug}}/tests/test_agentic_runner.py`
- Modify: `{{cookiecutter.project_slug}}/pyproject.toml`
- Modify: `hooks/post_gen_project.py`

**Interfaces:**
- Consumes: `FailureContext`, `AgentResult`, `AgentDiagnosis` (Task 2); `TraceRecorder` (Task 3); `ToolContext`, `TOOL_SPECS`, `read_task_log`, `read_repo_file`, `propose_patch`, `ToolError` (Task 4)
- Produces:
  - `AgentRunner` protocol: `run(ctx: FailureContext, recorder: TraceRecorder) -> AgentResult`
  - `FakeRunner(diagnosis: AgentDiagnosis, edits: list[ProposedEdit], tool_sequence: list[str])`
  - `ClaudeRunner(model: str = MODEL, max_iterations: int = 12)`
  - `MODEL: str`, `MAX_ITERATIONS: int`
  - `_model_kwargs(model: str) -> dict`
  - `dispatch_tool(tool_ctx, name: str, arguments: dict) -> str`
  - `SYSTEM_PROMPT: str`

- [ ] **Step 1: Write the failing test**

Create `{{cookiecutter.project_slug}}/tests/test_agentic_runner.py`:

```python
import pytest

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    FailureContext,
    ProposedEdit,
)
from {{ cookiecutter.project_slug }}.agentic.runner import (
    MODEL,
    FakeRunner,
    _model_kwargs,
    dispatch_tool,
)
from {{ cookiecutter.project_slug }}.agentic.tools import ToolContext, ToolError
from {{ cookiecutter.project_slug }}.agentic.tracing import TraceRecorder


@pytest.fixture()
def failure_ctx(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "dag.py").write_text('record = {"id": 1, "customer_name": "x"}\n')
    (tmp_path / "load.log").write_text("SchemaDriftError: unexpected field 'customer_name'\n")
    return FailureContext(
        dag_id="example_events_dag",
        dag_run_id="run_1",
        task_id="load",
        try_number=1,
        exception_type="SchemaDriftError",
        exception_message="unexpected field 'customer_name'",
        log_path=str(tmp_path / "load.log"),
        project_root=str(tmp_path),
    )


def test_model_kwargs_uses_temperature_for_haiku():
    kwargs = _model_kwargs("claude-haiku-4-5")
    assert kwargs["temperature"] == 0
    assert "output_config" not in kwargs


def test_model_kwargs_uses_effort_for_opus_and_never_temperature():
    kwargs = _model_kwargs("claude-opus-5")
    assert "temperature" not in kwargs
    assert kwargs["output_config"] == {"effort": "medium"}


def test_model_kwargs_falls_back_conservatively_for_unknown_models():
    # The intersection valid on every model: no sampling params, no effort.
    assert _model_kwargs("some-future-model") == {}


def test_default_model_comes_from_the_cookiecutter_variable():
    assert MODEL == "{{ cookiecutter.agentic_model }}"


def test_dispatch_tool_routes_to_read_task_log(failure_ctx, tmp_path):
    tool_ctx = ToolContext(project_root=tmp_path, log_path=tmp_path / "load.log")
    out = dispatch_tool(tool_ctx, "read_task_log", {"task_id": "load"})
    assert "customer_name" in out


def test_dispatch_tool_rejects_an_unknown_tool(tmp_path):
    tool_ctx = ToolContext(project_root=tmp_path, log_path=tmp_path / "load.log")
    with pytest.raises(ToolError, match="unknown tool"):
        dispatch_tool(tool_ctx, "rm_rf", {})


def test_fake_runner_produces_a_result_and_records_the_tool_sequence(failure_ctx):
    runner = FakeRunner(
        diagnosis=AgentDiagnosis(
            root_cause="field renamed",
            offending_field="customer_name",
            confidence="high",
            patch_summary="rename to name",
        ),
        edits=[
            ProposedEdit(
                path="src/dag.py",
                old_str="customer_name",
                new_str="name",
                rationale="matches DDL",
            )
        ],
        tool_sequence=["read_task_log", "read_repo_file", "propose_patch"],
    )
    recorder = TraceRecorder(
        dag_id=failure_ctx.dag_id,
        dag_run_id=failure_ctx.dag_run_id,
        task_id=failure_ctx.task_id,
        try_number=failure_ctx.try_number,
    )

    result = runner.run(failure_ctx, recorder)

    assert result.diagnosis.offending_field == "customer_name"
    assert [e.name for e in result.elements if e.kind == "tool_call"] == [
        "read_task_log",
        "read_repo_file",
        "propose_patch",
    ]


def test_fake_runner_needs_no_api_key(monkeypatch, failure_ctx):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    runner = FakeRunner(
        diagnosis=AgentDiagnosis(root_cause="x", confidence="low", patch_summary="y"),
        edits=[],
        tool_sequence=[],
    )
    recorder = TraceRecorder(dag_id="d", dag_run_id="r", task_id="t", try_number=1)
    assert runner.run(failure_ctx, recorder).diagnosis.root_cause == "x"
```

- [ ] **Step 2: Run test to verify it fails**

Re-bake, then: `uv run pytest tests/test_agentic_runner.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'my_data_project.agentic.runner'`

- [ ] **Step 3: Write the system prompt**

Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/prompts/diagnose.md`:

```markdown
You are diagnosing a failed Airflow task in a data pipeline.

Work in this order:

1. Call `read_task_log` to read the failing task's log. The exception message
   names the specific problem.
2. Call `read_repo_file` to read the source that produced the bad data, and the
   schema definition it violated. Do not guess at file contents.
3. Call `propose_patch` with the smallest change that fixes the root cause.

Rules:

- Diagnose from evidence you have actually read. Do not infer the contents of a
  file you have not opened.
- Fix the cause, not the symptom. If a field name does not match the schema,
  decide which side is wrong by reading both.
- Keep the patch minimal. Do not refactor, rename unrelated identifiers, add
  error handling, or tidy surrounding code.
- `old_str` must occur exactly once in the target file. Include surrounding
  context to make it unique.
- If you cannot determine the root cause from the log and the source, say so
  and set confidence to "low" rather than proposing a speculative patch.

When finished, return your diagnosis in the required schema.
```

- [ ] **Step 4: Write the implementation**

Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/runner.py`:

```python
"""The AgentRunner seam -- where all non-determinism is isolated.

Above this boundary everything is ordinary Python. FakeRunner returns a
fixture, ClaudeRunner calls the live API, and nothing downstream knows
which one ran. That is what makes the default test suite deterministic:
temperature=0 does not guarantee identical output, this seam does.
"""

import json
from pathlib import Path
from typing import Protocol

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    AgentResult,
    FailureContext,
    ProposedEdit,
)
from {{ cookiecutter.project_slug }}.agentic.tools import (
    TOOL_SPECS,
    ToolContext,
    ToolError,
    propose_patch,
    read_repo_file,
    read_task_log,
)
from {{ cookiecutter.project_slug }}.agentic.tracing import TraceRecorder, truncate

MODEL = "{{ cookiecutter.agentic_model }}"
MAX_ITERATIONS = 12
MAX_TOKENS = 8000

SYSTEM_PROMPT = (Path(__file__).parent / "prompts" / "diagnose.md").read_text()

DIAGNOSIS_SCHEMA = {
    "type": "object",
    "properties": {
        "root_cause": {"type": "string"},
        "offending_field": {"type": ["string", "null"]},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "patch_summary": {"type": "string"},
    },
    "required": ["root_cause", "offending_field", "confidence", "patch_summary"],
    "additionalProperties": False,
}

# Request parameters are tier-dependent, so they cannot be interpolated
# blindly from the cookiecutter variable. temperature is valid on Haiku 4.5
# and returns 400 on claude-opus-5; output_config.effort is the reverse.
# Unknown models get the intersection that is valid everywhere.
_MODEL_PARAMS: dict[str, dict] = {
    "claude-haiku-4-5": {"temperature": 0},
    "claude-sonnet-5": {"output_config": {"effort": "medium"}},
    "claude-opus-5": {"output_config": {"effort": "medium"}},
}


def _model_kwargs(model: str) -> dict:
    return dict(_MODEL_PARAMS.get(model, {}))


def dispatch_tool(tool_ctx: ToolContext, name: str, arguments: dict) -> str:
    """Route one tool call. The allowlist is this dict and nothing else."""
    if name == "read_task_log":
        return read_task_log(
            tool_ctx,
            task_id=arguments["task_id"],
            tail_lines=arguments.get("tail_lines", 200),
        )
    if name == "read_repo_file":
        return read_repo_file(tool_ctx, path=arguments["path"])
    if name == "propose_patch":
        return propose_patch(
            tool_ctx,
            path=arguments["path"],
            old_str=arguments["old_str"],
            new_str=arguments["new_str"],
            rationale=arguments["rationale"],
        )
    raise ToolError(f"unknown tool: {name}")


def render_context(ctx: FailureContext) -> str:
    return (
        f"Task `{ctx.task_id}` in DAG `{ctx.dag_id}` (run `{ctx.dag_run_id}`, "
        f"try {ctx.try_number}) failed.\n\n"
        f"Exception: {ctx.exception_type}: {ctx.exception_message}\n\n"
        "Read the task log and the relevant source, then propose a patch."
    )


class AgentRunner(Protocol):
    def run(self, ctx: FailureContext, recorder: TraceRecorder) -> AgentResult: ...


class FakeRunner:
    """Replays a fixture. The default test tier runs entirely on this."""

    def __init__(
        self,
        diagnosis: AgentDiagnosis,
        edits: list[ProposedEdit],
        tool_sequence: list[str],
    ) -> None:
        self.diagnosis = diagnosis
        self.edits = edits
        self.tool_sequence = tool_sequence

    def run(self, ctx: FailureContext, recorder: TraceRecorder) -> AgentResult:
        for name in self.tool_sequence:
            with recorder.element("tool_call", name) as box:
                box["output_summary"] = {"replayed": True}
        recorder.add(
            "result",
            "diagnosis",
            output_summary={"confidence": self.diagnosis.confidence},
        )
        return AgentResult(
            diagnosis=self.diagnosis,
            edits=list(self.edits),
            elements=list(recorder._elements),
        )


class ClaudeRunner:
    """Drives the real tool loop against the Anthropic API."""

    def __init__(self, model: str = MODEL, max_iterations: int = MAX_ITERATIONS) -> None:
        self.model = model
        self.max_iterations = max_iterations

    def _client(self):
        import anthropic

        return anthropic.Anthropic()

    def run(self, ctx: FailureContext, recorder: TraceRecorder) -> AgentResult:
        client = self._client()
        tool_ctx = ToolContext(
            project_root=Path(ctx.project_root), log_path=Path(ctx.log_path)
        )
        messages: list[dict] = [{"role": "user", "content": render_context(ctx)}]

        for _ in range(self.max_iterations):
            with recorder.element("llm_turn", f"turn_{len(messages)}") as box:
                response = client.messages.create(
                    model=self.model,
                    max_tokens=MAX_TOKENS,
                    system=SYSTEM_PROMPT,
                    tools=TOOL_SPECS,
                    messages=messages,
                    **_model_kwargs(self.model),
                )
                box["input_tokens"] = response.usage.input_tokens
                box["output_tokens"] = response.usage.output_tokens

            if response.stop_reason != "tool_use":
                break

            messages.append({"role": "assistant", "content": response.content})
            results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                with recorder.element(
                    "tool_call", block.name, input_summary=dict(block.input)
                ) as box:
                    try:
                        output = dispatch_tool(tool_ctx, block.name, dict(block.input))
                        is_error = False
                    except ToolError as exc:
                        output = str(exc)
                        is_error = True
                    box["output_summary"] = {"text": truncate(output, 500)}
                results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": truncate(output),
                        "is_error": is_error,
                    }
                )
            messages.append({"role": "user", "content": results})

        diagnosis = self._final_diagnosis(client, messages, recorder)
        return AgentResult(
            diagnosis=diagnosis,
            edits=list(tool_ctx.edits),
            elements=list(recorder._elements),
        )

    def _final_diagnosis(self, client, messages: list[dict], recorder) -> AgentDiagnosis:
        """One schema-constrained turn, so parsing cannot fail."""
        with recorder.element("llm_turn", "diagnosis") as box:
            response = client.messages.create(
                model=self.model,
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                messages=messages
                + [
                    {
                        "role": "user",
                        "content": "Return your final diagnosis in the required schema.",
                    }
                ],
                output_config={"format": {"type": "json_schema", "schema": DIAGNOSIS_SCHEMA}},
            )
            box["input_tokens"] = response.usage.input_tokens
            box["output_tokens"] = response.usage.output_tokens
        text = next(b.text for b in response.content if b.type == "text")
        parsed = AgentDiagnosis.model_validate(json.loads(text))
        recorder.add("result", "diagnosis", output_summary={"confidence": parsed.confidence})
        return parsed
```

Note the final turn passes `output_config` unconditionally because structured outputs are supported on every model in `_MODEL_PARAMS`, including Haiku 4.5. It does **not** merge `_model_kwargs`, which would collide on `output_config` for the Opus/Sonnet entries.

- [ ] **Step 5: Add the anthropic dependency**

In `{{cookiecutter.project_slug}}/pyproject.toml`, extend the agentic block added in Task 2:

```
{%- if cookiecutter.include_agentic %}
    "pydantic>=2.7",
    "anthropic>=0.40",
{%- endif %}
```

- [ ] **Step 6: Update the manifest**

Add `"tests/test_agentic_runner.py"` to the `include_agentic` entry's `remove` list. `gated` is unchanged — `runner.py` interpolates `cookiecutter.agentic_model`, not `cookiecutter.include_agentic`, and the manifest test matches on the flag name only.

- [ ] **Step 7: Run tests**

Re-bake, then: `uv run pytest tests/test_agentic_runner.py -v`
Expected: PASS (9 tests).

- [ ] **Step 8: Commit**

```bash
git add "{{cookiecutter.project_slug}}" hooks/
git commit -m "feat: add AgentRunner seam with fake and Claude implementations"
```

---

### Task 6: Patch publishing

**Files:**
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/publisher.py`
- Create: `{{cookiecutter.project_slug}}/tests/test_agentic_publisher.py`
- Modify: `hooks/post_gen_project.py`

**Interfaces:**
- Consumes: `AgentResult` (Task 2), `ToolContext`, `render_diff` (Task 4)
- Produces:
  - `PublishedPatch(patch_path: str | None, metadata_path: str, title: str, body: str, url: str | None)`
  - `PRPublisher` protocol: `publish(trace_id, result, tool_ctx) -> PublishedPatch`
  - `LocalPatchPublisher(artifacts_dir: Path = Path("artifacts"))`
  - `GitHubPublisher(repo: str, base_branch: str = "main")`
  - `default_publisher() -> PRPublisher`

- [ ] **Step 1: Write the failing test**

Create `{{cookiecutter.project_slug}}/tests/test_agentic_publisher.py`:

```python
import json

import pytest

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    AgentResult,
    ProposedEdit,
)
from {{ cookiecutter.project_slug }}.agentic.publisher import (
    LocalPatchPublisher,
    default_publisher,
)
from {{ cookiecutter.project_slug }}.agentic.tools import ToolContext, propose_patch


@pytest.fixture()
def tool_ctx(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "dag.py").write_text('record = {"id": 1, "customer_name": "x"}\n')
    ctx = ToolContext(project_root=tmp_path, log_path=tmp_path / "load.log")
    propose_patch(ctx, "src/dag.py", "customer_name", "name", "matches DDL column")
    return ctx


@pytest.fixture()
def result():
    return AgentResult(
        diagnosis=AgentDiagnosis(
            root_cause="extract() emits customer_name but the DDL declares name",
            offending_field="customer_name",
            confidence="high",
            patch_summary="rename customer_name to name in extract()",
        ),
        edits=[
            ProposedEdit(
                path="src/dag.py",
                old_str="customer_name",
                new_str="name",
                rationale="matches DDL column",
            )
        ],
        elements=[],
    )


def test_local_publisher_writes_a_patch_file(tmp_path, tool_ctx, result):
    published = LocalPatchPublisher(tmp_path).publish("trace1", result, tool_ctx)

    patch = (tmp_path / "trace1.patch").read_text()
    assert published.patch_path.endswith("trace1.patch")
    assert "--- a/src/dag.py" in patch
    assert '+record = {"id": 1, "name": "x"}' in patch


def test_local_publisher_writes_reviewable_metadata(tmp_path, tool_ctx, result):
    published = LocalPatchPublisher(tmp_path).publish("trace1", result, tool_ctx)

    meta = json.loads((tmp_path / "trace1.json").read_text())
    assert meta["title"] == published.title
    assert "customer_name" in meta["body"]
    assert meta["confidence"] == "high"
    assert meta["trace_id"] == "trace1"


def test_local_publisher_opens_no_pull_request(tmp_path, tool_ctx, result):
    published = LocalPatchPublisher(tmp_path).publish("trace1", result, tool_ctx)
    assert published.url is None


def test_local_publisher_handles_a_run_with_no_edits(tmp_path, result):
    empty_ctx = ToolContext(project_root=tmp_path, log_path=tmp_path / "load.log")
    result.edits = []

    published = LocalPatchPublisher(tmp_path).publish("trace2", result, empty_ctx)

    assert published.patch_path is None
    assert not (tmp_path / "trace2.patch").exists()
    assert (tmp_path / "trace2.json").is_file()


def test_default_publisher_is_local_without_configuration(monkeypatch):
    monkeypatch.delenv("AGENTIC_GITHUB_REPO", raising=False)
    assert isinstance(default_publisher(), LocalPatchPublisher)
```

- [ ] **Step 2: Run test to verify it fails**

Re-bake, then: `uv run pytest tests/test_agentic_publisher.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'my_data_project.agentic.publisher'`

- [ ] **Step 3: Write the implementation**

Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/publisher.py`:

```python
"""Turning an AgentResult into something a human can review.

LocalPatchPublisher is the default because a freshly generated scaffold
usually has no remote, and a first run that tries to push is a bad first
impression. The patch artifact is also exactly what the evals score, so
the diff is the assertable output either way.
"""

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from {{ cookiecutter.project_slug }}.agentic.contracts import AgentResult
from {{ cookiecutter.project_slug }}.agentic.tools import ToolContext, render_diff

DEFAULT_ARTIFACTS_DIR = Path("artifacts")


@dataclass
class PublishedPatch:
    metadata_path: str
    title: str
    body: str
    patch_path: str | None = None
    url: str | None = None


def _title(result: AgentResult) -> str:
    return f"fix: {result.diagnosis.patch_summary}"


def _body(trace_id: str, result: AgentResult) -> str:
    lines = [
        "Proposed automatically by the agentic healing DAG. **Review before merging.**",
        "",
        f"**Root cause:** {result.diagnosis.root_cause}",
        f"**Confidence:** {result.diagnosis.confidence}",
    ]
    if result.diagnosis.offending_field:
        lines.append(f"**Offending field:** `{result.diagnosis.offending_field}`")
    lines += ["", "**Edits:**"]
    for edit in result.edits:
        lines.append(f"- `{edit.path}` — {edit.rationale}")
    lines += ["", f"Trace id: `{trace_id}`"]
    return "\n".join(lines)


class PRPublisher(Protocol):
    def publish(
        self, trace_id: str, result: AgentResult, tool_ctx: ToolContext
    ) -> PublishedPatch: ...


class LocalPatchPublisher:
    """Writes a .patch plus metadata. Requires no credentials."""

    def __init__(self, artifacts_dir: Path = DEFAULT_ARTIFACTS_DIR) -> None:
        self.artifacts_dir = Path(artifacts_dir)

    def publish(
        self, trace_id: str, result: AgentResult, tool_ctx: ToolContext
    ) -> PublishedPatch:
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        title = _title(result)
        body = _body(trace_id, result)

        patch_path: str | None = None
        diff = render_diff(tool_ctx)
        if diff:
            target = self.artifacts_dir / f"{trace_id}.patch"
            target.write_text(diff)
            patch_path = str(target)

        metadata = {
            "trace_id": trace_id,
            "title": title,
            "body": body,
            "confidence": result.diagnosis.confidence,
            "root_cause": result.diagnosis.root_cause,
            "offending_field": result.diagnosis.offending_field,
            "patch_path": patch_path,
            "edits": [e.model_dump() for e in result.edits],
        }
        metadata_path = self.artifacts_dir / f"{trace_id}.json"
        metadata_path.write_text(json.dumps(metadata, indent=2))

        return PublishedPatch(
            metadata_path=str(metadata_path),
            title=title,
            body=body,
            patch_path=patch_path,
        )


class GitHubPublisher:
    """Opt-in: branches, applies, pushes, and opens a PR via the gh CLI.

    Enable by setting AGENTIC_GITHUB_REPO. Requires push credentials and an
    authenticated `gh` in the Airflow worker -- which is exactly why it is
    not the default.
    """

    def __init__(self, repo: str, base_branch: str = "main") -> None:
        self.repo = repo
        self.base_branch = base_branch

    def publish(
        self, trace_id: str, result: AgentResult, tool_ctx: ToolContext
    ) -> PublishedPatch:
        local = LocalPatchPublisher(tool_ctx.project_root / "artifacts").publish(
            trace_id, result, tool_ctx
        )
        if local.patch_path is None:
            return local

        root = tool_ctx.project_root
        branch = f"agentic/heal-{trace_id[:8]}"
        run = lambda *args: subprocess.run(args, cwd=root, check=True, capture_output=True)
        run("git", "checkout", "-b", branch)
        run("git", "apply", local.patch_path)
        run("git", "commit", "-am", local.title)
        run("git", "push", "-u", "origin", branch)
        created = subprocess.run(
            [
                "gh", "pr", "create",
                "--repo", self.repo,
                "--base", self.base_branch,
                "--head", branch,
                "--title", local.title,
                "--body", local.body,
            ],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        local.url = created.stdout.strip()
        return local


def default_publisher() -> PRPublisher:
    repo = os.environ.get("AGENTIC_GITHUB_REPO")
    if repo:
        return GitHubPublisher(repo)
    return LocalPatchPublisher()
```

If ruff flags the `run = lambda ...` assignment (rule E731), replace it with a nested `def run(*args): ...`.

- [ ] **Step 4: Update the manifest**

Add `"tests/test_agentic_publisher.py"` to the `include_agentic` entry's `remove` list.

- [ ] **Step 5: Run tests**

Re-bake, then: `uv run pytest tests/test_agentic_publisher.py -v`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add "{{cookiecutter.project_slug}}" hooks/
git commit -m "feat: publish agent patches as reviewable local artifacts"
```

---

### Task 7: The schema-drift failure

Makes the demo DAG fail on demand, with an error message derived from the DDL rather than hardcoded — so the runtime error and the eval's ground truth share one source.

**Files:**
- Modify: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/writer.py`
- Modify: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/dags/example_dag.py`
- Modify: `{{cookiecutter.project_slug}}/tests/test_lakehouse_writer.py`
- Modify: `{{cookiecutter.project_slug}}/tests/test_batch_dag.py`
- Modify: `hooks/post_gen_project.py`

**Interfaces:**
- Consumes: nothing from earlier tasks
- Produces:
  - `SchemaDriftError(Exception)` in `lakehouse/writer.py`
  - `declared_columns() -> list[str]` in `lakehouse/writer.py`
  - `example_events_dag` accepts `params={"failure_mode": ...}` with values `"none"` and `"schema_drift"`

- [ ] **Step 1: Write the failing test**

Append to `{{cookiecutter.project_slug}}/tests/test_lakehouse_writer.py`:

```python
import pytest

from {{ cookiecutter.project_slug }}.lakehouse.writer import (
    SchemaDriftError,
    declared_columns,
    get_connection,
    write_events,
)


def test_declared_columns_are_parsed_from_the_ddl():
    assert declared_columns() == ["id", "name", "ingested_at"]


def test_write_events_rejects_an_undeclared_field(tmp_path):
    con = get_connection(str(tmp_path / "lakehouse.duckdb"))

    with pytest.raises(SchemaDriftError) as excinfo:
        write_events(con, [{"id": 1, "customer_name": "example"}])

    message = str(excinfo.value)
    assert "customer_name" in message
    assert "name" in message


def test_write_events_accepts_a_conforming_record(tmp_path):
    con = get_connection(str(tmp_path / "lakehouse.duckdb"))
    write_events(con, [{"id": 1, "name": "example"}])
    assert con.execute("SELECT count(*) FROM events").fetchone()[0] == 1
```

Replace `{{cookiecutter.project_slug}}/tests/test_batch_dag.py` entirely:

```python
import pytest

from {{ cookiecutter.project_slug }}.batch.dags.example_dag import example_events_dag


def test_dag_has_no_import_errors():
    dag = example_events_dag()
    assert dag.dag_id == "example_events_dag"
    assert {"extract", "load"} <= {task.task_id for task in dag.tasks}


def test_dag_exposes_a_failure_mode_param():
    dag = example_events_dag()
    assert "failure_mode" in dag.params


@pytest.mark.parametrize("mode", ["none", "schema_drift"])
def test_failure_mode_accepts_both_values(mode):
    dag = example_events_dag()
    dag.params["failure_mode"].value = mode
    assert dag.params["failure_mode"].value == mode
```

The task-set assertion is a subset check rather than a count, so it stays true
in both flag states. Task 8 adds the heal-task assertions on top of it — this
task must not commit a knowingly-red test.

- [ ] **Step 2: Run tests to verify they fail**

Re-bake, then: `uv run pytest tests/test_lakehouse_writer.py tests/test_batch_dag.py -v`
Expected: FAIL — `ImportError: cannot import name 'SchemaDriftError'`, and the DAG tests fail on missing `params` / task count.

- [ ] **Step 3: Add DDL-derived validation to the writer**

Replace `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/writer.py`:

```python
import re
from pathlib import Path

import duckdb

DDL_PATH = Path(__file__).parent / "create_tables.sql"

# create_tables.sql is the schema's only hand-authored source (the post-gen
# hook also inlines it into dbt's on-run-start). Deriving the validation
# from it means the runtime error and the eval's ground truth cannot drift.
_COLUMN_RE = re.compile(r"^\s*(\w+)\s+\w+", re.MULTILINE)


class SchemaDriftError(Exception):
    """A record carries a field the events table does not declare."""


def declared_columns() -> list[str]:
    body = DDL_PATH.read_text().split("(", 1)[1].rsplit(")", 1)[0]
    return [m.group(1) for m in _COLUMN_RE.finditer(body)]


def get_connection(db_path: str = "lakehouse.duckdb") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
    con.execute(DDL_PATH.read_text())
    return con


def validate(records: list[dict]) -> None:
    columns = declared_columns()
    for record in records:
        unexpected = [key for key in record if key not in columns]
        if unexpected:
            raise SchemaDriftError(
                f"record carries undeclared field(s) {unexpected}; "
                f"the events table declares columns {columns}. "
                "Either the producer renamed a field or the DDL is out of date."
            )


def write_events(con: duckdb.DuckDBPyConnection, records: list[dict]) -> None:
    validate(records)
    for record in records:
        con.execute(
            "INSERT INTO events (id, name) VALUES (?, ?)",
            [record["id"], record["name"]],
        )
```

- [ ] **Step 4: Add the failure param to the demo DAG**

Replace `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/dags/example_dag.py`:

```python
from datetime import datetime

from airflow.sdk import Param, dag, task

from {{ cookiecutter.project_slug }}.batch.sinks import write_events

# Off by default: the shipped demo behaves exactly as it did before the
# agentic Pattern existed. Set failure_mode=schema_drift to trigger the
# healing task.
FAILURE_MODES = ["none", "schema_drift"]


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"failure_mode": Param("none", type="string", enum=FAILURE_MODES)},
)
def example_events_dag():
    @task
    def extract(**context) -> list[dict]:
        mode = context["params"]["failure_mode"]
        if mode == "schema_drift":
            # `customer_name` is not a column in lakehouse/create_tables.sql.
            return [{"id": 1, "customer_name": "example"}]
        return [{"id": 1, "name": "example"}]

    @task
    def load(records: list[dict]) -> None:
        write_events(records)

    load(extract())


example_events_dag()
```

- [ ] **Step 5: Update the manifest**

No change. Neither `example_dag.py` nor `test_batch_dag.py` contains a `cookiecutter.include_agentic` conditional after this task — the failure param and the DDL validation are unconditional. Both files gain their gate in Task 8, and the manifest is updated there.

The `include_agentic` entry's `gated` list therefore stays:

```python
        "gated": [
            "pyproject.toml",
        ],
```

Declaring a file in `gated` before it actually contains the flag reference fails `test_pattern_manifest.py`.

- [ ] **Step 6: Run tests**

Re-bake, then: `uv run pytest tests/test_lakehouse_writer.py tests/test_batch_dag.py -v`
Expected: PASS, all six tests.

Then from the repo root: `uv run pytest -m "not slow" -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add "{{cookiecutter.project_slug}}" hooks/
git commit -m "feat: add DDL-derived schema drift validation and failure_mode param"
```

---

### Task 8: The healing DAG task

Wires everything together and closes the failing assertion left by Task 7.

**Files:**
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/dags/__init__.py`
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/dags/healing_dag.py`
- Create: `{{cookiecutter.project_slug}}/tests/test_agentic_healing.py`
- Modify: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/dags/example_dag.py`
- Modify: `hooks/post_gen_project.py`

**Interfaces:**
- Consumes: everything from Tasks 2–7
- Produces:
  - `build_failure_context(context: dict, root: Path, log_path: Path | None = None) -> FailureContext`
  - `run_healing(ctx: FailureContext, runner=None, sink=None, publisher=None) -> tuple[Trace, PublishedPatch]`
  - `heal_on_failure` — an Airflow `@task` calling `run_healing`
  - `project_root() -> Path`

- [ ] **Step 1: Write the failing test**

Create `{{cookiecutter.project_slug}}/tests/test_agentic_healing.py`:

```python
import json

import pytest

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    FailureContext,
    ProposedEdit,
)
from {{ cookiecutter.project_slug }}.agentic.dags.healing_dag import (
    build_failure_context,
    run_healing,
)
from {{ cookiecutter.project_slug }}.agentic.publisher import LocalPatchPublisher
from {{ cookiecutter.project_slug }}.agentic.runner import FakeRunner
from {{ cookiecutter.project_slug }}.agentic.tracing import JsonlSink


@pytest.fixture()
def project(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "dag.py").write_text('record = {"id": 1, "customer_name": "x"}\n')
    (tmp_path / "load.log").write_text(
        "SchemaDriftError: record carries undeclared field(s) ['customer_name']\n"
    )
    return tmp_path


@pytest.fixture()
def failure_ctx(project):
    return FailureContext(
        dag_id="example_events_dag",
        dag_run_id="run_1",
        task_id="load",
        try_number=1,
        exception_type="SchemaDriftError",
        exception_message="record carries undeclared field(s) ['customer_name']",
        log_path=str(project / "load.log"),
        project_root=str(project),
    )


@pytest.fixture()
def runner():
    return FakeRunner(
        diagnosis=AgentDiagnosis(
            root_cause="extract() emits customer_name; the DDL declares name",
            offending_field="customer_name",
            confidence="high",
            patch_summary="rename customer_name to name",
        ),
        edits=[
            ProposedEdit(
                path="src/dag.py",
                old_str="customer_name",
                new_str="name",
                rationale="matches DDL column",
            )
        ],
        tool_sequence=["read_task_log", "read_repo_file", "propose_patch"],
    )


def test_build_failure_context_reads_the_airflow_context(project):
    class FakeTaskInstance:
        dag_id = "example_events_dag"
        task_id = "load"
        try_number = 2
        log_url = "http://localhost/log"

    context = {
        "task_instance": FakeTaskInstance(),
        "dag_run": type("R", (), {"run_id": "run_9"})(),
        "exception": ValueError("boom"),
    }

    ctx = build_failure_context(context, project, log_path=project / "load.log")

    assert ctx.dag_id == "example_events_dag"
    assert ctx.dag_run_id == "run_9"
    assert ctx.try_number == 2
    assert ctx.exception_type == "ValueError"
    assert ctx.exception_message == "boom"


def test_run_healing_produces_a_trace_and_a_patch(project, failure_ctx, runner):
    trace, published = run_healing(
        failure_ctx,
        runner=runner,
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    assert trace.status == "ok"
    assert (project / "traces" / f"{trace.trace_id}.json").is_file()
    assert (project / "artifacts" / f"{trace.trace_id}.patch").is_file()
    assert published.title.startswith("fix:")


def test_run_healing_records_the_tool_sequence_in_order(project, failure_ctx, runner):
    trace, _ = run_healing(
        failure_ctx,
        runner=runner,
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    elements = json.loads((project / "traces" / f"{trace.trace_id}.json").read_text())
    tool_calls = [e["name"] for e in elements if e["kind"] == "tool_call"]
    assert tool_calls == ["read_task_log", "read_repo_file", "propose_patch"]
    assert elements[-1]["kind"] == "agent_run"


def test_run_healing_writes_a_terminal_error_trace_when_the_agent_raises(
    project, failure_ctx
):
    class ExplodingRunner:
        def run(self, ctx, recorder):
            raise RuntimeError("model unavailable")

    with pytest.raises(RuntimeError):
        run_healing(
            failure_ctx,
            runner=ExplodingRunner(),
            sink=JsonlSink(project / "traces"),
            publisher=LocalPatchPublisher(project / "artifacts"),
        )

    written = list((project / "traces").glob("*.json"))
    assert len(written) == 1
    elements = json.loads(written[0].read_text())
    assert elements[-1]["kind"] == "agent_run"
    assert elements[-1]["status"] == "error"


def test_run_healing_survives_a_failing_trace_sink(project, failure_ctx, runner, caplog):
    class ExplodingSink:
        def write(self, trace):
            raise RuntimeError("table agent_traces does not exist")

    trace, published = run_healing(
        failure_ctx,
        runner=runner,
        sink=ExplodingSink(),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    assert published.patch_path is not None
    assert "failed to persist agent trace" in caplog.text
```

Also append the heal-task wiring assertions to `{{cookiecutter.project_slug}}/tests/test_batch_dag.py`. These are the spec's "heal task fires on failure, is skipped on success" coverage, asserted through the trigger rule rather than a full `dag.test()` run — a real DAG run needs an initialized Airflow metadata DB, which would make the default suite slow and stateful:

```python
{%- if cookiecutter.include_agentic %}


def test_heal_task_is_wired_downstream_of_load():
    dag = example_events_dag()
    assert "heal_on_failure" in {task.task_id for task in dag.tasks}
    heal = dag.get_task("heal_on_failure")
    assert "load" in heal.upstream_task_ids


def test_heal_task_runs_only_when_load_fails():
    from airflow.utils.trigger_rule import TriggerRule

    dag = example_events_dag()
    assert dag.get_task("heal_on_failure").trigger_rule == TriggerRule.ALL_FAILED
{%- endif %}
```

- [ ] **Step 2: Run test to verify it fails**

Re-bake, then: `uv run pytest tests/test_agentic_healing.py tests/test_batch_dag.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'my_data_project.agentic.dags'`

- [ ] **Step 3: Write the healing module**

Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/dags/__init__.py` as an empty file.

Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/agentic/dags/healing_dag.py`:

```python
"""The only Airflow-aware module in the agentic Pattern.

Keeping the import here is what lets every other module be unit-tested
with plain pytest and no scheduler. run_healing() takes its collaborators
as arguments so the whole loop is exercisable without Airflow at all.
"""

import logging
from pathlib import Path

from airflow.sdk import task

from {{ cookiecutter.project_slug }}.agentic.contracts import FailureContext, Trace
from {{ cookiecutter.project_slug }}.agentic.publisher import (
    PublishedPatch,
    default_publisher,
)
from {{ cookiecutter.project_slug }}.agentic.runner import ClaudeRunner
from {{ cookiecutter.project_slug }}.agentic.tools import ToolContext
from {{ cookiecutter.project_slug }}.agentic.tracing import (
    TraceRecorder,
    default_sink,
    write_trace_safely,
)

logger = logging.getLogger(__name__)


def project_root() -> Path:
    """Walk up from this module to the generated project's root."""
    return Path(__file__).resolve().parents[4]


def build_failure_context(
    context: dict, root: Path, log_path: Path | None = None
) -> FailureContext:
    ti = context["task_instance"]
    exception = context.get("exception")
    resolved_log = log_path or root / "logs" / f"{ti.dag_id}" / f"{ti.task_id}.log"
    return FailureContext(
        dag_id=ti.dag_id,
        dag_run_id=context["dag_run"].run_id,
        task_id=ti.task_id,
        try_number=ti.try_number,
        exception_type=type(exception).__name__ if exception else "UnknownError",
        exception_message=str(exception) if exception else "",
        log_path=str(resolved_log),
        project_root=str(root),
    )


def run_healing(
    ctx: FailureContext,
    runner=None,
    sink=None,
    publisher=None,
) -> tuple[Trace, PublishedPatch]:
    """Diagnose, trace, and publish. Collaborators are injectable for tests."""
    runner = runner or ClaudeRunner()
    sink = sink or default_sink()
    publisher = publisher or default_publisher()

    recorder = TraceRecorder(
        dag_id=ctx.dag_id,
        dag_run_id=ctx.dag_run_id,
        task_id=ctx.task_id,
        try_number=ctx.try_number,
    )
    tool_ctx = ToolContext(project_root=Path(ctx.project_root), log_path=Path(ctx.log_path))

    try:
        result = runner.run(ctx, recorder)
    except Exception:
        # finish() in the failure path too, so a crashed run still yields a
        # well-formed array ending in status "error".
        write_trace_safely(sink, recorder.finish("error"))
        raise

    tool_ctx.edits = list(result.edits)
    trace = recorder.finish("ok")
    write_trace_safely(sink, trace)
    published = publisher.publish(trace.trace_id, result, tool_ctx)
    logger.info(
        "healing run %s: %s (confidence=%s) -> %s",
        trace.trace_id,
        result.diagnosis.patch_summary,
        result.diagnosis.confidence,
        published.patch_path or "no patch",
    )
    return trace, published


@task
def heal_on_failure(**context) -> str:
    """Runs only when an upstream task failed (trigger_rule=ALL_FAILED)."""
    ctx = build_failure_context(context, project_root())
    trace, published = run_healing(ctx)
    return published.patch_path or trace.trace_id
```

- [ ] **Step 4: Wire the heal task into the demo DAG**

In `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/dags/example_dag.py`, add the gated import after the existing one:

```python
from {{ cookiecutter.project_slug }}.batch.sinks import write_events
{%- if cookiecutter.include_agentic %}
from airflow.utils.trigger_rule import TriggerRule

from {{ cookiecutter.project_slug }}.agentic.dags.healing_dag import heal_on_failure
{%- endif %}
```

And replace the DAG body's final wiring line:

```python
    loaded = load(extract())
{%- if cookiecutter.include_agentic %}
    heal_on_failure.override(trigger_rule=TriggerRule.ALL_FAILED)() << loaded
{%- endif %}
```

- [ ] **Step 5: Update the manifest**

`example_dag.py` now branches on the flag. The `include_agentic` entry becomes:

```python
    {
        "flag_name": "include_agentic",
        "flag": {{ cookiecutter.include_agentic }},
        "remove": [
            f"src/{SLUG}/agentic",
            "tests/test_agentic_contracts.py",
            "tests/test_agentic_tracing.py",
            "tests/test_agentic_tools.py",
            "tests/test_agentic_runner.py",
            "tests/test_agentic_publisher.py",
            "tests/test_agentic_healing.py",
        ],
        "gated": [
            "pyproject.toml",
            f"src/{SLUG}/batch/dags/example_dag.py",
            "tests/test_batch_dag.py",
        ],
    },
```

- [ ] **Step 6: Run all generated tests**

Re-bake, then: `uv run pytest -v`
Expected: PASS, including `test_batch_dag.py::test_dag_has_no_import_errors` with three tasks.

- [ ] **Step 7: Verify the flag-off bake still works**

```bash
rm -rf /tmp/scaffold-noagent && mkdir -p /tmp/scaffold-noagent
uv run cookiecutter . --no-input --output-dir /tmp/scaffold-noagent include_agentic=false
cd /tmp/scaffold-noagent/my_data_project && uv sync && uv run pytest -v
```

Expected: PASS with two DAG tasks and no agentic tests collected.

- [ ] **Step 8: Run the template suite**

Run: `uv run pytest -m "not slow" -v`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add "{{cookiecutter.project_slug}}" hooks/
git commit -m "feat: trigger the healing task on batch DAG failure"
```

---

### Task 9: Eval harness — cases, replay, scoring

**Files:**
- Create: `{{cookiecutter.project_slug}}/evals/__init__.py`
- Create: `{{cookiecutter.project_slug}}/evals/cases/schema_drift.yaml`
- Create: `{{cookiecutter.project_slug}}/evals/harness.py`
- Create: `{{cookiecutter.project_slug}}/evals/replay.py`
- Create: `{{cookiecutter.project_slug}}/evals/cassettes/schema_drift.json`
- Create: `{{cookiecutter.project_slug}}/evals/test_cases.py`
- Modify: `{{cookiecutter.project_slug}}/pyproject.toml`
- Modify: `hooks/post_gen_project.py`

**Interfaces:**
- Consumes: `AgentResult`, `Trace` (Task 2); `FakeRunner` (Task 5); `run_healing` (Task 8)
- Produces:
  - `EvalCase(id, failure_mode, expect: dict, score: dict)` and `load_cases() -> list[EvalCase]`
  - `score_case(case: EvalCase, trace: Trace, published: PublishedPatch) -> dict` returning `{"case_id": str, "required": {name: bool}, "advisory": {name: bool}, "passed": bool}`
  - `ReplayRunner(cassette_path: Path)` satisfying `AgentRunner`
  - `requires_api_key` — a pytest skip marker helper

- [ ] **Step 1: Write the failing test**

Create `{{cookiecutter.project_slug}}/evals/test_cases.py`:

```python
import os

import pytest

from evals.harness import load_cases, score_case
from evals.replay import ReplayRunner, cassette_for
from {{ cookiecutter.project_slug }}.agentic.dags.healing_dag import run_healing
from {{ cookiecutter.project_slug }}.agentic.publisher import LocalPatchPublisher
from {{ cookiecutter.project_slug }}.agentic.runner import ClaudeRunner
from {{ cookiecutter.project_slug }}.agentic.tracing import JsonlSink

from .conftest import build_case_project

requires_api_key = pytest.mark.skipif(
    not os.environ.get("ANTHROPIC_API_KEY"),
    reason="ANTHROPIC_API_KEY is not set",
)

CASES = load_cases()
CASE_IDS = [case.id for case in CASES]


def test_at_least_one_case_is_defined():
    assert CASES, "evals/cases/ contains no YAML cases"


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_case_declares_required_and_advisory_criteria(case):
    assert case.score["required"], f"{case.id} gates on nothing"
    overlap = set(case.score["required"]) & set(case.score["advisory"])
    assert not overlap, f"{case.id} lists {overlap} as both required and advisory"


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_case_passes_under_replay(case, tmp_path):
    project, ctx = build_case_project(tmp_path, case)
    trace, published = run_healing(
        ctx,
        runner=ReplayRunner(cassette_for(case.id)),
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    scored = score_case(case, trace, published)

    assert scored["passed"], scored["required"]


@requires_api_key
@pytest.mark.live
@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_case_passes_against_the_live_model(case, tmp_path):
    project, ctx = build_case_project(tmp_path, case)
    trace, published = run_healing(
        ctx,
        runner=ClaudeRunner(),
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    scored = score_case(case, trace, published)

    assert scored["passed"], scored["required"]
```

Create `{{cookiecutter.project_slug}}/evals/conftest.py`:

```python
"""Builds a throwaway project tree matching a case's failure_mode."""

from pathlib import Path

from {{ cookiecutter.project_slug }}.agentic.contracts import FailureContext

DAG_SOURCE = '''from airflow.sdk import dag, task


@dag()
def example_events_dag():
    @task
    def extract() -> list[dict]:
        return [{"id": 1, "customer_name": "example"}]
'''

DDL_SOURCE = """CREATE TABLE IF NOT EXISTS events (
    id BIGINT,
    name VARCHAR,
    ingested_at TIMESTAMP DEFAULT now()
);
"""

LOG_SOURCE = (
    "[2026-07-29T00:00:00] INFO - Task load starting\\n"
    "[2026-07-29T00:00:01] ERROR - Task failed with exception\\n"
    "Traceback (most recent call last):\\n"
    "SchemaDriftError: record carries undeclared field(s) ['customer_name']; "
    "the events table declares columns ['id', 'name', 'ingested_at']. "
    "Either the producer renamed a field or the DDL is out of date.\\n"
)


def build_case_project(tmp_path: Path, case) -> tuple[Path, FailureContext]:
    dag_dir = tmp_path / "src" / "pkg" / "batch" / "dags"
    dag_dir.mkdir(parents=True)
    (dag_dir / "example_dag.py").write_text(DAG_SOURCE)

    ddl_dir = tmp_path / "src" / "pkg" / "lakehouse"
    ddl_dir.mkdir(parents=True)
    (ddl_dir / "create_tables.sql").write_text(DDL_SOURCE)

    (tmp_path / "load.log").write_text(LOG_SOURCE)

    ctx = FailureContext(
        dag_id="example_events_dag",
        dag_run_id=f"eval_{case.id}",
        task_id="load",
        try_number=1,
        exception_type="SchemaDriftError",
        exception_message="record carries undeclared field(s) ['customer_name']",
        log_path=str(tmp_path / "load.log"),
        project_root=str(tmp_path),
    )
    return tmp_path, ctx
```

- [ ] **Step 2: Run test to verify it fails**

Re-bake, then: `cd /tmp/scaffold-dev/my_data_project && uv run pytest evals/ -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'evals.harness'`

- [ ] **Step 3: Write the case file**

Create `{{cookiecutter.project_slug}}/evals/cases/schema_drift.yaml`:

```yaml
id: schema_drift
failure_mode: schema_drift
expect:
  # Order-sensitive: this is what proves the agent read the log before
  # proposing a patch, rather than pattern-matching the exception name.
  tools_called: [read_task_log, read_repo_file, propose_patch]
  diagnosis_mentions: [customer_name, name]
  patch_targets: src/pkg/batch/dags/example_dag.py
  min_confidence: medium
score:
  # required gates the eval; advisory reports only. patch_targets is
  # objectively checkable. diagnosis_mentions is keyword matching on prose:
  # useful signal, poor gate.
  required: [tools_called, patch_targets]
  advisory: [diagnosis_mentions, min_confidence]
```

- [ ] **Step 4: Write the harness**

Create `{{cookiecutter.project_slug}}/evals/__init__.py` as an empty file.

Create `{{cookiecutter.project_slug}}/evals/harness.py`:

```python
"""Cases are data: adding a case means adding a YAML file, not writing a test.

That matters in a generated project, whose failure cases will differ from
the ones this template ships.
"""

import json
from dataclasses import dataclass
from pathlib import Path

import yaml

CASES_DIR = Path(__file__).parent / "cases"
CONFIDENCE_ORDER = ["low", "medium", "high"]


@dataclass
class EvalCase:
    id: str
    failure_mode: str
    expect: dict
    score: dict


def load_cases() -> list[EvalCase]:
    cases = []
    for path in sorted(CASES_DIR.glob("*.yaml")):
        raw = yaml.safe_load(path.read_text())
        cases.append(
            EvalCase(
                id=raw["id"],
                failure_mode=raw["failure_mode"],
                expect=raw["expect"],
                score=raw["score"],
            )
        )
    return cases


def _tools_called(trace, expected: list[str]) -> bool:
    actual = [e.name for e in trace.elements if e.kind == "tool_call"]
    return actual == expected


def _patch_targets(published, expected: str) -> bool:
    if published.patch_path is None:
        return False
    meta = json.loads(Path(published.metadata_path).read_text())
    return any(edit["path"] == expected for edit in meta["edits"])


def _diagnosis_mentions(published, expected: list[str]) -> bool:
    meta = json.loads(Path(published.metadata_path).read_text())
    haystack = (meta["root_cause"] + " " + meta["body"]).lower()
    return all(term.lower() in haystack for term in expected)


def _min_confidence(published, expected: str) -> bool:
    meta = json.loads(Path(published.metadata_path).read_text())
    return CONFIDENCE_ORDER.index(meta["confidence"]) >= CONFIDENCE_ORDER.index(expected)


def score_case(case: EvalCase, trace, published) -> dict:
    checks = {
        "tools_called": lambda: _tools_called(trace, case.expect["tools_called"]),
        "patch_targets": lambda: _patch_targets(published, case.expect["patch_targets"]),
        "diagnosis_mentions": lambda: _diagnosis_mentions(
            published, case.expect["diagnosis_mentions"]
        ),
        "min_confidence": lambda: _min_confidence(published, case.expect["min_confidence"]),
    }
    required = {name: checks[name]() for name in case.score["required"]}
    advisory = {name: checks[name]() for name in case.score["advisory"]}
    return {
        "case_id": case.id,
        "required": required,
        "advisory": advisory,
        "passed": all(required.values()),
    }


def patch_text(published) -> str:
    """The rendered diff, read back from the artifact the publisher wrote.

    Re-deriving it from a fresh ToolContext would yield an empty string --
    the staged edits live on the context the runner used, not a new one.
    """
    if published.patch_path is None:
        return ""
    return Path(published.patch_path).read_text()
```

- [ ] **Step 5: Write the replay runner and its cassette**

Create `{{cookiecutter.project_slug}}/evals/replay.py`:

```python
"""Deterministic replay of a recorded agent run.

This replays the tool sequence and final diagnosis rather than mocking the
runner outright, so tool dispatch, path confinement, and diff rendering all
execute for real. Only the model's decisions are canned.
"""

import json
from pathlib import Path

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    AgentResult,
    ProposedEdit,
)
from {{ cookiecutter.project_slug }}.agentic.runner import dispatch_tool
from {{ cookiecutter.project_slug }}.agentic.tools import ToolContext, ToolError

CASSETTE_DIR = Path(__file__).parent / "cassettes"


def cassette_for(case_id: str) -> Path:
    return CASSETTE_DIR / f"{case_id}.json"


class ReplayRunner:
    """Drives real tool execution from a recorded call list."""

    def __init__(self, cassette_path: Path) -> None:
        self.cassette = json.loads(Path(cassette_path).read_text())

    def run(self, ctx, recorder) -> AgentResult:
        tool_ctx = ToolContext(
            project_root=Path(ctx.project_root), log_path=Path(ctx.log_path)
        )
        for call in self.cassette["tool_calls"]:
            with recorder.element(
                "tool_call", call["name"], input_summary=call["input"]
            ) as box:
                try:
                    output = dispatch_tool(tool_ctx, call["name"], call["input"])
                    box["output_summary"] = {"chars": len(output)}
                except ToolError as exc:
                    box["output_summary"] = {"error": str(exc)}

        diagnosis = AgentDiagnosis.model_validate(self.cassette["diagnosis"])
        recorder.add("result", "diagnosis", output_summary={"confidence": diagnosis.confidence})
        return AgentResult(
            diagnosis=diagnosis,
            edits=[ProposedEdit.model_validate(e) for e in self.cassette["edits"]],
            elements=list(recorder._elements),
        )
```

Create `{{cookiecutter.project_slug}}/evals/cassettes/schema_drift.json`:

```json
{
  "tool_calls": [
    {"name": "read_task_log", "input": {"task_id": "load", "tail_lines": 50}},
    {"name": "read_repo_file", "input": {"path": "src/pkg/batch/dags/example_dag.py"}},
    {"name": "read_repo_file", "input": {"path": "src/pkg/lakehouse/create_tables.sql"}},
    {
      "name": "propose_patch",
      "input": {
        "path": "src/pkg/batch/dags/example_dag.py",
        "old_str": "customer_name",
        "new_str": "name",
        "rationale": "The events table declares a `name` column; extract() emits `customer_name`."
      }
    }
  ],
  "diagnosis": {
    "root_cause": "extract() emits a record keyed customer_name, but the events table declared in create_tables.sql has a name column. The producer renamed the field without updating the schema.",
    "offending_field": "customer_name",
    "confidence": "high",
    "patch_summary": "rename customer_name to name in extract()"
  },
  "edits": [
    {
      "path": "src/pkg/batch/dags/example_dag.py",
      "old_str": "customer_name",
      "new_str": "name",
      "rationale": "The events table declares a `name` column; extract() emits `customer_name`."
    }
  ]
}
```

The cassette's `tool_calls` list has four entries while the case expects three names in `tools_called`. Update the case's `expect.tools_called` to match the recorded sequence:

```yaml
  tools_called: [read_task_log, read_repo_file, read_repo_file, propose_patch]
```

- [ ] **Step 6: Register the pytest markers and eval dependency**

In `{{cookiecutter.project_slug}}/pyproject.toml`, add the agentic dependency entry for `pyyaml` and a pytest config block:

```
{%- if cookiecutter.include_agentic %}
    "pydantic>=2.7",
    "anthropic>=0.40",
    "pyyaml>=6.0",
{%- endif %}
```

And after the `[tool.ruff]` block:

```
{%- if cookiecutter.include_agentic %}

[tool.pytest.ini_options]
testpaths = ["tests", "evals"]
markers = [
    "live: hits the real Anthropic API; deselect with -m \"not live\". Skips without ANTHROPIC_API_KEY",
    "graded: runs the LLM grader over a live run; deselect with -m \"not graded\". Skips without ANTHROPIC_API_KEY",
]
addopts = "-m 'not live and not graded'"
{%- endif %}
```

`addopts` is what keeps `uv run pytest` deterministic by default: tiers 3 and 4 are deselected unless explicitly requested.

- [ ] **Step 7: Update the manifest**

Add `"evals"` to `remove`. `pyproject.toml` is already in `gated`.

- [ ] **Step 8: Run the eval suite**

Re-bake, then:

```bash
cd /tmp/scaffold-dev/my_data_project && uv sync
uv run pytest evals/ -v
```

Expected: PASS with the live test deselected by `addopts`.

Verify the live tier is reachable but skipping:

```bash
uv run pytest evals/ -m live -v
```

Expected: SKIPPED (no `ANTHROPIC_API_KEY`), not failed.

- [ ] **Step 9: Commit**

```bash
git add "{{cookiecutter.project_slug}}" hooks/
git commit -m "feat: add data-driven eval cases with deterministic replay tier"
```

---

### Task 10: The grader

**Files:**
- Create: `{{cookiecutter.project_slug}}/evals/grader.py`
- Create: `{{cookiecutter.project_slug}}/evals/cassettes/schema_drift.grade.json`
- Create: `{{cookiecutter.project_slug}}/evals/test_grader.py`
- Modify: `{{cookiecutter.project_slug}}/evals/test_cases.py`
- Modify: `hooks/post_gen_project.py`

**Interfaces:**
- Consumes: `EvalCase` (Task 9), `Trace` (Task 2)
- Produces:
  - `CriterionVerdict(criterion: str, score: int, passed: bool, feedback: str)`
  - `GradeReport(case_id: str, model: str, overall: int, verdicts: list[CriterionVerdict], summary: str, suggested_rubric_gaps: list[str])`
  - `Grader` protocol: `grade(case, trace, diff) -> GradeReport`
  - `FakeGrader(report: GradeReport)`, `ClaudeGrader(model: str = GRADER_MODEL)`
  - `render_markdown(report: GradeReport) -> str`
  - `write_report(report: GradeReport, reports_dir: Path, timestamp: str) -> Path`
  - `GRADER_MODEL: str`

- [ ] **Step 1: Write the failing test**

Create `{{cookiecutter.project_slug}}/evals/test_grader.py`:

```python
import json

from evals.grader import (
    GRADER_MODEL,
    CriterionVerdict,
    FakeGrader,
    GradeReport,
    render_markdown,
    write_report,
)
from evals.harness import load_cases


def _report() -> GradeReport:
    return GradeReport(
        case_id="schema_drift",
        model="test-model",
        overall=4,
        verdicts=[
            CriterionVerdict(
                criterion="tools_called",
                score=5,
                passed=True,
                feedback="Read the log before patching.",
            ),
            CriterionVerdict(
                criterion="patch_targets",
                score=3,
                passed=True,
                feedback="Patched the DAG, though the DDL was also a valid target.",
            ),
        ],
        summary="Correct diagnosis, minimal patch.",
        suggested_rubric_gaps=["no criterion checks the patch is minimal"],
    )


def test_grader_model_comes_from_the_cookiecutter_variable():
    assert GRADER_MODEL == "{{ cookiecutter.agentic_grader_model }}"


def test_fake_grader_returns_its_fixture():
    case = load_cases()[0]
    report = FakeGrader(_report()).grade(case, trace=None, diff="")
    assert report.overall == 4
    assert report.verdicts[0].criterion == "tools_called"


def test_render_markdown_includes_every_criterion():
    markdown = render_markdown(_report())
    assert "| tools_called | 5 | pass |" in markdown
    assert "| patch_targets | 3 | pass |" in markdown
    assert "Correct diagnosis" in markdown
    assert "no criterion checks the patch is minimal" in markdown


def test_write_report_emits_json_and_markdown(tmp_path):
    path = write_report(_report(), tmp_path, timestamp="20260729T000000Z")

    assert path.name == "schema_drift-20260729T000000Z.json"
    saved = json.loads(path.read_text())
    assert saved["overall"] == 4
    assert saved["model"] == "test-model"

    markdown = (tmp_path / "schema_drift-20260729T000000Z.md").read_text()
    assert "tools_called" in markdown


def test_report_records_the_grader_model_for_cross_version_comparison():
    # Grader scores are not comparable across model versions; recording the
    # model is what lets a reader tell that. Assert the value round-trips,
    # not merely that the field is declared.
    report = _report()
    written = json.loads(report.model_dump_json())
    assert written["model"] == "test-model"
    assert "test-model" in render_markdown(report)


def test_render_markdown_marks_a_failed_criterion_as_fail():
    report = _report()
    report.verdicts[1].passed = False
    assert "| patch_targets | 3 | fail |" in render_markdown(report)
```

- [ ] **Step 2: Run test to verify it fails**

Re-bake, then: `uv run pytest evals/test_grader.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'evals.grader'`

- [ ] **Step 3: Write the grader**

Create `{{cookiecutter.project_slug}}/evals/grader.py`:

```python
"""LLM grading of a healing run, with the same seam treatment as AgentRunner.

The grader is a second agent, so it gets FakeGrader/ClaudeGrader for the
same reason the runner does: the eval suite must not acquire a
non-deterministic dependency in the layer whose job is determinism.

It lives under evals/ rather than agentic/ because it is test
infrastructure -- the healing DAG never imports it.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel

GRADER_MODEL = "{{ cookiecutter.agentic_grader_model }}"
REPORTS_DIR = Path(__file__).parent / "reports"


class CriterionVerdict(BaseModel):
    criterion: str
    score: int
    passed: bool
    feedback: str


class GradeReport(BaseModel):
    case_id: str
    model: str
    overall: int
    verdicts: list[CriterionVerdict]
    summary: str
    suggested_rubric_gaps: list[str] = []


GRADE_SCHEMA = {
    "type": "object",
    "properties": {
        "overall": {"type": "integer", "minimum": 0, "maximum": 5},
        "verdicts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "criterion": {"type": "string"},
                    "score": {"type": "integer", "minimum": 0, "maximum": 5},
                    "passed": {"type": "boolean"},
                    "feedback": {"type": "string"},
                },
                "required": ["criterion", "score", "passed", "feedback"],
                "additionalProperties": False,
            },
        },
        "summary": {"type": "string"},
        "suggested_rubric_gaps": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["overall", "verdicts", "summary", "suggested_rubric_gaps"],
    "additionalProperties": False,
}

GRADER_SYSTEM = """You grade an automated debugging agent's work against a rubric.

You receive the rubric, the agent's full execution trace, and the patch it
produced. You do NOT receive the agent's own instructions -- grading against
those would measure compliance, not correctness.

Score each rubric criterion independently from 0 to 5 and say specifically
what was right or wrong. A single overall number hides which criterion moved;
per-criterion feedback is the point.

Also list criteria the rubric should have had but did not. If the agent did
something notably good or bad that no criterion covers, that is a rubric gap.
"""


class Grader(Protocol):
    def grade(self, case, trace, diff: str) -> GradeReport: ...


class FakeGrader:
    """Returns a fixture. Keeps report rendering testable without an API key."""

    def __init__(self, report: GradeReport) -> None:
        self.report = report

    def grade(self, case, trace, diff: str) -> GradeReport:
        return self.report


class ClaudeGrader:
    def __init__(self, model: str = GRADER_MODEL) -> None:
        self.model = model

    def grade(self, case, trace, diff: str) -> GradeReport:
        import anthropic

        from {{ cookiecutter.project_slug }}.agentic.runner import _model_kwargs

        elements = json.dumps(
            [e.model_dump(exclude_none=True) for e in trace.elements], indent=2
        )
        prompt = (
            f"# Rubric for case `{case.id}`\n\n"
            f"```yaml\n{json.dumps(case.expect, indent=2)}\n```\n\n"
            f"# Agent execution trace\n\n```json\n{elements}\n```\n\n"
            f"# Proposed patch\n\n```diff\n{diff or '(no patch produced)'}\n```\n\n"
            "Grade each rubric criterion."
        )
        response = anthropic.Anthropic().messages.create(
            model=self.model,
            max_tokens=4000,
            system=GRADER_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
            output_config={"format": {"type": "json_schema", "schema": GRADE_SCHEMA}},
            **_model_kwargs(self.model),
        )
        text = next(b.text for b in response.content if b.type == "text")
        payload = json.loads(text)
        return GradeReport(case_id=case.id, model=self.model, **payload)


def render_markdown(report: GradeReport) -> str:
    lines = [
        f"# Grade report — `{report.case_id}`",
        "",
        f"**Overall:** {report.overall}/5 · **Grader:** `{report.model}`",
        "",
        "| Criterion | Score | Result | Feedback |",
        "| --- | --- | --- | --- |",
    ]
    for verdict in report.verdicts:
        outcome = "pass" if verdict.passed else "fail"
        lines.append(
            f"| {verdict.criterion} | {verdict.score} | {outcome} | {verdict.feedback} |"
        )
    lines += ["", "## Summary", "", report.summary]
    if report.suggested_rubric_gaps:
        lines += ["", "## Suggested rubric gaps", ""]
        lines += [f"- {gap}" for gap in report.suggested_rubric_gaps]
        lines += [
            "",
            "_Advisory only. These are never assertions — they are how the rubric improves._",
        ]
    return "\n".join(lines) + "\n"


def write_report(
    report: GradeReport, reports_dir: Path = REPORTS_DIR, timestamp: str | None = None
) -> Path:
    """Feedback that only appears in pytest output gets ignored."""
    stamp = timestamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / f"{report.case_id}-{stamp}.json"
    json_path.write_text(report.model_dump_json(indent=2))
    (reports_dir / f"{report.case_id}-{stamp}.md").write_text(render_markdown(report))
    return json_path
```

- [ ] **Step 4: Add the graded tier to the case tests**

Append to `{{cookiecutter.project_slug}}/evals/test_cases.py`:

```python
@requires_api_key
@pytest.mark.graded
@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_case_is_graded_and_reported(case, tmp_path):
    from evals.grader import ClaudeGrader, write_report
    from evals.harness import patch_text

    project, ctx = build_case_project(tmp_path, case)
    trace, published = run_healing(
        ctx,
        runner=ClaudeRunner(),
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    report = ClaudeGrader().grade(case, trace, patch_text(published))
    path = write_report(report)

    print(f"\ngrade report: {path}")
    # These assert the grader produced a *usable* report, not that the agent
    # scored well -- this tier reports, it does not gate on quality.
    assert 0 <= report.overall <= 5
    assert report.model == GRADER_MODEL
    graded = {v.criterion for v in report.verdicts}
    assert set(case.score["required"]) <= graded, (
        f"grader skipped required criteria: {set(case.score['required']) - graded}"
    )
    assert path.is_file() and path.with_suffix(".md").is_file()
```

Add `GRADER_MODEL` to the imports at the top of the `from evals.grader import ...` line in this file.

These assertions can fail: a grader that ignores a required criterion, returns
an out-of-range score, or writes no report fails the test. What they do *not*
do is gate on the agent's score — that is what keeps this tier reporting
rather than blocking.

- [ ] **Step 5: Add a reports directory placeholder**

Create `{{cookiecutter.project_slug}}/evals/reports/.gitkeep` as an empty file, and add `evals/reports/*.json` and `evals/reports/*.md` to `{{cookiecutter.project_slug}}/.gitignore`.

- [ ] **Step 6: Update the manifest**

No change — `evals` is already in `remove` as a whole directory.

- [ ] **Step 7: Run tests**

Re-bake, then:

```bash
cd /tmp/scaffold-dev/my_data_project && uv sync
uv run pytest evals/ -v
uv run pytest evals/ -m graded -v
```

Expected: the first PASSES with graded deselected; the second SKIPS without an API key.

Then the full generated suite: `uv run pytest -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add "{{cookiecutter.project_slug}}"
git commit -m "feat: add LLM grader with per-criterion feedback and reports"
```

---

### Task 11: Integration, docs, and full-matrix verification

**Files:**
- Modify: `{{cookiecutter.project_slug}}/docker-compose.yml`
- Modify: `{{cookiecutter.project_slug}}/.github/workflows/ci.yml`
- Modify: `{{cookiecutter.project_slug}}/README.md`
- Modify: `README.md`
- Modify: `docs/INSTALLATION.md`
- Modify: `tests/test_bake_agentic.py`
- Modify: `tests/test_bake_integration.py`
- Modify: `hooks/post_gen_project.py`

**Interfaces:**
- Consumes: everything
- Produces: no new code interfaces

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_bake_agentic.py`:

```python
def test_compose_mounts_the_agentic_dags_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    compose = (result.project_path / "docker-compose.yml").read_text()
    assert "ANTHROPIC_API_KEY" in compose


def test_compose_omits_the_api_key_when_agentic_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    compose = (result.project_path / "docker-compose.yml").read_text()
    assert "ANTHROPIC_API_KEY" not in compose


def test_ci_runs_evals_deterministically_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    ci = (result.project_path / ".github" / "workflows" / "ci.yml").read_text()
    assert "not live and not graded" in ci


def test_generated_readme_documents_the_agentic_pattern(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    readme = (result.project_path / "README.md").read_text()
    assert "failure_mode" in readme
    assert "artifacts/" in readme


@pytest.mark.slow
def test_agentic_bake_generated_tests_pass(cookies):
    result = bake_with(
        cookies,
        include_batch=True,
        include_streaming=False,
        include_lakehouse=True,
        include_dbt=False,
        include_agentic=True,
    )
    assert result.exit_code == 0

    from conftest import run_cmd

    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")


@pytest.mark.slow
def test_agentic_without_lakehouse_generated_tests_pass(cookies):
    result = bake_with(
        cookies,
        include_batch=True,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=False,
        include_agentic=True,
    )
    assert result.exit_code == 0

    from conftest import run_cmd

    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")
```

Append to `tests/test_bake_integration.py`:

```python
def test_agentic_bake_lints_clean(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    assert result.exit_code == 0
    run_cmd(result.project_path, "uvx", "ruff", "check", ".")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_bake_agentic.py tests/test_bake_integration.py -m "not slow" -v`
Expected: FAIL on the compose, CI, and README assertions.

- [ ] **Step 3: Gate the compose service**

In `{{cookiecutter.project_slug}}/docker-compose.yml`, inside the `airflow-standalone` service's `environment` block, add after `PYTHONPATH`:

```
{%- if cookiecutter.include_agentic %}
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY:-}
{%- endif %}
```

And extend `_PIP_ADDITIONAL_REQUIREMENTS` on the same line it already appears:

```
      _PIP_ADDITIONAL_REQUIREMENTS: "pyarrow>=17.0{% if cookiecutter.include_lakehouse %} duckdb>=1.0{% endif %}{% if cookiecutter.include_agentic %} anthropic>=0.40 pydantic>=2.7{% endif %}"
```

- [ ] **Step 4: Gate the CI workflow**

In `{{cookiecutter.project_slug}}/.github/workflows/ci.yml`, replace the pytest line:

```
{%- if cookiecutter.include_agentic %}
      - run: uv run pytest -m "not live and not graded" || [ $? -eq 5 ]
{%- else %}
      - run: uv run pytest || [ $? -eq 5 ]
{%- endif %}
```

The explicit marker filter is redundant with `addopts` but documents the intent where a reader will look for it.

- [ ] **Step 5: Document the Pattern in the generated README**

Append to `{{cookiecutter.project_slug}}/README.md`:

```
{%- if cookiecutter.include_agentic %}

## Agentic self-healing

When a batch DAG task fails, a healing task runs an AI agent that reads the
failure logs, diagnoses the root cause, and writes a patch for you to review.

### Try it

Trigger the demo DAG with the failure injected:

```bash
docker compose up -d airflow-standalone
# In the Airflow UI, trigger example_events_dag with config:
#   {"failure_mode": "schema_drift"}
```

The `load` task fails, the `heal_on_failure` task runs, and you get:

- `artifacts/<trace_id>.patch` — a unified diff, ready to `git apply`
- `artifacts/<trace_id>.json` — the proposed PR title, body, and confidence
{%- if cookiecutter.include_lakehouse %}
- a row in the `agent_traces` DuckDB table holding the full run as a JSON array
{%- else %}
- `artifacts/traces/<trace_id>.json` — the full run as a JSON array
{%- endif %}

Nothing is applied automatically. The agent proposes; you merge.

### Configuration

| Variable | Purpose |
| --- | --- |
| `ANTHROPIC_API_KEY` | Required for live runs. The default test suite does not need it. |
| `AGENTIC_GITHUB_REPO` | Set to `owner/repo` to open real pull requests instead of writing local patches. Requires push credentials and an authenticated `gh`. |

The agent's tools are limited to reading the task log, reading a file inside
the project root, and staging a single-occurrence replacement. It cannot run
shell commands or write to disk.

Edit the diagnosis prompt at `src/{{ cookiecutter.project_slug }}/agentic/prompts/diagnose.md`.

### Evals

```bash
uv run pytest evals/                # deterministic; no API key needed
uv run pytest evals/ -m live        # runs the real model
uv run pytest evals/ -m graded      # runs the model, then grades the run
```

Add a case by dropping a YAML file into `evals/cases/`. Graded runs write a
report to `evals/reports/`.
{%- endif %}
```

- [ ] **Step 6: Document the Pattern in the template README**

In the repo-root `README.md`:

Add to the Features Pattern list, after Transformation:

```markdown
  - **Agentic** — a self-healing loop: a failing Batch DAG triggers an AI agent that diagnoses from logs and proposes a patch for human review
```

Add to the template-options table:

```markdown
| `include_agentic`     | `true`            | Scaffold the agentic self-healing loop (requires `include_batch`) |
| `agentic_model`       | `claude-haiku-4-5` | Model used for diagnosis                                 |
| `agentic_grader_model` | `claude-haiku-4-5` | Model used by the eval grader                           |
```

Add to the Patterns section:

```markdown
- **Agentic** scaffolds `src/<slug>/agentic/`, a healing task wired into the Batch DAG with `trigger_rule=ALL_FAILED`, JSON-array run traces, and an `evals/` suite. It is the one Pattern with a dependency: it requires **Batch**, since there is no DAG to heal without one.
```

- [ ] **Step 7: Document it in the installation guide**

In `docs/INSTALLATION.md`, add this section after the existing per-Pattern documentation:

```markdown
### Agentic Pattern

The Agentic Pattern requires the Batch Pattern. Generating with
`include_agentic=true include_batch=false` is rejected at generation time —
there is no DAG to heal.

```bash
cookiecutter gh:vincyf1/cookiecutter-data-scaffold --no-input \
  project_name="Orders Pipeline" \
  agentic_model=claude-haiku-4-5
```

**No API key is needed to generate, install, or test the project.** The
generated test suite runs entirely against a fake agent runner and recorded
replays, so `uv run pytest` works offline.

An `ANTHROPIC_API_KEY` is needed only to run the agent for real:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
docker compose up -d airflow-standalone
```

Then trigger `example_events_dag` from the Airflow UI with the config
`{"failure_mode": "schema_drift"}`. The `load` task fails, `heal_on_failure`
runs, and the results land in the generated project:

| Path | Contents |
| --- | --- |
| `artifacts/<trace_id>.patch` | Unified diff, ready for `git apply` |
| `artifacts/<trace_id>.json` | Proposed PR title, body, confidence, and edits |
| `artifacts/traces/<trace_id>.json` | The full agent run as a JSON array (when Lakehouse is off) |
| `agent_traces` table | The same JSON array, in DuckDB (when Lakehouse is on) |

**Troubleshooting**

| Symptom | Cause |
| --- | --- |
| `heal_on_failure` is skipped | `load` succeeded. The heal task uses `trigger_rule=ALL_FAILED`. |
| Healing task fails with an auth error | `ANTHROPIC_API_KEY` is not visible to the Airflow worker. Export it before `docker compose up`. |
| Trace missing but the patch exists | Trace writes are non-blocking by design; check the task log for a `failed to persist agent trace` warning. |
| `uv run pytest` tries to call the API | Marker filtering was bypassed. The `live` and `graded` tiers are deselected by `addopts` in `pyproject.toml`. |
```

- [ ] **Step 8: Run the full template suite**

```bash
uv run pytest -m "not slow" -v
```

Expected: PASS.

- [ ] **Step 9: Run the slow matrix**

```bash
uv run pytest -m slow -v
```

Expected: PASS. This is the real gate — it bakes and runs generated projects for real.

- [ ] **Step 10: Verify the flag-off path one more time**

```bash
rm -rf /tmp/scaffold-final && mkdir -p /tmp/scaffold-final
uv run cookiecutter . --no-input --output-dir /tmp/scaffold-final include_agentic=false
cd /tmp/scaffold-final/my_data_project && uv sync && uv run pytest -v
grep -r "agentic" . --include="*.py" --include="*.toml" --include="*.yml" || echo "no agentic leftovers"
```

Expected: tests PASS and the grep reports no leftovers — this is the "no dead branches" promise the README makes.

- [ ] **Step 11: Commit**

```bash
git add .
git commit -m "feat: wire agentic Pattern into compose, CI, and docs"
```

---

## Verification Checklist

Run all of these before considering the work complete:

- [ ] `uv run pytest -m "not slow" -v` — template suite green
- [ ] `uv run pytest -m slow -v` — bake matrix green
- [ ] Bake with all Patterns on; `uv run pytest` in the generated project is green with no API key and no network
- [ ] Bake with `include_agentic=false`; no file mentions "agentic"
- [ ] Bake with `include_lakehouse=false, include_agentic=true`; traces fall back to JSONL and tests pass
- [ ] Bake with `include_agentic=true, include_batch=false`; the bake is rejected with a clear message
- [ ] `uv run pytest evals/ -m live` skips rather than fails without a key
- [ ] `uvx ruff check .` clean in a generated project
