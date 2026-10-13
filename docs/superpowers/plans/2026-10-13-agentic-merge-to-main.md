# Agentic Pattern Merge to main Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge `main` (PR #2's `Event`/`EventSink` contract) into
`agentic-self-healing-pattern`, move schema-drift detection onto
`Event.from_mapping`, retire `missing_config`, and ship the branch to `main`
by PR with a merge commit.

**Architecture:** One merge commit resolves the 10 conflicts by taking
`main`'s storage layer and re-adding the branch's agentic wiring. Follow-up
commits then add `SchemaDriftError` to `events.py` test-first, repoint the
synthetic eval fixture and cassettes at the `Event` contract, and update the
docs. The demo has one failure mode, `schema_drift`, in every configuration.

**Tech Stack:** cookiecutter + Jinja templates, Python 3.12, Airflow 3
(`airflow.sdk`), DuckDB, PyArrow, pytest + pytest-cookies, ruff, uv.

**Spec:** `docs/superpowers/specs/2026-10-13-agentic-merge-to-main-design.md`

## Global Constraints

- Work only in the worktree `.worktrees/agentic-pattern`, branch
  `agentic-self-healing-pattern`. Never commit to `main`.
- Commit messages carry no `Co-Authored-By` or other Claude attribution;
  credit only the user.
- `SchemaDriftError` subclasses `ValueError` and lives in
  `src/{{cookiecutter.project_slug}}/events.py`.
- `Event.from_mapping` raises it only when a declared field is missing;
  extra fields alone are accepted.
- Declared fields come from `dataclasses.fields(Event)`, never a
  hand-written list.
- `FAILURE_MODES = ["none", "schema_drift"]` in every configuration.
- The PR merges with a merge commit, not squash or rebase.
- Out of scope: demo features (patch validation, Airflow REST, dbt tools),
  idempotent pipelines, the pre-existing `ruff format` drift.

## Review Focus

1. **A lakehouse-off project triggered with `schema_drift`.** `load` fails with
   `SchemaDriftError`, and the healer runs and lands a patch. Before this plan,
   this configuration shipped `missing_config` instead. Pinned by the slow seam
   test, now parametrized with lakehouse on and off (Tasks 1 and 2).
2. **The streaming consumer receiving a renamed field.** The message is skipped
   as malformed; the consumer does not crash. Pinned by a new payload in
   `test_run_skips_invalid_payload_and_continues` (Task 2).
3. **A producer that adds a field.** The record still becomes an `Event`.
   Pinned by `test_extra_fields_alone_are_accepted` (Task 2).
4. **The eval fixture drifting from the real contract.** Replay would pass
   while the agent sees a log the runtime never writes. Pinned by
   `test_the_fixture_log_matches_what_the_real_contract_raises` (Task 3).
5. **A user's own eval case still naming `missing_config`.** It fails with an
   error that lists `schema_drift` as the only supported mode. Pinned by the
   updated `test_unknown_failure_mode_reports_the_case_and_supported_modes`
   (Task 3).

---

## Paths used below

- `SLUG_DIR` = `{{cookiecutter.project_slug}}` (the template project root)
- `PKG` = `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}`

Quote these paths in a shell: zsh treats `{{` literally, but globs break
on `*`. Run every command from `.worktrees/agentic-pattern`.

Test commands:

- Template fast tier: `uv run pytest -m "not slow" -q`
- Template slow tier, one test: `uv run pytest -m slow -q -k <name>`
- Generated project: bake through the Python API, never the cookiecutter
  CLI. The CLI passes `include_lakehouse=false` through as the string
  `"false"`, which Jinja treats as true, and the hook would render it as the
  bare name `false`. Put this function at the top of any shell block that
  bakes. It prints the generated project's path.

```bash
bake() {  # bake <output_dir> <lakehouse: on|off> [agentic: on|off, default on]
  uv run python -c 'import sys
from cookiecutter.main import cookiecutter
print(cookiecutter(".", no_input=True, output_dir=sys.argv[1], extra_context={
    "include_lakehouse": sys.argv[2] == "on",
    "include_agentic": sys.argv[3] == "on",
}))' "$1" "$2" "${3:-on}"
}
```

---

### Task 1: Merge main and resolve the 10 conflicts

The merge commit takes `main`'s storage layer everywhere and re-adds the
agentic wiring. It also removes `missing_config`'s runtime pieces, because
they import `batch/sinks.py`, which `main` deleted. Drift detection is not
added yet. Until Task 2, `Event.from_mapping` raises `KeyError('name')` for
the drift record, so **both cases of the slow seam test are expected to
fail at the end of this task**. That failure is Task 2's red test.

**Files:**
- Modify (conflicts): `hooks/post_gen_project.py`, `tests/test_bake_integration.py`,
  `SLUG_DIR/README.md`, `PKG/batch/dags/example_dag.py`, `PKG/lakehouse/writer.py`,
  `PKG/sinks.py`, `SLUG_DIR/tests/test_batch_dag.py`,
  `SLUG_DIR/tests/test_lakehouse_writer.py`, `SLUG_DIR/tests/test_streaming_consumer.py`
- Delete: `PKG/batch/sinks.py`, `PKG/batch/config.py`, `PKG/batch/pipeline.toml`,
  `SLUG_DIR/tests/test_batch_config.py`
- Modify (no conflict): `tests/test_bake_agentic.py`

**Interfaces:**
- Consumes from `main`: `Event` and `Event.from_mapping(record) -> Event` from
  `{{ cookiecutter.project_slug }}.events`, and `create_event_sink(destination: Path) -> EventSink`
  from `{{ cookiecutter.project_slug }}.sinks`.
- Produces: `FAILURE_MODES` and `DEFAULT_DESTINATION` in `example_dag.py`. The
  `load` task has signature `load(records: list[dict]) -> None`; its
  `python_callable` takes the records list positionally.

- [ ] **Step 1: Start the merge**

```bash
git status --short          # must be clean apart from untracked files
git merge --no-ff --no-commit main
```

Expected: `Automatic merge failed` with 9 `UU` files and 1 `UD` file
(`PKG/batch/sinks.py`).

- [ ] **Step 2: Take main's side for four files**

The branch's changes to these are obsolete. It split an import line in the
old `sinks.py`, and the old writer's validation moves to `events.py` in
Task 2. `main`'s streaming tests already end the poll loop with
`pytest.raises(StopIteration)`, so the branch's `contextlib.suppress`
rewrite has nothing left to apply to.

```bash
git checkout --theirs -- \
  '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/sinks.py' \
  '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/writer.py' \
  '{{cookiecutter.project_slug}}/tests/test_lakehouse_writer.py' \
  '{{cookiecutter.project_slug}}/tests/test_streaming_consumer.py'
git rm -q \
  '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/sinks.py' \
  '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/config.py' \
  '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/pipeline.toml' \
  '{{cookiecutter.project_slug}}/tests/test_batch_config.py'
```

- [ ] **Step 3: Write the merged `PKG/batch/dags/example_dag.py`**

Replace the whole file with:

```python
from datetime import UTC, datetime
from pathlib import Path

from airflow.sdk import Param, dag, task
{%- if cookiecutter.include_agentic %}
from airflow.task.trigger_rule import TriggerRule
{%- endif %}

{% if cookiecutter.include_agentic -%}
from {{ cookiecutter.project_slug }}.agentic.dags.healing_dag import (
    heal_on_failure,
    record_failure,
)
{% endif -%}
from {{ cookiecutter.project_slug }}.events import Event
from {{ cookiecutter.project_slug }}.sinks import create_event_sink
{% if cookiecutter.include_lakehouse %}
DEFAULT_DESTINATION = Path("lakehouse.duckdb")
{%- else %}
DEFAULT_DESTINATION = Path("data/events/batch")
{%- endif %}

# Off by default: the shipped demo behaves exactly as it did before the
# self-healing loop existed. Set failure_mode=schema_drift to make extract()
# emit a record that no longer matches the Event contract in events.py.
FAILURE_MODES = ["none", "schema_drift"]


# Airflow expects a timezone-aware start_date; a naive datetime here is a
# latent bug, not just a lint finding.
@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    params={"failure_mode": Param("none", type="string", enum=FAILURE_MODES)},
)
def example_events_dag():
    @task
    def extract(**context) -> list[dict]:
        if context["params"]["failure_mode"] == "schema_drift":
            # The producer renamed `name`; the Event contract still declares it.
            return [{"id": 1, "customer_name": "example"}]
        return [{"id": 1, "name": "example"}]

{%- if cookiecutter.include_agentic %}

    @task(on_failure_callback=record_failure)
{%- else %}

    @task
{%- endif %}
    def load(records: list[dict]) -> None:
        events = [Event.from_mapping(record) for record in records]
        with create_event_sink(DEFAULT_DESTINATION) as sink:
            sink.write(events)
{%- if cookiecutter.include_agentic %}

    loaded = load(extract())
    # The healer is told which task failed. It cannot read that off its own
    # TaskInstance -- Airflow injects the healing task's own `ti`, so
    # `ti.task_id` there is "heal_on_failure".
    heal_on_failure.override(trigger_rule=TriggerRule.ALL_FAILED)(
        failed_task_id="load"
    ) << loaded
{%- else %}

    load(extract())
{%- endif %}


example_events_dag()
```

The comment on the drift line names `name`, not `"customer_name": "example"`.
The slow seam test's stand-in patch uses that exact string as `old_str`, and
`render_diff` requires it to be unique in the file.

- [ ] **Step 4: Write the merged `SLUG_DIR/tests/test_batch_dag.py`**

Replace the whole file with:

```python
import pytest
from airflow.sdk.exceptions import ParamValidationError

from {{ cookiecutter.project_slug }}.batch.dags.example_dag import (
    FAILURE_MODES,
    example_events_dag,
)


def test_dag_has_no_import_errors():
    dag = example_events_dag()
    assert dag.dag_id == "example_events_dag"
    assert {"extract", "load"} <= {task.task_id for task in dag.tasks}


def test_dag_exposes_a_failure_mode_param():
    dag = example_events_dag()
    assert "failure_mode" in dag.params


def test_the_demo_offers_schema_drift_in_every_configuration():
    # A mode the shipped code cannot raise is worse than no mode at all: the
    # README promises the failure fires, and with ALL_FAILED wiring a load
    # task that quietly succeeds means the healer is skipped entirely.
    assert FAILURE_MODES == ["none", "schema_drift"]


@pytest.mark.parametrize("mode", ["none", "schema_drift"])
def test_failure_mode_accepts_both_values(mode):
    dag = example_events_dag()
    # dag.params[...] resolves to the bare value; plain attribute assignment
    # on the Param wrapper skips validation entirely. Drive the value
    # through resolve(), which is where the enum constraint actually runs.
    param = dag.params.get_param("failure_mode")
    assert param.resolve(value=mode) == mode


def test_failure_mode_rejects_a_value_outside_the_enum():
    dag = example_events_dag()
    param = dag.params.get_param("failure_mode")
    with pytest.raises(ParamValidationError):
        param.resolve(value="bogus")


def test_load_persists_validated_events(tmp_path, monkeypatch):
    import importlib

    module = importlib.import_module("{{ cookiecutter.project_slug }}.batch.dags.example_dag")
    destination = tmp_path / "output"
    monkeypatch.setattr(module, "DEFAULT_DESTINATION", destination)
    dag = example_events_dag()
    dag.get_task("load").python_callable([{"id": 7, "name": "loaded"}])
{%- if cookiecutter.include_lakehouse %}
    import duckdb

    with duckdb.connect(str(destination)) as connection:
        assert connection.execute("SELECT id, name FROM events").fetchall() == [(7, "loaded")]
{%- else %}
    import pyarrow.parquet as pq

    files = list(destination.glob("*.parquet"))
    assert len(files) == 1
    assert pq.read_table(files[0]).to_pylist() == [{"id": 7, "name": "loaded"}]
{%- endif %}
{%- if cookiecutter.include_agentic %}


def test_heal_task_is_wired_downstream_of_load():
    dag = example_events_dag()
    assert "heal_on_failure" in {task.task_id for task in dag.tasks}
    heal = dag.get_task("heal_on_failure")
    assert "load" in heal.upstream_task_ids


def test_heal_task_runs_only_when_load_fails():
    from airflow.task.trigger_rule import TriggerRule

    dag = example_events_dag()
    assert dag.get_task("heal_on_failure").trigger_rule == TriggerRule.ALL_FAILED


def test_heal_task_is_told_which_task_failed():
    # Airflow injects the *healing* task's own TaskInstance into its
    # context, so the failed task's id has to be supplied by the wiring --
    # reading it off `ti.task_id` at runtime yields "heal_on_failure".
    dag = example_events_dag()
    assert dag.get_task("heal_on_failure").op_kwargs["failed_task_id"] == "load"


def test_load_reports_its_exception_to_the_healer():
    # `context["exception"]` is a callback-context key; an ordinary
    # downstream task never sees it. This callback is the only thing that
    # carries the real exception across to heal_on_failure.
    from {{ cookiecutter.project_slug }}.agentic.dags.healing_dag import record_failure

    dag = example_events_dag()
    callbacks = dag.get_task("load").on_failure_callback
    if not isinstance(callbacks, list):
        callbacks = [callbacks]
    assert record_failure in callbacks
{%- endif %}
```

- [ ] **Step 5: Resolve `hooks/post_gen_project.py`**

There are three edits, and the conflict hunk is the first of them.

(a) Replace the conflict hunk in the `include_lakehouse` entry's `"gated"`
list. The list must end up exactly as follows.
`tests/test_pattern_manifest.py` asserts set equality against the
`include_lakehouse` sites in the tree.

```python
        "gated": [
            "README.md",
            "docker-compose.yml",
            "pyproject.toml",
            "tests/test_parquet_sink.py",
            "tests/test_batch_dag.py",
            f"src/{SLUG}/sinks.py",
            f"src/{SLUG}/batch/dags/example_dag.py",
            f"src/{SLUG}/streaming/consumer.py",
            f"src/{SLUG}/agentic/tracing.py",
            "tests/test_agentic_tracing.py",
            "tests/test_agentic_healing.py",
            "transformation/dbt_project.yml",
            "transformation/models/staging/stg_events.sql",
            "transformation/profiles.yml",
        ],
```

(b) In the `include_batch` entry's `"remove"` list, delete the line
`"tests/test_batch_config.py",`.

(c) In `COMPOUND_REMOVALS`, delete the whole `batch/pipeline.toml` entry:
its five comment lines and the
`({{ cookiecutter.include_batch and not cookiecutter.include_lakehouse }}, [...])` tuple.

The spec says to add `events.py` to the agentic `gated` list. **Do not.**
`events.py` contains no `include_agentic` gate, and the manifest test's set
equality would fail. The spec's intent, that `events.py` ships whenever
Batch does, is already met by the existing `COMPOUND_REMOVALS` entry.

- [ ] **Step 6: Resolve `SLUG_DIR/README.md`**

Keep both sides: the branch's agentic block first, then `main`'s event-ingestion
block. Remove the three conflict markers. Inside the agentic block, replace
the run-config comment and the explanatory paragraph (everything from
`# In the Airflow UI` down to, but not including, `The \`load\` task fails`)
with:

````markdown
# In the Airflow UI, trigger example_events_dag with config:
#   {"failure_mode": "schema_drift"}
```

`extract` emits a record keyed `customer_name` instead of `name`. `load` turns
each record into an `Event` before writing it, so it raises `SchemaDriftError`
naming the missing field, the undeclared one, and the fields the contract in
`src/{{ cookiecutter.project_slug }}/events.py` declares. This works with or
without the Lakehouse Pattern.

````

Keep the line `The \`load\` task fails, the \`heal_on_failure\` task runs, ...`
and everything after it unchanged.

- [ ] **Step 7: Resolve `tests/test_bake_integration.py`**

Keep both sides of the single conflict hunk. Remove the markers so that,
after `test_docker_compose_is_valid_yaml_for_all_batch_streaming_combinations`,
the file has three things in this order:
`test_docker_compose_pip_requirements_for_all_lakehouse_agentic_combinations`,
`test_agentic_bake_lints_clean`, and `main`'s
`@pytest.mark.slow def test_default_bake_generated_tests_pass`. Every other
test in the file has already merged cleanly; leave it alone.

- [ ] **Step 8: Update `tests/test_bake_agentic.py` for the retired mode**

(a) Change the parametrization of
`test_readme_documents_the_failure_mode_this_bake_actually_ships` to:

```python
@pytest.mark.parametrize(
    ("lakehouse", "mode"),
    [(True, "schema_drift"), (False, "schema_drift")],
)
```

and replace its leading comment with:

```python
    # The README once promised a failure the lakehouse-off bake could not
    # raise, so `load` succeeded and the ALL_FAILED healer was skipped. Both
    # configurations now ship schema_drift; pin that the docs and DAG agree.
```

(b) Delete `test_pipeline_config_ships_only_where_there_is_no_ddl` entirely.
Task 3 replaces it with a whole-tree check.

(c) In `test_demo_failure_triggers_the_healer_and_lands_a_patch`'s
parametrize list, replace the `missing_config` tuple with:

```python
        (
            False,
            "schema_drift",
            "customer_name",
            '"customer_name": "example"',
            '"name": "example"',
        ),
```

Leave `test_evals_present_when_enabled` alone; Task 3 owns the evals.

- [ ] **Step 9: Verify no conflict markers or stale imports remain**

```bash
git diff --check
git grep -n -e '<<<<<<<' -e '>>>>>>>' -e 'batch.sinks' -e 'batch.config' -e 'write_events' -- . ':!docs/superpowers'
```

Expected: no output from either command.

- [ ] **Step 10: Run the template fast tier and lint**

```bash
uv run pytest -m "not slow" -q
uv run ruff check .
```

Expected: all pass. If `test_pattern_manifest_gated_files_match_template_tree`
fails, its message prints the declared and actual sets; fix the list from
Step 5(a) to match the actual set, not the other way round.

- [ ] **Step 11: Confirm the expected red slow test**

```bash
uv run pytest -m slow -q -k test_demo_failure_triggers_the_healer_and_lands_a_patch
```

Expected: both cases FAIL at `assert needle in ctx["exception_message"]`,
because the message is `'name'` (a `KeyError`). Note the failure. Do not fix
it here; it is Task 2's red test.

- [ ] **Step 12: Commit the merge**

```bash
git add -A
git commit -m "Merge main: adopt the Event contract and lifecycle-managed sinks

Takes main's storage layer for sinks.py, lakehouse/writer.py and their
tests, and rewires the demo DAG onto Event.from_mapping and
create_event_sink() while keeping the healing task. Retires the
missing_config failure mode and batch/config.py with it: schema_drift
now ships in every configuration.

Drift detection itself moves to events.py in the next commit; until
then the slow seam test fails on a bare KeyError."
```

---

### Task 2: Detect schema drift on the Event contract

**Files:**
- Modify: `PKG/events.py`
- Modify: `SLUG_DIR/tests/test_events.py`
- Modify: `SLUG_DIR/tests/test_batch_dag.py` (append one test)
- Modify: `SLUG_DIR/tests/test_streaming_consumer.py` (one parametrize value)
- Modify: `hooks/post_gen_project.py` (add `tests/test_events.py` to lakehouse `gated`)

**Interfaces:**
- Consumes: `Event` from Task 1, unchanged in shape.
- Produces: `class SchemaDriftError(ValueError)` in
  `{{ cookiecutter.project_slug }}.events`. Its message format, which Task 3's
  fixture copies verbatim, is:
  `record lacks field(s) {missing}[ and carries undeclared field(s) {undeclared}]; the Event contract declares {declared}. Either the producer renamed a field or the contract is out of date.`
  Each `{...}` is a Python `list[str]` repr. `declared` is in dataclass field
  order, and `missing`/`undeclared` are in record order.

- [ ] **Step 1: Write the failing tests in `test_events.py`**

Change the import line to
`from {{ cookiecutter.project_slug }}.events import Event, SchemaDriftError`.
Then append:

```python


def test_a_renamed_field_raises_schema_drift_naming_both_sides():
    with pytest.raises(SchemaDriftError) as excinfo:
        Event.from_mapping({"id": 1, "customer_name": "example"})

    assert str(excinfo.value) == (
        "record lacks field(s) ['name'] and carries undeclared field(s) "
        "['customer_name']; the Event contract declares ['id', 'name']. "
        "Either the producer renamed a field or the contract is out of date."
    )


def test_a_missing_field_alone_does_not_claim_an_undeclared_one():
    with pytest.raises(SchemaDriftError) as excinfo:
        Event.from_mapping({"id": 1})

    assert "undeclared" not in str(excinfo.value)
    assert "['name']" in str(excinfo.value)


def test_schema_drift_is_a_value_error():
    # The streaming consumer skips ValueError as malformed; drift must stay
    # inside that net rather than crash the consumer.
    assert issubclass(SchemaDriftError, ValueError)


def test_extra_fields_alone_are_accepted():
    # Additive evolution: a producer may add a field before the contract
    # learns about it. Only a missing declared field is drift.
    record = {"id": 1, "name": "example", "source": "kafka"}
    assert Event.from_mapping(record) == Event(id=1, name="example")
{%- if cookiecutter.include_lakehouse %}


def test_every_event_field_is_a_column_in_the_events_table():
    # The contract and the DDL are two declarations of one schema. A field
    # the table lacks would pass from_mapping and then fail the INSERT.
    from dataclasses import fields

    import duckdb

    from {{ cookiecutter.project_slug }}.lakehouse.writer import DDL_PATH

    with duckdb.connect() as connection:
        connection.execute(DDL_PATH.read_text())
        columns = {row[0] for row in connection.execute("DESCRIBE events").fetchall()}

    assert {field.name for field in fields(Event)} <= columns
{%- endif %}
```

- [ ] **Step 2: Add the consumer and DAG tests**

In `SLUG_DIR/tests/test_streaming_consumer.py`, extend the parametrize list of
`test_run_skips_invalid_payload_and_continues` with a renamed-field payload:

```python
@pytest.mark.parametrize(
    "payload",
    [None, b"null", b"[]", b'{"id": true, "name": "bad"}', b'{"id": 3, "customer_name": "x"}'],
)
```

Append to `SLUG_DIR/tests/test_batch_dag.py`, directly before the
`{%- if cookiecutter.include_agentic %}` line:

```python


def test_the_drift_record_fails_load_with_a_diagnosable_error(tmp_path, monkeypatch):
    # The demo's failure must raise through the real load path, in every
    # configuration, with a message the healer can diagnose from.
    import importlib

    from {{ cookiecutter.project_slug }}.events import SchemaDriftError

    module = importlib.import_module("{{ cookiecutter.project_slug }}.batch.dags.example_dag")
    monkeypatch.setattr(module, "DEFAULT_DESTINATION", tmp_path / "output")
    dag = example_events_dag()

    with pytest.raises(SchemaDriftError, match="customer_name"):
        dag.get_task("load").python_callable([{"id": 1, "customer_name": "example"}])
```

- [ ] **Step 3: Add `test_events.py` to the lakehouse gated list**

In `hooks/post_gen_project.py`, in the `include_lakehouse` entry's `"gated"`
list, add `"tests/test_events.py",` after `"tests/test_batch_dag.py",`.

- [ ] **Step 4: Bake and run the new tests to see them fail**

```bash
# (define bake() first; see "Paths used below")
P=$(bake "$(mktemp -d)" on)
(cd "$P" && uv sync -q && uv run pytest -q tests/test_events.py tests/test_batch_dag.py tests/test_streaming_consumer.py)
```

Expected: FAIL with `ImportError: cannot import name 'SchemaDriftError'`.

- [ ] **Step 5: Implement in `PKG/events.py`**

Replace the whole file with:

```python
from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any


class SchemaDriftError(ValueError):
    """A record lacks a field the Event contract declares.

    Its message is the healing agent's starting evidence, so it names what
    is missing, what arrived instead, and what the contract declares.
    """


@dataclass(frozen=True, slots=True)
class Event:
    id: int
    name: str

    def __post_init__(self) -> None:
        if isinstance(self.id, bool) or not isinstance(self.id, int):
            raise TypeError("event id must be an integer")
        if not isinstance(self.name, str):
            raise TypeError("event name must be a string")
        if not self.name:
            raise ValueError("event name must not be empty")

    @classmethod
    def from_mapping(cls, record: Mapping[str, Any]) -> "Event":
        declared = [field.name for field in fields(cls)]
        missing = [name for name in declared if name not in record]
        if missing:
            # Extra fields alone are not drift: a producer may add one before
            # the contract learns about it. Only a missing field is.
            undeclared = [key for key in record if key not in declared]
            detail = f"record lacks field(s) {missing}"
            if undeclared:
                detail += f" and carries undeclared field(s) {undeclared}"
            raise SchemaDriftError(
                f"{detail}; the Event contract declares {declared}. "
                "Either the producer renamed a field or the contract is out of date."
            )
        return cls(id=record["id"], name=record["name"])
```

- [ ] **Step 6: Re-bake and run the generated tests**

```bash
# (define bake() first)
for lh in on off; do
  P=$(bake "$(mktemp -d)" $lh)
  (cd "$P" && uv sync -q && uv run pytest -q && uv run ruff check .)
done
```

Expected: all pass in both bakes. The lakehouse-off bake renders no DDL
test. The run includes `evals/`, whose fixture is still the old synthetic
DDL; Task 3 changes that.

- [ ] **Step 7: Run the template tiers, including the red seam test from Task 1**

```bash
uv run pytest -m "not slow" -q
uv run ruff check .
uv run pytest -m slow -q -k "test_demo_failure_triggers_the_healer_and_lands_a_patch or test_batch_only_bake_generated_tests_pass or test_streaming_only_bake_generated_tests_pass"
```

Expected: all pass, including both seam cases.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: detect schema drift on the Event contract

Event.from_mapping raises SchemaDriftError, a ValueError, when a
declared field is missing, naming the missing, undeclared and declared
fields. Extra fields alone are still accepted. The demo failure now
fires with or without the Lakehouse Pattern, and a lakehouse-on test
keeps the contract a subset of the DDL."
```

---

### Task 3: Point the evals at the Event contract and drop missing_config

**Files:**
- Modify: `SLUG_DIR/evals/conftest.py`
- Delete: `SLUG_DIR/evals/cases/missing_config.yaml`, `SLUG_DIR/evals/cassettes/missing_config.json`
- Modify: `SLUG_DIR/evals/cassettes/schema_drift.json`, `SLUG_DIR/evals/cassettes/schema_drift_broken_patch.json`
- Modify: `SLUG_DIR/evals/test_cases.py`
- Modify: `tests/test_bake_agentic.py`

**Interfaces:**
- Consumes: the `SchemaDriftError` message format from Task 2. The real
  `Event` is importable in a generated project as
  `{{ cookiecutter.project_slug }}.events`.
- Produces: `evals/conftest.py` exports `SCHEMA_DRIFT_LOG`, `EVENT_SOURCE`,
  `FAILURE_EXCEPTIONS = {"schema_drift": (type_name, message)}` and
  `build_case_project(tmp_path, case) -> tuple[Path, FailureContext]`, which
  keeps its signature.

- [ ] **Step 1: Write the failing tests in `evals/test_cases.py`**

Update the expected string in
`test_unknown_failure_mode_reports_the_case_and_supported_modes` to end with
`"expected one of: schema_drift"`.

Update the imports at the top of the file. Change
`from .conftest import build_case_project` to
`from .conftest import FAILURE_EXCEPTIONS, SCHEMA_DRIFT_LOG, build_case_project`,
and add `from {{ cookiecutter.project_slug }}.events import Event, SchemaDriftError`
directly after the `agentic.tracing` import line, where isort places it.
Then append:

```python


def test_the_fixture_log_matches_what_the_real_contract_raises():
    # The fixture is synthetic, so nothing else stops it drifting from the
    # runtime: replay would keep passing while the agent read a log the real
    # DAG can no longer produce.
    with pytest.raises(SchemaDriftError) as excinfo:
        Event.from_mapping({"id": 1, "customer_name": "example"})

    exception_type, exception_message = FAILURE_EXCEPTIONS["schema_drift"]
    assert exception_type == "SchemaDriftError"
    assert str(excinfo.value).startswith(exception_message)
    assert f"SchemaDriftError: {excinfo.value}" in SCHEMA_DRIFT_LOG
```

In `tests/test_bake_agentic.py`, edit `test_evals_present_when_enabled`: replace
the two `missing_config` asserts with:

```python
    assert not (evals / "cases" / "missing_config.yaml").exists()
    assert not (evals / "cassettes" / "missing_config.json").exists()
```

Then append, after `test_readme_documents_the_failure_mode_this_bake_actually_ships`:

```python
@pytest.mark.parametrize("lakehouse", [True, False])
def test_no_bake_ships_the_retired_missing_config_mode(cookies, lakehouse):
    result = bake_with(
        cookies, include_agentic=True, include_batch=True, include_lakehouse=lakehouse
    )
    project = result.project_path
    slug = result.context["project_slug"]

    assert not (project / "src" / slug / "batch" / "config.py").exists()
    assert not (project / "src" / slug / "batch" / "pipeline.toml").exists()
    assert not (project / "tests" / "test_batch_config.py").exists()
    offenders = [
        path.relative_to(project).as_posix()
        for path in project.rglob("*")
        if path.is_file() and "missing_config" in path.read_text(errors="ignore")
    ]
    assert offenders == []
```

- [ ] **Step 2: Run to see them fail**

```bash
uv run pytest -q tests/test_bake_agentic.py -k "evals_present or retired_missing_config"
# (define bake() first)
P=$(bake "$(mktemp -d)" on)
(cd "$P" && uv sync -q && uv run pytest -q evals/test_cases.py)
```

Expected: the bake tests FAIL, finding `evals/cases/missing_config.yaml`. The
eval tests FAIL on the supported-modes string, and on
`test_the_fixture_log_matches...` because the fixture still uses the old message.

- [ ] **Step 3: Rewrite `SLUG_DIR/evals/conftest.py`**

Replace the whole file with:

```python
"""Builds a throwaway project tree matching a case's failure_mode.

The tree is synthetic rather than a real bake, so a case is independent of
which Patterns this project was generated with. It still mirrors the shape
the demo ships: a DAG whose extract() emits a renamed field, and the Event
contract that load() validates each record against.
"""

from pathlib import Path

from {{ cookiecutter.project_slug }}.agentic.contracts import FailureContext

SCHEMA_DRIFT_DAG = '''from airflow.sdk import dag, task


@dag()
def example_events_dag():
    @task
    def extract() -> list[dict]:
        return [{"id": 1, "customer_name": "example"}]
'''

EVENT_SOURCE = '''from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any


class SchemaDriftError(ValueError):
    """A record lacks a field the Event contract declares."""


@dataclass(frozen=True, slots=True)
class Event:
    id: int
    name: str

    @classmethod
    def from_mapping(cls, record: Mapping[str, Any]) -> "Event":
        declared = [field.name for field in fields(cls)]
        missing = [name for name in declared if name not in record]
        if missing:
            raise SchemaDriftError(f"record lacks field(s) {missing}")
        return cls(id=record["id"], name=record["name"])
'''

SCHEMA_DRIFT_MESSAGE = (
    "record lacks field(s) ['name'] and carries undeclared field(s) "
    "['customer_name']; the Event contract declares ['id', 'name']. "
    "Either the producer renamed a field or the contract is out of date."
)

SCHEMA_DRIFT_LOG = (
    "[2026-07-29T00:00:00] INFO - Task load starting\n"
    "[2026-07-29T00:00:01] ERROR - Task failed with exception\n"
    "Traceback (most recent call last):\n"
    f"SchemaDriftError: {SCHEMA_DRIFT_MESSAGE}\n"
)

FAILURE_EXCEPTIONS = {
    "schema_drift": ("SchemaDriftError", SCHEMA_DRIFT_MESSAGE),
}


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def build_case_project(tmp_path: Path, case) -> tuple[Path, FailureContext]:
    if case.failure_mode not in FAILURE_EXCEPTIONS:
        supported = ", ".join(sorted(FAILURE_EXCEPTIONS))
        raise ValueError(
            f"eval case {case.id!r} has unknown failure_mode {case.failure_mode!r}; "
            f"expected one of: {supported}"
        )

    _write(tmp_path / "src" / "pkg" / "batch" / "dags" / "example_dag.py", SCHEMA_DRIFT_DAG)
    _write(tmp_path / "src" / "pkg" / "events.py", EVENT_SOURCE)
    (tmp_path / "load.log").write_text(SCHEMA_DRIFT_LOG)
    exception_type, exception_message = FAILURE_EXCEPTIONS[case.failure_mode]

    ctx = FailureContext(
        dag_id="example_events_dag",
        dag_run_id=f"eval_{case.id}",
        task_id="load",
        try_number=1,
        exception_type=exception_type,
        exception_message=exception_message,
        log_path=str(tmp_path / "load.log"),
        project_root=str(tmp_path),
    )
    return tmp_path, ctx
```

`SCHEMA_DRIFT_DAG` keeps `customer_name` exactly once. The cassette's
`propose_patch` uses it as `old_str`, which must be unique.

- [ ] **Step 4: Update the cassettes and delete the missing_config pair**

```bash
git rm -q '{{cookiecutter.project_slug}}/evals/cases/missing_config.yaml' \
          '{{cookiecutter.project_slug}}/evals/cassettes/missing_config.json'
```

In both `schema_drift.json` and `schema_drift_broken_patch.json`, make these edits:

- The third tool call becomes
  `{"name": "read_repo_file", "input": {"path": "src/pkg/events.py"}},`
- `diagnosis.root_cause` becomes
  `"extract() emits a record keyed customer_name, but the Event contract in events.py declares name. The producer renamed the field without updating the contract."`

In `schema_drift.json` only, change the `propose_patch` `rationale` to
`"The Event contract declares `name`; extract() emits `customer_name`."`.
Leave `schema_drift_broken_patch.json`'s deliberately wrong `old_str` and its
rationale alone.

- [ ] **Step 5: Run the evals and template tests**

```bash
# (define bake() first)
for lh in on off; do
  P=$(bake "$(mktemp -d)" $lh)
  (cd "$P" && uv sync -q && uv run pytest -q && uv run ruff check .)
done
uv run pytest -m "not slow" -q
```

Expected: all pass in both bakes. The replay tier's `exact_tool_sequence`
still holds because the cassette keeps four calls in the same order.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test: replay evals against the Event contract and drop missing_config

The synthetic eval project now carries events.py instead of the DDL,
its log uses SchemaDriftError's real message, and a test keeps the two
from drifting apart. The missing_config case and cassette go with the
failure mode they exercised."
```

---

### Task 4: Update the docs and the tool description

**Files:**
- Modify: `PKG/agentic/tools.py:310` (the `read_repo_file` description)
- Modify: `docs/agentic-pattern-walkthrough.html:670-692`
- Modify: `docs/agentic-pattern-verification.html:776-778`
- Modify: `docs/superpowers/specs/2026-10-13-agentic-merge-to-main-design.md` (status)

**Interfaces:** none. This is text the agent and readers see.

- [ ] **Step 1: Tool description**

In `tools.py`, change the `read_repo_file` description's second sentence to:

```python
            "the DAG that produced the bad record and the contract it was validated against."
```

The model reads this description. Pointing it at "the DDL" would send it to
a file that lakehouse-off projects do not have.

- [ ] **Step 2: Walkthrough**

In `docs/agentic-pattern-walkthrough.html`:

- In the "the drift" code block, replace the `+ CREATE TABLE ...` line with
  `<span class="add">+ class Event: id: int; name: str  # declared</span>`.
- Replace the following `<p>` with:

```html
      <p>
        The validation that catches it is <em>derived from the contract</em>, not hardcoded.
        <code>Event.from_mapping</code> reads the declared fields off the <code>Event</code>
        dataclass in <code>events.py</code>, the one type every sink accepts, so the runtime
        error and the eval's ground truth cannot drift apart. The exception names the
        missing field, the undeclared one that arrived instead, and the declared fields,
        because that message is the agent's primary evidence.
      </p>
```

- Replace the "Two configurations, two failures" `<div class="note">` with:

```html
      <div class="note">
        <span class="note-label">One failure, every configuration</span>
        <p>
          Because drift is caught on the contract rather than against the DuckDB DDL, the
          same <code>schema_drift</code> mode fires with the Lakehouse Pattern on or off.
          When it is on, a generated test keeps the contract a subset of the table's columns.
        </p>
      </div>
```

- [ ] **Step 3: Verification guide**

In `docs/agentic-pattern-verification.html`, change
`<span class="cmt"># with this run config (all Patterns on):</span>` to
`<span class="cmt"># with this run config (any Pattern combination with Batch):</span>`.
Then delete the two lines that follow `{"failure_mode": "schema_drift"}`:
the `With the Lakehouse Pattern off` comment and the `missing_config` line.
Keep the closing `</code></pre>` on the `schema_drift` line.

- [ ] **Step 4: Spec status**

In the spec, change `**Status:** Draft for review` to `**Status:** Approved`.
Under "Resolving the 10 conflicts", in the `hooks/post_gen_project.py` row,
replace `Add \`events.py\` to the agentic \`gated\` list.` with
`Add \`tests/test_events.py\` to the lakehouse \`gated\` list (its DDL-subset test is gated).`

- [ ] **Step 5: Verify and commit**

```bash
git grep -n missing_config -- . ':!docs/superpowers'
uv run pytest -m "not slow" -q
```

Expected: no `missing_config` hits outside `docs/superpowers`; tests pass.

```bash
git add -A
git commit -m "docs: describe drift detection on the Event contract"
```

---

### Task 5: Full verification

No new code. This task is the evidence the PR quotes.

- [ ] **Step 1: Template suite, both tiers**

```bash
uv run pytest -q                 # fast + slow
uv run ruff check .
git diff --check main...HEAD
```

Expected: all pass. Record the pass counts for the PR.

- [ ] **Step 2: Generated projects, agentic on, lakehouse on and off**

```bash
# (define bake() first)
BAKES=$(mktemp)   # keep this shell open for Step 3
for lh in on off; do
  P=$(bake "$(mktemp -d)" $lh)
  (cd "$P" && uv sync -q && uv run pytest -q && uv run ruff check .)
  echo "$P" >> "$BAKES"
done
```

Expected: both pass. Record the counts.

- [ ] **Step 3: Whitespace review of the edited templates**

PR #2 shipped a blank-line defect at a `{%-` boundary, so render each edited
template and read it:

```bash
while read -r P; do
  sed -n 1,30p "$P"/src/*/batch/dags/example_dag.py
  sed -n 1,40p "$P"/README.md
done < "$BAKES"
```

Expected: two blank lines before each top-level `def`/decorator. There
should be no run of three blank lines, and the README should have no stray
blank lines or orphaned code fences.

- [ ] **Step 4: Check the default bake leaves agentic out**

```bash
P=$(uv run python -c 'from cookiecutter.main import cookiecutter; import tempfile; print(cookiecutter(".", no_input=True, output_dir=tempfile.mkdtemp()))')
test ! -d "$P/evals" && echo "agentic off by default"
```

---

### Task 6: Ship (needs the user's go-ahead)

Pushing and opening a PR are outward-facing. Ask the user before Step 1.

- [ ] **Step 1: Push the branch**

```bash
git push -u origin agentic-self-healing-pattern
```

- [ ] **Step 2: Open the PR**

`gh` is not authenticated on this machine. Either the user runs
`! gh auth login` and then runs `gh pr create --base main --title "feat: agentic self-healing Pattern" --body-file <file>`,
or the user opens
`https://github.com/vincyf1/cookiecutter-data-scaffold/compare/main...agentic-self-healing-pattern`.

The PR body covers:

- what the Pattern does
- that `include_agentic` defaults to false
- the three merge decisions from the spec
- the credential guard
- the test counts from Task 5

It carries no Claude attribution.

- [ ] **Step 3: Merge with a merge commit after review**

Use "Create a merge commit" in the GitHub UI, or
`gh pr merge --merge`.
