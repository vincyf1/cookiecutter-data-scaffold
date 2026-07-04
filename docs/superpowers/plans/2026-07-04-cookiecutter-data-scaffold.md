# Cookiecutter Data Scaffold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a cookiecutter template that generates data engineering project scaffolds from four independent, combinable Patterns (Batch, Streaming, Lakehouse, Transformation), each composing with the others when jointly enabled.

**Architecture:** A `cookiecutter.json` exposes four boolean flags (`include_batch`, `include_streaming`, `include_lakehouse`, `include_dbt`). Shared/baseline files (pyproject.toml, docker-compose.yml, CI workflow, pre-commit config) use inline Jinja `{% if %}` blocks to assemble pattern-specific sections within a single template file. Whole pattern-owned directories (e.g. `src/{{project_slug}}/batch/`) are always rendered by cookiecutter and then pruned by `hooks/post_gen_project.py` when their flag is off — directory existence is a hook concern, shared-file content assembly is a Jinja concern. Every combination of the four flags (16 total) is valid; there is no cross-flag validation.

**Tech Stack:** cookiecutter 2.2+, pytest + pytest-cookies (testing the template itself), uv + Ruff + pytest (baseline tooling in generated projects), Apache Airflow (Batch), Kafka + confluent-kafka-python (Streaming), Apache Iceberg + DuckDB (Lakehouse), dbt-duckdb (Transformation).

## Global Constraints

- Python 3.12 fixed in generated projects (not a cookiecutter prompt).
- Dependency manager: uv. Linter/formatter: Ruff. CI: GitHub Actions.
- Pattern flags are booleans (JSON `true`/`false`, not `"y"/"n"` strings) — requires cookiecutter>=2.2.
- All 16 combinations of `include_batch`/`include_streaming`/`include_lakehouse`/`include_dbt` are valid. No pre_gen validation rejects any combination.
- Scope is local/dev only for v1: no cloud provider, no Terraform/IaC, no server-based query engines (DuckDB only, not Trino/ClickHouse).
- Lakehouse and Transformation are independent of each other, but compose when both enabled (dbt sources reference lakehouse tables). Batch/Streaming compose with Lakehouse via a shared `lakehouse/writer.py`; without Lakehouse, they fall back to a default Parquet sink.
- dbt testing stays dbt-native (schema tests), not wrapped in pytest. Batch/Streaming/Lakehouse get pytest example tests.
- Pre-commit is always scaffolded (not a flag): generic hygiene hooks + Ruff always; sqlfluff added only when `include_dbt` is true.
- SQL migrations for DuckDB/Lakehouse are explicitly out of scope for this plan — deferred.

---

## File Structure

This repo (the template source):

```
cookiecutter.json
hooks/
  post_gen_project.py
pyproject.toml                          # this repo's own dev tooling (pytest, pytest-cookies, ruff)
tests/
  conftest.py
  test_bake_baseline.py
  test_bake_batch.py
  test_bake_streaming.py
  test_bake_lakehouse.py
  test_bake_dbt.py
  test_bake_integration.py
{{cookiecutter.project_slug}}/
  pyproject.toml
  README.md
  .gitignore
  .pre-commit-config.yaml
  docker-compose.yml
  .github/workflows/ci.yml
  src/{{cookiecutter.project_slug}}/
    __init__.py
    batch/
      __init__.py
      dags/example_dag.py
      sinks.py
    streaming/
      __init__.py
      consumer.py
      sinks.py
    lakehouse/
      __init__.py
      create_tables.sql
      writer.py
  tests/
    test_batch_dag.py
    test_streaming_consumer.py
    test_lakehouse_writer.py
  transformation/
    dbt_project.yml
    profiles.yml
    models/staging/
      stg_events.sql
      schema.yml
      sources.yml
    seeds/
      events_seed.csv
```

Each pattern's owned tree (`batch/`, `streaming/`, `lakehouse/`, `transformation/` and their matching test files) is always emitted by cookiecutter, then pruned in `hooks/post_gen_project.py` based on the flags. Shared files (`pyproject.toml`, `docker-compose.yml`, `.pre-commit-config.yaml`, `.github/workflows/ci.yml`) always exist and use inline `{% if %}` blocks.

The running example entity across all patterns is a table/topic called **`events`**, so Batch writes `events`, Streaming consumes `events`, Lakehouse creates an `events` Iceberg table, and dbt stages `stg_events`.

---

## Task 1: Template test harness + minimal cookiecutter.json

**Files:**
- Create: `pyproject.toml`
- Create: `cookiecutter.json`
- Create: `{{cookiecutter.project_slug}}/README.md`
- Create: `tests/conftest.py`
- Test: `tests/test_bake_baseline.py`

**Interfaces:**
- Produces: `cookiecutter.json` keys `project_name`, `project_slug`, `author_name` — every later task adds keys to this same file.

- [ ] **Step 1: Write this repo's own pyproject.toml**

```toml
[project]
name = "cookiecutter-data-scaffold"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "cookiecutter>=2.2",
]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-cookies>=0.7",
    "ruff>=0.6",
]

[tool.ruff]
target-version = "py312"
line-length = 100
```

- [ ] **Step 2: Install dependencies**

Run: `uv sync`
Expected: lockfile created, `.venv` populated, no errors.

- [ ] **Step 3: Write the failing test**

```python
# tests/test_bake_baseline.py
def test_default_bake_succeeds(cookies):
    result = cookies.bake()
    assert result.exit_code == 0
    assert result.exception is None
    assert result.project_path.is_dir()
    assert (result.project_path / "README.md").is_file()
```

```python
# tests/conftest.py
# pytest-cookies auto-registers the `cookies` fixture; no fixtures needed here yet.
```

- [ ] **Step 4: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_baseline.py -v`
Expected: FAIL — `cookiecutter.json` does not exist yet.

- [ ] **Step 5: Write cookiecutter.json**

```json
{
  "project_name": "My Data Project",
  "project_slug": "{{ cookiecutter.project_name.lower().replace(' ', '_').replace('-', '_') }}",
  "author_name": "Your Name"
}
```

- [ ] **Step 6: Write the minimal template README**

```markdown
# {{ cookiecutter.project_name }}

Generated by cookiecutter-data-scaffold.
```

- [ ] **Step 7: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_baseline.py -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml cookiecutter.json tests/conftest.py tests/test_bake_baseline.py "{{cookiecutter.project_slug}}/README.md"
git commit -m "feat: minimal cookiecutter template with test harness"
```

---

## Task 2: Four pattern flags + directory pruning hook

**Files:**
- Modify: `cookiecutter.json`
- Create: `hooks/post_gen_project.py`
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/__init__.py`
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/__init__.py`
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/__init__.py`
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/__init__.py`
- Create: `{{cookiecutter.project_slug}}/transformation/dbt_project.yml`
- Test: `tests/test_bake_baseline.py` (extend)

**Interfaces:**
- Consumes: `cookiecutter.json` from Task 1.
- Produces: flags `include_batch`, `include_streaming`, `include_lakehouse`, `include_dbt` (all `true` by default) — every later task's `{% if cookiecutter.include_X %}` blocks reference these exact names. Produces pruning convention in `hooks/post_gen_project.py`: a `REMOVAL_RULES` list of `(flag_bool, [paths])` tuples that later tasks append entries to.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bake_baseline.py (append)
def test_all_patterns_off_removes_pattern_dirs(cookies):
    result = cookies.bake(extra_context={
        "include_batch": False,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": False,
    })
    assert result.exit_code == 0
    project = result.project_path
    slug = result.context["project_slug"]
    assert not (project / "src" / slug / "batch").exists()
    assert not (project / "src" / slug / "streaming").exists()
    assert not (project / "src" / slug / "lakehouse").exists()
    assert not (project / "transformation").exists()


def test_all_patterns_on_keeps_pattern_dirs(cookies):
    result = cookies.bake(extra_context={
        "include_batch": True,
        "include_streaming": True,
        "include_lakehouse": True,
        "include_dbt": True,
    })
    assert result.exit_code == 0
    project = result.project_path
    slug = result.context["project_slug"]
    assert (project / "src" / slug / "batch").is_dir()
    assert (project / "src" / slug / "streaming").is_dir()
    assert (project / "src" / slug / "lakehouse").is_dir()
    assert (project / "transformation").is_dir()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_baseline.py -v`
Expected: FAIL — flags don't exist in `cookiecutter.json`, `KeyError` on `result.context["project_slug"]` extra_context, or missing directories.

- [ ] **Step 3: Add flags to cookiecutter.json**

```json
{
  "project_name": "My Data Project",
  "project_slug": "{{ cookiecutter.project_name.lower().replace(' ', '_').replace('-', '_') }}",
  "author_name": "Your Name",
  "include_batch": true,
  "include_streaming": true,
  "include_lakehouse": true,
  "include_dbt": true
}
```

- [ ] **Step 4: Create the pattern-owned package stubs**

```python
# {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/__init__.py
```

```python
# {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/__init__.py
```

```python
# {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/__init__.py
```

```python
# {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/__init__.py
```

```yaml
# {{cookiecutter.project_slug}}/transformation/dbt_project.yml
name: '{{ cookiecutter.project_slug }}'
version: '1.0.0'
profile: '{{ cookiecutter.project_slug }}'
model-paths: ["models"]
seed-paths: ["seeds"]
```

- [ ] **Step 5: Write the pruning hook**

```python
# hooks/post_gen_project.py
import os
import shutil

PROJECT_DIR = os.path.realpath(os.curdir)
SLUG = "{{ cookiecutter.project_slug }}"

REMOVAL_RULES = [
    ("{{ cookiecutter.include_batch }}" == "True", [
        f"src/{SLUG}/batch",
        "tests/test_batch_dag.py",
    ]),
    ("{{ cookiecutter.include_streaming }}" == "True", [
        f"src/{SLUG}/streaming",
        "tests/test_streaming_consumer.py",
    ]),
    ("{{ cookiecutter.include_lakehouse }}" == "True", [
        f"src/{SLUG}/lakehouse",
        "tests/test_lakehouse_writer.py",
    ]),
    ("{{ cookiecutter.include_dbt }}" == "True", [
        "transformation",
    ]),
]


def remove(relative_path):
    full_path = os.path.join(PROJECT_DIR, relative_path)
    if os.path.isdir(full_path):
        shutil.rmtree(full_path)
    elif os.path.isfile(full_path):
        os.remove(full_path)


def main():
    for keep, paths in REMOVAL_RULES:
        if not keep:
            for path in paths:
                remove(path)


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_baseline.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add cookiecutter.json hooks/post_gen_project.py "{{cookiecutter.project_slug}}/src" "{{cookiecutter.project_slug}}/transformation/dbt_project.yml" tests/test_bake_baseline.py
git commit -m "feat: add pattern flags and directory pruning hook"
```

---

## Task 3: Baseline pyproject.toml (uv + Ruff + Python 3.12, conditional deps)

**Files:**
- Create: `{{cookiecutter.project_slug}}/pyproject.toml`
- Test: `tests/test_bake_baseline.py` (extend)

**Interfaces:**
- Consumes: flags from Task 2.
- Produces: generated project's `[project.dependencies]` list — later tasks (Batch/Streaming/Lakehouse/dbt) don't modify this file again; all pattern dependencies are declared here in one place, gated by the same `{% if %}` blocks.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bake_baseline.py (append)
def test_pyproject_declares_only_enabled_pattern_deps(cookies):
    result = cookies.bake(extra_context={
        "include_batch": True,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": False,
    })
    pyproject = (result.project_path / "pyproject.toml").read_text()
    assert "apache-airflow" in pyproject
    assert "confluent-kafka" not in pyproject
    assert "pyiceberg" not in pyproject
    assert "dbt-duckdb" not in pyproject
    assert 'requires-python = ">=3.12"' in pyproject
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_baseline.py -v`
Expected: FAIL — `{{cookiecutter.project_slug}}/pyproject.toml` does not exist.

- [ ] **Step 3: Write the template**

```toml
# {{cookiecutter.project_slug}}/pyproject.toml
[project]
name = "{{ cookiecutter.project_slug }}"
version = "0.1.0"
description = "{{ cookiecutter.project_name }}"
authors = [{ name = "{{ cookiecutter.author_name }}" }]
requires-python = ">=3.12"
dependencies = [
    "duckdb>=1.0",
{%- if cookiecutter.include_batch %}
    "apache-airflow>=2.9",
{%- endif %}
{%- if cookiecutter.include_streaming %}
    "confluent-kafka>=2.4",
{%- endif %}
{%- if cookiecutter.include_lakehouse %}
    "pyiceberg>=0.7",
{%- endif %}
{%- if cookiecutter.include_dbt %}
    "dbt-duckdb>=1.8",
{%- endif %}
]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "ruff>=0.6",
]

[tool.ruff]
target-version = "py312"
line-length = 100
src = ["src"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_baseline.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add "{{cookiecutter.project_slug}}/pyproject.toml" tests/test_bake_baseline.py
git commit -m "feat: baseline pyproject.toml with conditional pattern dependencies"
```

---

## Task 4: Baseline pre-commit config (hygiene + Ruff always, sqlfluff conditional)

**Files:**
- Create: `{{cookiecutter.project_slug}}/.pre-commit-config.yaml`
- Test: `tests/test_bake_baseline.py` (extend)

**Interfaces:**
- Consumes: `include_dbt` flag from Task 2.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bake_baseline.py (append)
def test_precommit_includes_sqlfluff_only_when_dbt_enabled(cookies):
    with_dbt = cookies.bake(extra_context={"include_dbt": True})
    without_dbt = cookies.bake(extra_context={"include_dbt": False})

    with_dbt_config = (with_dbt.project_path / ".pre-commit-config.yaml").read_text()
    without_dbt_config = (without_dbt.project_path / ".pre-commit-config.yaml").read_text()

    assert "sqlfluff" in with_dbt_config
    assert "sqlfluff" not in without_dbt_config
    assert "ruff-pre-commit" in with_dbt_config
    assert "ruff-pre-commit" in without_dbt_config
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_baseline.py -v`
Expected: FAIL — `.pre-commit-config.yaml` does not exist.

- [ ] **Step 3: Write the template**

```yaml
# {{cookiecutter.project_slug}}/.pre-commit-config.yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.6.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.6.9
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
{%- if cookiecutter.include_dbt %}

  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 3.1.0
    hooks:
      - id: sqlfluff-lint
        files: ^transformation/models/
{%- endif %}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_baseline.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add "{{cookiecutter.project_slug}}/.pre-commit-config.yaml" tests/test_bake_baseline.py
git commit -m "feat: baseline pre-commit config with conditional sqlfluff"
```

---

## Task 5: Baseline CI workflow (lint + pytest always, dbt test conditional)

**Files:**
- Create: `{{cookiecutter.project_slug}}/.github/workflows/ci.yml`
- Test: `tests/test_bake_baseline.py` (extend)

**Interfaces:**
- Consumes: `include_dbt` flag from Task 2.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bake_baseline.py (append)
def test_ci_includes_dbt_test_step_only_when_dbt_enabled(cookies):
    with_dbt = cookies.bake(extra_context={"include_dbt": True})
    without_dbt = cookies.bake(extra_context={"include_dbt": False})

    with_dbt_ci = (with_dbt.project_path / ".github" / "workflows" / "ci.yml").read_text()
    without_dbt_ci = (without_dbt.project_path / ".github" / "workflows" / "ci.yml").read_text()

    assert "dbt test" in with_dbt_ci
    assert "dbt test" not in without_dbt_ci
    assert "ruff check" in with_dbt_ci
    assert "pytest" in with_dbt_ci
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_baseline.py -v`
Expected: FAIL — `.github/workflows/ci.yml` does not exist.

- [ ] **Step 3: Write the template**

```yaml
# {{cookiecutter.project_slug}}/.github/workflows/ci.yml
name: CI

on:
  push:
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv sync
      - run: uv run ruff check .
      - run: uv run pytest
{%- if cookiecutter.include_dbt %}
      - name: dbt deps
        working-directory: transformation
        run: uv run dbt deps
      - name: dbt build
        working-directory: transformation
        run: uv run dbt build
      - name: dbt test
        working-directory: transformation
        run: uv run dbt test
{%- endif %}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_baseline.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add "{{cookiecutter.project_slug}}/.github/workflows/ci.yml" tests/test_bake_baseline.py
git commit -m "feat: baseline CI workflow with conditional dbt test step"
```

---

## Task 6: Batch pattern (Airflow DAG + default Parquet sink)

**Files:**
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/dags/example_dag.py`
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/sinks.py`
- Create: `{{cookiecutter.project_slug}}/tests/test_batch_dag.py`
- Test: `tests/test_bake_batch.py`

**Interfaces:**
- Produces: `sinks.write_events(records: list[dict]) -> None` in `batch/sinks.py` — Task 9 modifies this file's body (not its signature) to branch on `include_lakehouse`.

- [ ] **Step 1: Write the failing test (this repo's test, asserting generated content)**

```python
# tests/test_bake_batch.py
def test_batch_dag_present_when_enabled(cookies):
    result = cookies.bake(extra_context={"include_batch": True})
    slug = result.context["project_slug"]
    dag_path = result.project_path / "src" / slug / "batch" / "dags" / "example_dag.py"
    assert dag_path.is_file()
    assert "events" in dag_path.read_text()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_batch.py -v`
Expected: FAIL — `example_dag.py` does not exist.

- [ ] **Step 3: Write the default sink**

```python
# {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/sinks.py
"""Default output sink for the batch pipeline."""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_DIR = Path("data/events")


def write_events(records: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records)
    pq.write_table(table, OUTPUT_DIR / "events.parquet")
```

- [ ] **Step 4: Write the example DAG**

```python
# {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/dags/example_dag.py
from datetime import datetime

from airflow.decorators import dag, task

from {{ cookiecutter.project_slug }}.batch.sinks import write_events


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def example_events_dag():
    @task
    def extract() -> list[dict]:
        return [{"id": 1, "name": "example"}]

    @task
    def load(records: list[dict]) -> None:
        write_events(records)

    load(extract())


example_events_dag()
```

- [ ] **Step 5: Write the generated project's example test**

```python
# {{cookiecutter.project_slug}}/tests/test_batch_dag.py
from {{ cookiecutter.project_slug }}.batch.dags.example_dag import example_events_dag


def test_dag_has_no_import_errors():
    dag = example_events_dag()
    assert dag.dag_id == "example_events_dag"
    assert len(dag.tasks) == 2
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_batch.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add "{{cookiecutter.project_slug}}/src" "{{cookiecutter.project_slug}}/tests/test_batch_dag.py" tests/test_bake_batch.py
git commit -m "feat: batch pattern with Airflow DAG and default Parquet sink"
```

---

## Task 7: Streaming pattern (Kafka consumer + default Parquet sink)

**Files:**
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/consumer.py`
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/sinks.py`
- Create: `{{cookiecutter.project_slug}}/tests/test_streaming_consumer.py`
- Test: `tests/test_bake_streaming.py`

**Interfaces:**
- Produces: `sinks.write_events(records: list[dict]) -> None` in `streaming/sinks.py` (mirrors Batch's, independent implementation) — Task 9 modifies its body to branch on `include_lakehouse`. Produces `consumer.handle_message(raw_value: bytes) -> dict` in `consumer.py` — the generated test in Step 5 calls this exact function.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bake_streaming.py
def test_streaming_consumer_present_when_enabled(cookies):
    result = cookies.bake(extra_context={"include_streaming": True})
    slug = result.context["project_slug"]
    consumer_path = result.project_path / "src" / slug / "streaming" / "consumer.py"
    assert consumer_path.is_file()
    assert "confluent_kafka" in consumer_path.read_text()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_streaming.py -v`
Expected: FAIL — `consumer.py` does not exist.

- [ ] **Step 3: Write the default sink**

```python
# {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/sinks.py
"""Default output sink for the streaming consumer."""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_DIR = Path("data/events")


def write_events(records: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records)
    pq.write_table(table, OUTPUT_DIR / "events_stream.parquet", compression="snappy")
```

- [ ] **Step 4: Write the consumer**

```python
# {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/consumer.py
import json

from confluent_kafka import Consumer

from {{ cookiecutter.project_slug }}.streaming.sinks import write_events

TOPIC = "events"


def handle_message(raw_value: bytes) -> dict:
    return json.loads(raw_value)


def run(bootstrap_servers: str = "localhost:9092") -> None:
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "{{ cookiecutter.project_slug }}-consumer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            record = handle_message(msg.value())
            write_events([record])
    finally:
        consumer.close()
```

- [ ] **Step 5: Write the generated project's example test**

```python
# {{cookiecutter.project_slug}}/tests/test_streaming_consumer.py
from {{ cookiecutter.project_slug }}.streaming.consumer import handle_message


def test_handle_message_parses_json():
    raw = b'{"id": 1, "name": "example"}'
    assert handle_message(raw) == {"id": 1, "name": "example"}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_streaming.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add "{{cookiecutter.project_slug}}/src" "{{cookiecutter.project_slug}}/tests/test_streaming_consumer.py" tests/test_bake_streaming.py
git commit -m "feat: streaming pattern with Kafka consumer and default Parquet sink"
```

---

## Task 8: Lakehouse pattern (Iceberg tables via DuckDB + writer)

**Files:**
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/create_tables.sql`
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/writer.py`
- Create: `{{cookiecutter.project_slug}}/tests/test_lakehouse_writer.py`
- Test: `tests/test_bake_lakehouse.py`

**Interfaces:**
- Produces: `writer.write_events(con, records: list[dict]) -> None` in `lakehouse/writer.py` — this exact signature is what Task 9 imports and calls from Batch's and Streaming's `sinks.py` when `include_lakehouse` is true.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bake_lakehouse.py
def test_lakehouse_writer_present_when_enabled(cookies):
    result = cookies.bake(extra_context={"include_lakehouse": True})
    slug = result.context["project_slug"]
    writer_path = result.project_path / "src" / slug / "lakehouse" / "writer.py"
    assert writer_path.is_file()
    assert "def write_events(" in writer_path.read_text()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_lakehouse.py -v`
Expected: FAIL — `writer.py` does not exist.

- [ ] **Step 3: Write the table DDL**

```sql
-- {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/create_tables.sql
INSTALL iceberg;
LOAD iceberg;

CREATE TABLE IF NOT EXISTS events (
    id BIGINT,
    name VARCHAR,
    ingested_at TIMESTAMP DEFAULT now()
);
```

- [ ] **Step 4: Write the writer**

```python
# {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/writer.py
from pathlib import Path

import duckdb

DDL_PATH = Path(__file__).parent / "create_tables.sql"


def get_connection(db_path: str = "lakehouse.duckdb") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
    con.execute(DDL_PATH.read_text())
    return con


def write_events(con: duckdb.DuckDBPyConnection, records: list[dict]) -> None:
    for record in records:
        con.execute(
            "INSERT INTO events (id, name) VALUES (?, ?)",
            [record["id"], record["name"]],
        )
```

- [ ] **Step 5: Write the generated project's example test**

```python
# {{cookiecutter.project_slug}}/tests/test_lakehouse_writer.py
from {{ cookiecutter.project_slug }}.lakehouse.writer import get_connection, write_events


def test_write_events_inserts_rows(tmp_path):
    con = get_connection(str(tmp_path / "test.duckdb"))
    write_events(con, [{"id": 1, "name": "example"}])
    result = con.execute("SELECT id, name FROM events").fetchall()
    assert result == [(1, "example")]
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_lakehouse.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add "{{cookiecutter.project_slug}}/src" "{{cookiecutter.project_slug}}/tests/test_lakehouse_writer.py" tests/test_bake_lakehouse.py
git commit -m "feat: lakehouse pattern with Iceberg tables via DuckDB"
```

---

## Task 9: Wire Batch and Streaming to Lakehouse writer when both enabled

**Files:**
- Modify: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/sinks.py`
- Modify: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/sinks.py`
- Test: `tests/test_bake_integration.py`

**Interfaces:**
- Consumes: `write_events(con, records)` from `lakehouse/writer.py` (Task 8).
- Produces: `sinks.write_events(records: list[dict]) -> None` keeps its original signature from Tasks 6/7 in both branches — callers (the DAG, the consumer) never change.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bake_integration.py
def test_batch_sink_uses_lakehouse_writer_when_both_enabled(cookies):
    result = cookies.bake(extra_context={"include_batch": True, "include_lakehouse": True})
    slug = result.context["project_slug"]
    sinks = (result.project_path / "src" / slug / "batch" / "sinks.py").read_text()
    assert "lakehouse.writer" in sinks
    assert "pyarrow" not in sinks


def test_batch_sink_uses_parquet_when_lakehouse_disabled(cookies):
    result = cookies.bake(extra_context={"include_batch": True, "include_lakehouse": False})
    slug = result.context["project_slug"]
    sinks = (result.project_path / "src" / slug / "batch" / "sinks.py").read_text()
    assert "lakehouse.writer" not in sinks
    assert "pyarrow" in sinks


def test_streaming_sink_uses_lakehouse_writer_when_both_enabled(cookies):
    result = cookies.bake(extra_context={"include_streaming": True, "include_lakehouse": True})
    slug = result.context["project_slug"]
    sinks = (result.project_path / "src" / slug / "streaming" / "sinks.py").read_text()
    assert "lakehouse.writer" in sinks
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_integration.py -v`
Expected: FAIL — `sinks.py` files currently always use pyarrow, no conditional branch.

- [ ] **Step 3: Rewrite batch/sinks.py with the conditional branch**

```python
# {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/sinks.py
"""Output sink for the batch pipeline."""
{%- if cookiecutter.include_lakehouse %}
from {{ cookiecutter.project_slug }}.lakehouse.writer import get_connection, write_events as _write_to_lakehouse


def write_events(records: list[dict]) -> None:
    con = get_connection()
    _write_to_lakehouse(con, records)
{%- else %}
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_DIR = Path("data/events")


def write_events(records: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records)
    pq.write_table(table, OUTPUT_DIR / "events.parquet")
{%- endif %}
```

- [ ] **Step 4: Rewrite streaming/sinks.py with the same conditional branch**

```python
# {{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/sinks.py
"""Output sink for the streaming consumer."""
{%- if cookiecutter.include_lakehouse %}
from {{ cookiecutter.project_slug }}.lakehouse.writer import get_connection, write_events as _write_to_lakehouse


def write_events(records: list[dict]) -> None:
    con = get_connection()
    _write_to_lakehouse(con, records)
{%- else %}
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_DIR = Path("data/events")


def write_events(records: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records)
    pq.write_table(table, OUTPUT_DIR / "events_stream.parquet", compression="snappy")
{%- endif %}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_integration.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add "{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/sinks.py" "{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/sinks.py" tests/test_bake_integration.py
git commit -m "feat: wire batch and streaming sinks to lakehouse writer when enabled"
```

---

## Task 10: Transformation (dbt) pattern, independent of Lakehouse

**Files:**
- Create: `{{cookiecutter.project_slug}}/transformation/profiles.yml`
- Create: `{{cookiecutter.project_slug}}/transformation/seeds/events_seed.csv`
- Create: `{{cookiecutter.project_slug}}/transformation/models/staging/stg_events.sql`
- Create: `{{cookiecutter.project_slug}}/transformation/models/staging/schema.yml`
- Test: `tests/test_bake_dbt.py`

**Interfaces:**
- Produces: dbt source name `raw_events_seed` (from the seed) used by `stg_events.sql` — Task 11 adds a second, lakehouse-backed source and changes `stg_events.sql`'s `{% if %}` branch, not this task's seed-based branch.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bake_dbt.py
def test_dbt_project_present_when_enabled(cookies):
    result = cookies.bake(extra_context={"include_dbt": True, "include_lakehouse": False})
    project = result.project_path
    assert (project / "transformation" / "profiles.yml").is_file()
    assert (project / "transformation" / "models" / "staging" / "stg_events.sql").is_file()
    schema = (project / "transformation" / "models" / "staging" / "schema.yml").read_text()
    assert "not_null" in schema
    assert "unique" in schema
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_dbt.py -v`
Expected: FAIL — `transformation/profiles.yml` does not exist.

- [ ] **Step 3: Write profiles.yml (defaults to local DuckDB)**

```yaml
# {{cookiecutter.project_slug}}/transformation/profiles.yml
{{ cookiecutter.project_slug }}:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'dev.duckdb'
      threads: 4
```

- [ ] **Step 4: Write the seed**

File `{{cookiecutter.project_slug}}/transformation/seeds/events_seed.csv` — a plain CSV with no leading comment line (dbt requires the first line to be the header row):

```csv
id,name
1,example
2,another_example
```

- [ ] **Step 5: Write the staging model (seed-based branch; Task 11 adds the lakehouse branch)**

Note: cookiecutter Jinja-renders every template file by default, but `stg_events.sql` also needs to contain dbt's own `{{ source(...) }}` / `{{ ref(...) }}` Jinja for dbt to render later at `dbt build` time. Wrap the dbt-Jinja pieces in `{% raw %}...{% endraw %}` so cookiecutter's renderer passes them through literally, while the outer `{% if cookiecutter.include_lakehouse %}` stays live for cookiecutter to evaluate now:

```sql
-- {{cookiecutter.project_slug}}/transformation/models/staging/stg_events.sql
select
    id,
    name
from
{%- if cookiecutter.include_lakehouse %}
{% raw %}    {{ source('lakehouse', 'events') }}{% endraw %}
{%- else %}
{% raw %}    {{ ref('events_seed') }}{% endraw %}
{%- endif %}
```

- [ ] **Step 6: Write schema.yml with dbt-native tests**

```yaml
# {{cookiecutter.project_slug}}/transformation/models/staging/schema.yml
version: 2

models:
  - name: stg_events
    columns:
      - name: id
        tests: [not_null, unique]
      - name: name
        tests: [not_null]
```

- [ ] **Step 7: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_dbt.py -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add "{{cookiecutter.project_slug}}/transformation" tests/test_bake_dbt.py
git commit -m "feat: transformation pattern with dbt defaulting to local DuckDB"
```

---

## Task 11: Compose Transformation with Lakehouse (dbt sources reference Iceberg tables)

**Files:**
- Create: `{{cookiecutter.project_slug}}/transformation/models/staging/sources.yml`
- Modify: `{{cookiecutter.project_slug}}/transformation/models/staging/schema.yml`
- Test: `tests/test_bake_integration.py` (extend)

**Interfaces:**
- Consumes: the `events` Iceberg table name from `lakehouse/create_tables.sql` (Task 8) and the `{{ source('lakehouse', 'events') }}` reference already written into `stg_events.sql` in Task 10.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bake_integration.py (append)
def test_dbt_sources_reference_lakehouse_when_both_enabled(cookies):
    result = cookies.bake(extra_context={"include_dbt": True, "include_lakehouse": True})
    staging_dir = result.project_path / "transformation" / "models" / "staging"
    assert (staging_dir / "sources.yml").is_file()
    sources = (staging_dir / "sources.yml").read_text()
    assert "name: lakehouse" in sources
    assert "name: events" in sources


def test_dbt_sources_absent_when_lakehouse_disabled(cookies):
    result = cookies.bake(extra_context={"include_dbt": True, "include_lakehouse": False})
    staging_dir = result.project_path / "transformation" / "models" / "staging"
    assert not (staging_dir / "sources.yml").exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_integration.py -v`
Expected: FAIL — `sources.yml` does not exist, and its removal isn't wired into the pruning hook.

- [ ] **Step 3: Write sources.yml**

```yaml
# {{cookiecutter.project_slug}}/transformation/models/staging/sources.yml
version: 2

sources:
  - name: lakehouse
    schema: main
    tables:
      - name: events
```

- [ ] **Step 4: Add removal rule to the pruning hook for the lakehouse-off case**

```python
# hooks/post_gen_project.py — extend REMOVAL_RULES
REMOVAL_RULES = [
    ("{{ cookiecutter.include_batch }}" == "True", [
        f"src/{SLUG}/batch",
        "tests/test_batch_dag.py",
    ]),
    ("{{ cookiecutter.include_streaming }}" == "True", [
        f"src/{SLUG}/streaming",
        "tests/test_streaming_consumer.py",
    ]),
    ("{{ cookiecutter.include_lakehouse }}" == "True", [
        f"src/{SLUG}/lakehouse",
        "tests/test_lakehouse_writer.py",
        "transformation/models/staging/sources.yml",
    ]),
    ("{{ cookiecutter.include_dbt }}" == "True", [
        "transformation",
    ]),
]
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_integration.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add "{{cookiecutter.project_slug}}/transformation/models/staging/sources.yml" hooks/post_gen_project.py tests/test_bake_integration.py
git commit -m "feat: compose dbt sources with lakehouse tables when both enabled"
```

---

## Task 12: docker-compose.yml with conditional services

**Files:**
- Create: `{{cookiecutter.project_slug}}/docker-compose.yml`
- Test: `tests/test_bake_integration.py` (extend)

**Interfaces:**
- Consumes: `include_batch`, `include_streaming` flags. (Lakehouse and Transformation need no service — DuckDB is embedded, no server to run.)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bake_integration.py (append)
def test_docker_compose_has_airflow_service_only_when_batch_enabled(cookies):
    with_batch = cookies.bake(extra_context={"include_batch": True, "include_streaming": False})
    without_batch = cookies.bake(extra_context={"include_batch": False, "include_streaming": False})

    with_batch_compose = (with_batch.project_path / "docker-compose.yml").read_text()
    without_batch_compose = (without_batch.project_path / "docker-compose.yml").read_text()

    assert "airflow-webserver" in with_batch_compose
    assert "airflow-webserver" not in without_batch_compose


def test_docker_compose_has_kafka_service_only_when_streaming_enabled(cookies):
    result = cookies.bake(extra_context={"include_streaming": True})
    compose = (result.project_path / "docker-compose.yml").read_text()
    assert "kafka:" in compose
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_integration.py -v`
Expected: FAIL — `docker-compose.yml` does not exist.

- [ ] **Step 3: Write the template**

```yaml
# {{cookiecutter.project_slug}}/docker-compose.yml
services:
{%- if cookiecutter.include_batch %}
  airflow-webserver:
    image: apache/airflow:2.9.3
    command: webserver
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
    volumes:
      - ./src/{{ cookiecutter.project_slug }}/batch/dags:/opt/airflow/dags
{%- endif %}
{%- if cookiecutter.include_streaming %}
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
{%- endif %}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_integration.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add "{{cookiecutter.project_slug}}/docker-compose.yml" tests/test_bake_integration.py
git commit -m "feat: docker-compose with conditional airflow and kafka services"
```

---

## Task 13: Full integration bake and lint validation

**Files:**
- Test: `tests/test_bake_integration.py` (extend)

**Interfaces:**
- Consumes: every template file from Tasks 1–12.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bake_integration.py (append)
import subprocess


def test_full_bake_all_patterns_lints_clean(cookies):
    result = cookies.bake(extra_context={
        "include_batch": True,
        "include_streaming": True,
        "include_lakehouse": True,
        "include_dbt": True,
    })
    assert result.exit_code == 0

    lint = subprocess.run(
        ["uvx", "ruff", "check", "."],
        cwd=result.project_path,
        capture_output=True,
        text=True,
    )
    assert lint.returncode == 0, lint.stdout + lint.stderr


def test_full_bake_all_patterns_off_lints_clean(cookies):
    result = cookies.bake(extra_context={
        "include_batch": False,
        "include_streaming": False,
        "include_lakehouse": False,
        "include_dbt": False,
    })
    assert result.exit_code == 0

    lint = subprocess.run(
        ["uvx", "ruff", "check", "."],
        cwd=result.project_path,
        capture_output=True,
        text=True,
    )
    assert lint.returncode == 0, lint.stdout + lint.stderr
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bake_integration.py -v -k full_bake`
Expected: FAIL or PASS-with-lint-errors — run it to see the actual current state; fix whatever Ruff flags (commonly: unused imports left over from a prior task's template edits, missing trailing newline).

- [ ] **Step 3: Fix any lint errors surfaced**

Address each Ruff finding in the specific template file it points to (e.g. `src/{{cookiecutter.project_slug}}/batch/sinks.py`), re-running the lint command after each fix until clean.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_bake_integration.py -v -k full_bake`
Expected: PASS

- [ ] **Step 5: Run the entire test suite**

Run: `uv run pytest -v`
Expected: All tests across `tests/test_bake_baseline.py`, `tests/test_bake_batch.py`, `tests/test_bake_streaming.py`, `tests/test_bake_lakehouse.py`, `tests/test_bake_dbt.py`, `tests/test_bake_integration.py` PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/test_bake_integration.py
git commit -m "test: full integration bake across all pattern combinations lints clean"
```
