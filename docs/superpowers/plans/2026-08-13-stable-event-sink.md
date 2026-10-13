# Stable Event Sink Implementation Plan

> **Implementation status (2026-09-20):** Tasks 1–7 implemented on `feat/stable-event-sink`.
> The original step checkboxes below retain the proposed execution sequence rather
> than claiming that every suggested commit or test command ran verbatim.
>
> Adjustments: retain `events.py` for Lakehouse-only projects; prune Parquet tests
> when ingestion is disabled; use modern typing and UTC annotations for lint;
> make empty DuckDB batches no-ops and close connections on initialization failure;
> safely skip Kafka tombstones. Batch persistence is tested by executing the load
> callable. Verification: `uv run pytest -q` — 46 passed (including all slow tests);
> `.venv/bin/ruff check .` and `git diff --check` — passed. Changes are grouped into focused commits on the feature branch.


> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the generated project's variant `write_events` helpers with one typed, context-managed event-sink interface and lifecycle-owning DuckDB and partitioned-Parquet adapters.

**Architecture:** `events.py` owns the validated `Event` contract. `sinks.py` owns the `EventSink` protocol, the partitioned-Parquet adapter, and the generated-variant factory; the Lakehouse module owns the DuckDB adapter. Batch opens one sink per Airflow load task, while Streaming opens one sink for the consumer lifetime. Adapter destinations are explicit constructor arguments with defaults selected only at the two composition roots.

**Tech Stack:** Python 3.12, `dataclasses`, `typing.Protocol`, DuckDB, PyArrow Parquet, Airflow, confluent-kafka, Cookiecutter/Jinja, pytest, pytest-cookies, Ruff

---

## File Structure

- Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/events.py`: validated immutable `Event` domain value.
- Modify `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/sinks.py`: stable `EventSink` protocol, partitioned-Parquet adapter, and `create_event_sink(destination)` factory.
- Modify `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/writer.py`: context-managed `DuckDBEventSink` with transaction ownership.
- Modify `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/consumer.py`: typed decoding and consumer-lifetime sink ownership.
- Modify `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/dags/example_dag.py`: typed conversion and task-run sink ownership.
- Delete `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/sinks.py`: shallow pass-through module superseded by the stable seam.
- Delete `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/sinks.py`: shallow pass-through module superseded by the stable seam.
- Create `{{cookiecutter.project_slug}}/tests/test_events.py`: event validation tests.
- Create `{{cookiecutter.project_slug}}/tests/test_parquet_sink.py`: partitioned output and context-manager tests, present when Lakehouse is disabled.
- Modify `{{cookiecutter.project_slug}}/tests/test_lakehouse_writer.py`: DuckDB lifecycle, transaction, and validation tests.
- Modify `{{cookiecutter.project_slug}}/tests/test_streaming_consumer.py`: typed decoding and one-sink-per-consumer-lifetime tests.
- Modify `{{cookiecutter.project_slug}}/tests/test_batch_dag.py`: DAG structure remains importable after direct sink composition.
- Modify `hooks/post_gen_project.py`: remove deleted modules from gated paths and prune `events.py` plus `test_events.py` when neither ingestion Pattern survives.
- Modify `tests/test_bake_integration.py`: assert the stable interface and execute the two storage variants.
- Modify `tests/test_bake_baseline.py`: assert shared event files are removed when Batch and Streaming are both disabled.

### Task 1: Add The Validated Event Contract

**Files:**
- Create: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/events.py`
- Create: `{{cookiecutter.project_slug}}/tests/test_events.py`
- Modify: `hooks/post_gen_project.py:79-85`
- Modify: `tests/test_bake_baseline.py:16-30`

- [ ] **Step 1: Write failing generated-project tests**

Create `{{cookiecutter.project_slug}}/tests/test_events.py`:

```python
import pytest

from {{ cookiecutter.project_slug }}.events import Event


def test_event_from_mapping_returns_typed_event():
    assert Event.from_mapping({"id": 1, "name": "example"}) == Event(id=1, name="example")


@pytest.mark.parametrize(
    "record",
    [
        {},
        {"id": 1},
        {"name": "example"},
        {"id": "1", "name": "example"},
        {"id": 1, "name": ""},
    ],
)
def test_event_from_mapping_rejects_invalid_records(record):
    with pytest.raises((TypeError, ValueError, KeyError)):
        Event.from_mapping(record)
```

Extend `test_all_patterns_off_removes_pattern_dirs` in `tests/test_bake_baseline.py`:

```python
assert not (project / "src" / slug / "events.py").exists()
assert not (project / "tests" / "test_events.py").exists()
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
uv run pytest tests/test_bake_baseline.py::test_all_patterns_off_removes_pattern_dirs -v
uv run pytest tests/test_bake_integration.py::test_streaming_only_bake_generated_tests_pass -v
```

Expected: baseline fails because `events.py` is not pruned after it is added; generated test execution fails with `ModuleNotFoundError` until the implementation exists.

- [ ] **Step 3: Implement the minimal Event value**

Create `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/events.py`:

```python
from dataclasses import dataclass
from typing import Any, Mapping


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
        return cls(id=record["id"], name=record["name"])
```

Change `COMPOUND_REMOVALS` in `hooks/post_gen_project.py` to:

```python
COMPOUND_REMOVALS = [
    ({{ cookiecutter.include_batch or cookiecutter.include_streaming }}, [
        f"src/{SLUG}/events.py",
        f"src/{SLUG}/sinks.py",
        "tests/test_events.py",
    ]),
]
```

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```bash
uv run pytest tests/test_bake_baseline.py::test_all_patterns_off_removes_pattern_dirs -v
uv run pytest tests/test_bake_integration.py::test_streaming_only_bake_generated_tests_pass -v
```

Expected: both pass.

- [ ] **Step 5: Commit**

```bash
git add hooks/post_gen_project.py tests/test_bake_baseline.py '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/events.py' '{{cookiecutter.project_slug}}/tests/test_events.py'
git commit -m "feat: add validated event contract"
```

### Task 2: Add The Stable Sink Interface And Parquet Adapter

**Files:**
- Modify: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/sinks.py`
- Create: `{{cookiecutter.project_slug}}/tests/test_parquet_sink.py`
- Delete: `{{cookiecutter.project_slug}}/tests/test_streaming_sinks.py`
- Modify: `hooks/post_gen_project.py:18-85`
- Modify: `tests/test_bake_integration.py:6-35`

- [ ] **Step 1: Write failing Parquet adapter tests**

Create `{{cookiecutter.project_slug}}/tests/test_parquet_sink.py`:

```python
{%- if not cookiecutter.include_lakehouse %}
from pathlib import Path

import pyarrow.parquet as pq

from {{ cookiecutter.project_slug }}.events import Event
from {{ cookiecutter.project_slug }}.sinks import PartitionedParquetEventSink


def test_parquet_sink_writes_an_immutable_partition_per_batch(tmp_path):
    with PartitionedParquetEventSink(tmp_path) as sink:
        sink.write([Event(id=1, name="first")])
        sink.write([Event(id=2, name="second")])

    partitions = sorted(tmp_path.glob("*.parquet"))
    assert len(partitions) == 2
    rows = sorted(
        (row for path in partitions for row in pq.read_table(path).to_pylist()),
        key=lambda row: row["id"],
    )
    assert rows == [{"id": 1, "name": "first"}, {"id": 2, "name": "second"}]


def test_parquet_sink_publishes_no_partial_file_when_write_fails(tmp_path, monkeypatch):
    def fail_write(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr("{{ cookiecutter.project_slug }}.sinks.pq.write_table", fail_write)

    with PartitionedParquetEventSink(tmp_path) as sink:
        try:
            sink.write([Event(id=1, name="example")])
        except OSError:
            pass

    assert list(tmp_path.iterdir()) == []


def test_parquet_sink_accepts_path_constructor_argument(tmp_path):
    destination = Path(tmp_path) / "events"
    with PartitionedParquetEventSink(destination) as sink:
        sink.write([])
    assert destination.is_dir()
{%- endif %}
```

Replace the old textual sink tests in `tests/test_bake_integration.py` with:

```python
def test_shared_sink_exposes_one_interface_in_each_storage_variant(cookies):
    with_lakehouse = bake_with(cookies, include_lakehouse=True)
    without_lakehouse = bake_with(cookies, include_lakehouse=False)

    for result in (with_lakehouse, without_lakehouse):
        slug = result.context["project_slug"]
        sinks = (result.project_path / "src" / slug / "sinks.py").read_text()
        assert "class EventSink(Protocol)" in sinks
        assert "def create_event_sink(destination: Path) -> EventSink" in sinks
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
uv run pytest tests/test_bake_integration.py::test_shared_sink_exposes_one_interface_in_each_storage_variant -v
uv run pytest tests/test_bake_integration.py::test_streaming_only_bake_generated_tests_pass -v
```

Expected: failures because `EventSink`, `create_event_sink`, and `PartitionedParquetEventSink` do not exist.

- [ ] **Step 3: Implement the protocol and Parquet adapter**

Replace `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/sinks.py` with:

```python
from pathlib import Path
from types import TracebackType
from typing import Protocol, Sequence

from {{ cookiecutter.project_slug }}.events import Event
{%- if cookiecutter.include_lakehouse %}
from {{ cookiecutter.project_slug }}.lakehouse.writer import DuckDBEventSink
{%- else %}
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
{%- endif %}


class EventSink(Protocol):
    def __enter__(self) -> "EventSink": ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    def write(self, events: Sequence[Event]) -> None: ...


{%- if not cookiecutter.include_lakehouse %}
class PartitionedParquetEventSink:
    def __init__(self, destination: Path) -> None:
        self.destination = destination

    def __enter__(self) -> "PartitionedParquetEventSink":
        self.destination.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    def write(self, events: Sequence[Event]) -> None:
        if not events:
            return
        partition_id = uuid4().hex
        final_path = self.destination / f"part-{partition_id}.parquet"
        temporary_path = self.destination / f".{partition_id}.tmp"
        table = pa.Table.from_pylist(
            [{"id": event.id, "name": event.name} for event in events]
        )
        try:
            pq.write_table(table, temporary_path, compression="snappy")
            temporary_path.replace(final_path)
        finally:
            temporary_path.unlink(missing_ok=True)
{%- endif %}


def create_event_sink(destination: Path) -> EventSink:
{%- if cookiecutter.include_lakehouse %}
    return DuckDBEventSink(destination)
{%- else %}
    return PartitionedParquetEventSink(destination)
{%- endif %}
```

Delete `{{cookiecutter.project_slug}}/tests/test_streaming_sinks.py`.

Update the Streaming Pattern manifest to remove `tests/test_streaming_sinks.py`. Add `tests/test_parquet_sink.py` to the Lakehouse Pattern's `gated` paths so its conditional content remains tracked. Remove `batch/sinks.py` and `streaming/sinks.py` from the Lakehouse Pattern's `gated` paths.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```bash
uv run pytest tests/test_pattern_manifest.py -v
uv run pytest tests/test_bake_integration.py::test_shared_sink_exposes_one_interface_in_each_storage_variant -v
uv run pytest tests/test_bake_integration.py::test_streaming_only_bake_generated_tests_pass -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add hooks/post_gen_project.py tests/test_bake_integration.py '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/sinks.py' '{{cookiecutter.project_slug}}/tests/test_parquet_sink.py' '{{cookiecutter.project_slug}}/tests/test_streaming_sinks.py'
git commit -m "feat: add partitioned parquet event sink"
```

### Task 3: Add The Lifecycle-Owning DuckDB Adapter

**Files:**
- Modify: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/writer.py`
- Modify: `{{cookiecutter.project_slug}}/tests/test_lakehouse_writer.py`

- [ ] **Step 1: Write failing DuckDB adapter tests**

Replace `{{cookiecutter.project_slug}}/tests/test_lakehouse_writer.py` with:

```python
import pytest

from {{ cookiecutter.project_slug }}.events import Event
from {{ cookiecutter.project_slug }}.lakehouse.writer import DuckDBEventSink


def test_duckdb_sink_inserts_one_batch_transactionally(tmp_path):
    database = tmp_path / "test.duckdb"
    with DuckDBEventSink(database) as sink:
        sink.write([Event(id=1, name="example"), Event(id=2, name="second")])
        rows = sink.connection.execute("SELECT id, name FROM events ORDER BY id").fetchall()
    assert rows == [(1, "example"), (2, "second")]


def test_duckdb_sink_closes_connection_on_context_exit(tmp_path):
    sink = DuckDBEventSink(tmp_path / "test.duckdb")
    with sink:
        pass
    with pytest.raises(Exception):
        sink.connection.execute("SELECT 1")


def test_duckdb_sink_rolls_back_whole_batch_on_database_failure(tmp_path):
    database = tmp_path / "test.duckdb"
    with DuckDBEventSink(database) as sink:
        sink.connection.execute("CREATE UNIQUE INDEX events_id ON events(id)")
        with pytest.raises(Exception):
            sink.write([Event(id=1, name="first"), Event(id=1, name="duplicate")])
        rows = sink.connection.execute("SELECT id, name FROM events").fetchall()
    assert rows == []
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
uv run pytest tests/test_bake_integration.py::test_lakehouse_only_bake_generated_tests_pass -v
```

Expected: generated tests fail because `DuckDBEventSink` is missing.

- [ ] **Step 3: Implement the DuckDB adapter**

Replace `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/writer.py` with:

```python
from pathlib import Path
from types import TracebackType
from typing import Sequence

import duckdb

from {{ cookiecutter.project_slug }}.events import Event

DDL_PATH = Path(__file__).parent / "create_tables.sql"


class DuckDBEventSink:
    def __init__(self, destination: Path) -> None:
        self.destination = destination
        self.connection: duckdb.DuckDBPyConnection

    def __enter__(self) -> "DuckDBEventSink":
        self.connection = duckdb.connect(str(self.destination))
        self.connection.execute(DDL_PATH.read_text())
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.connection.close()

    def write(self, events: Sequence[Event]) -> None:
        self.connection.execute("BEGIN")
        try:
            self.connection.executemany(
                "INSERT INTO events (id, name) VALUES (?, ?)",
                [(event.id, event.name) for event in events],
            )
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        self.connection.execute("COMMIT")
```

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```bash
uv run pytest tests/test_bake_integration.py::test_lakehouse_only_bake_generated_tests_pass -v
```

Expected: pass, including connection closure and transaction rollback.

- [ ] **Step 5: Commit**

```bash
git add '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/lakehouse/writer.py' '{{cookiecutter.project_slug}}/tests/test_lakehouse_writer.py'
git commit -m "feat: add transactional duckdb event sink"
```

### Task 4: Give Streaming One Sink Per Consumer Lifetime

**Files:**
- Modify: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/consumer.py`
- Delete: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/sinks.py`
- Modify: `{{cookiecutter.project_slug}}/tests/test_streaming_consumer.py`
- Modify: `hooks/post_gen_project.py:32-43`

- [ ] **Step 1: Write failing consumer-lifecycle tests**

Replace `{{cookiecutter.project_slug}}/tests/test_streaming_consumer.py` with tests that inject real lightweight fakes instead of patching persistence calls:

```python
from pathlib import Path

import pytest

from {{ cookiecutter.project_slug }}.events import Event
from {{ cookiecutter.project_slug }}.streaming.consumer import handle_message, run


class Message:
    def __init__(self, value: bytes):
        self._value = value

    def error(self):
        return None

    def value(self):
        return self._value


class Consumer:
    def __init__(self):
        self.messages = iter([Message(b"not json"), Message(b'{"id": 2, "name": "ok"}')])
        self.closed = False

    def subscribe(self, topics):
        self.topics = topics

    def poll(self, timeout):
        return next(self.messages)

    def close(self):
        self.closed = True


class Sink:
    def __init__(self):
        self.enter_count = 0
        self.exit_count = 0
        self.batches = []

    def __enter__(self):
        self.enter_count += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.exit_count += 1

    def write(self, events):
        self.batches.append(list(events))


def test_handle_message_returns_typed_event():
    assert handle_message(b'{"id": 1, "name": "example"}') == Event(id=1, name="example")


def test_handle_message_rejects_invalid_event():
    with pytest.raises((TypeError, ValueError, KeyError)):
        handle_message(b'{"id": "bad", "name": "example"}')


def test_run_owns_one_sink_for_consumer_lifetime():
    consumer = Consumer()
    sink = Sink()

    with pytest.raises(StopIteration):
        run(
            consumer_factory=lambda config: consumer,
            sink_factory=lambda destination: sink,
            destination=Path("unused"),
        )

    assert sink.enter_count == 1
    assert sink.exit_count == 1
    assert sink.batches == [[Event(id=2, name="ok")]]
    assert consumer.closed is True
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
uv run pytest tests/test_bake_integration.py::test_streaming_only_bake_generated_tests_pass -v
```

Expected: generated tests fail because `handle_message` returns a dictionary and `run` has no injected factories or destination.

- [ ] **Step 3: Implement consumer-lifetime ownership**

Update `streaming/consumer.py` so its public composition interface is:

```python
def handle_message(raw_value: bytes) -> Event:
    return Event.from_mapping(json.loads(raw_value))


def run(
    bootstrap_servers: str = "localhost:9092",
    *,
    destination: Path = DEFAULT_DESTINATION,
    consumer_factory=Consumer,
    sink_factory=create_event_sink,
) -> None:
    consumer = consumer_factory({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "{{ cookiecutter.project_slug }}-consumer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    try:
        with sink_factory(destination) as sink:
            while True:
                msg = consumer.poll(1.0)
                if msg is None or msg.error():
                    continue
                try:
                    event = handle_message(msg.value())
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    logger.warning("Skipping malformed event (%d bytes)", len(msg.value()))
                    continue
                sink.write([event])
    finally:
        consumer.close()
```

Define `DEFAULT_DESTINATION` with Jinja at module scope:

```python
{%- if cookiecutter.include_lakehouse %}
DEFAULT_DESTINATION = Path("lakehouse.duckdb")
{%- else %}
DEFAULT_DESTINATION = Path("data/events/streaming")
{%- endif %}
```

Delete `streaming/sinks.py`, remove it from the Streaming Pattern's `remove` list, and remove any remaining manifest references.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```bash
uv run pytest tests/test_pattern_manifest.py -v
uv run pytest tests/test_bake_integration.py::test_streaming_only_bake_generated_tests_pass -v
uv run pytest tests/test_bake_integration.py::test_lakehouse_only_bake_generated_tests_pass -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add hooks/post_gen_project.py '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/consumer.py' '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/streaming/sinks.py' '{{cookiecutter.project_slug}}/tests/test_streaming_consumer.py'
git commit -m "refactor: own sink for streaming lifetime"
```

### Task 5: Give Each Airflow Load Task Its Own Sink

**Files:**
- Modify: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/dags/example_dag.py`
- Delete: `{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/sinks.py`
- Modify: `{{cookiecutter.project_slug}}/tests/test_batch_dag.py`
- Modify: `hooks/post_gen_project.py:18-30`

- [ ] **Step 1: Write the failing Batch composition assertion**

Extend `test_batch_dag.py`:

```python
def test_dag_load_task_uses_stable_sink_interface():
    source = Path(example_events_dag.__code__.co_filename).read_text()
    assert "with create_event_sink(DEFAULT_DESTINATION) as sink:" in source
    assert "sink.write(events)" in source
```

Add `from pathlib import Path` to that test file.

- [ ] **Step 2: Run test and verify RED**

Run:

```bash
uv run pytest tests/test_bake_integration.py::test_batch_only_bake_generated_tests_pass -v
```

Expected: generated test fails because the DAG still imports the deleted Batch helper.

- [ ] **Step 3: Compose the sink directly in the Airflow task**

In `example_dag.py`, import `timezone`, `Path`, `Event`, and `create_event_sink`; use a timezone-aware start date to resolve the currently failing Ruff check. Set the variant default using Jinja:

```python
{%- if cookiecutter.include_lakehouse %}
DEFAULT_DESTINATION = Path("lakehouse.duckdb")
{%- else %}
DEFAULT_DESTINATION = Path("data/events/batch")
{%- endif %}
```

Keep `extract()` JSON-serializable, and replace `load()` with:

```python
@task
def load(records: list[dict]) -> None:
    events = [Event.from_mapping(record) for record in records]
    with create_event_sink(DEFAULT_DESTINATION) as sink:
        sink.write(events)
```

Change the decorator to:

```python
@dag(schedule="@daily", start_date=datetime(2024, 1, 1, tzinfo=timezone.utc), catchup=False)
```

Delete `batch/sinks.py` and remove it from the Batch Pattern's `remove` list and all gated lists.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```bash
uv run pytest tests/test_pattern_manifest.py -v
uv run pytest tests/test_bake_integration.py::test_batch_only_bake_generated_tests_pass -v
uv run pytest tests/test_bake_integration.py::test_full_bake_all_patterns_lints_clean -v
```

Expected: all pass, including the previously observed `DTZ001` failure.

- [ ] **Step 5: Commit**

```bash
git add hooks/post_gen_project.py '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/dags/example_dag.py' '{{cookiecutter.project_slug}}/src/{{cookiecutter.project_slug}}/batch/sinks.py' '{{cookiecutter.project_slug}}/tests/test_batch_dag.py'
git commit -m "refactor: own sink per airflow load task"
```

### Task 6: Add Cross-Pattern Regression Coverage

**Files:**
- Modify: `tests/test_bake_integration.py:134-215`
- Modify: `tests/test_bake_baseline.py:33-59`

- [ ] **Step 1: Write a failing dependency-locality test**

Add to `tests/test_bake_integration.py`:

```python
@pytest.mark.slow
def test_default_bake_generated_tests_pass(cookies):
    result = bake_with(cookies)
    assert result.exit_code == 0

    run_cmd(result.project_path, "uv", "sync")
    run_cmd(result.project_path, "uv", "run", "pytest", "-v")
```

Add to `tests/test_bake_baseline.py`:

```python
def test_no_ingestion_patterns_omit_storage_adapter_dependencies(cookies):
    result = bake_with(
        cookies,
        include_batch=False,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=True,
    )
    pyproject = (result.project_path / "pyproject.toml").read_text()
    assert "pyarrow" not in pyproject
    assert "duckdb" not in pyproject
```

- [ ] **Step 2: Run test and verify RED**

Run:

```bash
uv run pytest tests/test_bake_baseline.py::test_no_ingestion_patterns_omit_storage_adapter_dependencies -v
```

Expected: fail because generated `pyproject.toml` still declares `pyarrow` unconditionally.

- [ ] **Step 3: Scope PyArrow to the Parquet adapter**

Wrap the generated PyArrow requirement in `{{cookiecutter.project_slug}}/pyproject.toml`:

```toml
{%- if (cookiecutter.include_batch or cookiecutter.include_streaming) and not cookiecutter.include_lakehouse %}
    "pyarrow>=17.0",
{%- endif %}
```

Do not add environment-backed configuration, migration support, compatibility aliases, consumer buffering, or retry/dead-letter behavior; those remain separate recommendations.

- [ ] **Step 4: Run the complete verification suite**

Run:

```bash
uv run pytest -m "not slow"
uv run pytest tests/test_bake_integration.py::test_streaming_only_bake_generated_tests_pass -v
uv run pytest tests/test_bake_integration.py::test_lakehouse_only_bake_generated_tests_pass -v
uv run pytest tests/test_bake_integration.py::test_batch_only_bake_generated_tests_pass -v
uv run pytest tests/test_bake_integration.py::test_default_bake_generated_tests_pass -v
uv run ruff check .
```

Expected: every command exits 0 with no failures or lint errors.

- [ ] **Step 5: Commit**

```bash
git add tests/test_bake_integration.py tests/test_bake_baseline.py '{{cookiecutter.project_slug}}/pyproject.toml'
git commit -m "test: cover composed event sink variants"
```

### Task 7: Update Generated Documentation

**Files:**
- Modify: `{{cookiecutter.project_slug}}/README.md`
- Modify: `README.md:97-105`

- [ ] **Step 1: Add a failing generated documentation assertion**

In `tests/test_bake_integration.py`, add:

```python
def test_generated_readme_documents_event_sink_outputs(cookies):
    result = bake_with(cookies)
    readme = (result.project_path / "README.md").read_text()
    assert "EventSink" in readme
    assert "lakehouse.duckdb" in readme
    assert "consumer lifetime" in readme
```

- [ ] **Step 2: Run test and verify RED**

Run:

```bash
uv run pytest tests/test_bake_integration.py::test_generated_readme_documents_event_sink_outputs -v
```

Expected: fail because the generated README currently has only three lines.

- [ ] **Step 3: Document the new interface and outputs**

Add a concise generated README section explaining:

```markdown
## Event ingestion

Batch and Streaming write typed `Event` values through the context-managed `EventSink` interface.

{% if cookiecutter.include_lakehouse %}
The DuckDB adapter keeps one connection for the caller's context and writes to `lakehouse.duckdb` by default.
{% else %}
The Parquet adapter writes immutable partition files below `data/events/batch/` or `data/events/streaming/`.
{% endif %}

The Airflow load task opens one sink per task run. The Kafka consumer opens one sink for the consumer lifetime.
```

Update the root Pattern descriptions to say that Batch and Streaming use a shared typed sink interface and that non-Lakehouse output is partitioned Parquet.

- [ ] **Step 4: Run documentation and manifest checks**

Run:

```bash
uv run pytest tests/test_bake_integration.py::test_generated_readme_documents_event_sink_outputs -v
uv run pytest tests/test_pattern_manifest.py -v
uv run ruff check .
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add README.md tests/test_bake_integration.py '{{cookiecutter.project_slug}}/README.md'
git commit -m "docs: explain generated event sink lifecycle"
```

## Final Verification

- [ ] Run `uv run pytest -m "not slow"` and confirm zero failures.
- [ ] Run all generated-project slow tests with `uv run pytest -m slow -v` and confirm zero failures.
- [ ] Run `uv run ruff check .` and confirm zero errors.
- [ ] Run `git diff --check` and confirm no whitespace errors.
- [ ] Inspect `git status --short` and confirm only intended files changed; leave the existing untracked `.idea/` directory untouched.
