# Merging the Agentic Pattern into main — Design

**Date:** 2026-10-13
**Status:** Draft for review
**Branch:** `agentic-self-healing-pattern` → `main`

## Summary

`main` now has the lifecycle-managed event sinks from PR #2: an immutable
`Event` contract, one `EventSink` protocol, and `create_event_sink()`. The
agentic branch was built on the older dict-based `write_events` sinks, so it
conflicts with `main` in 10 files. Its schema-drift failure also stops working
on `main`, because records become `Event`s before they reach a sink.

This design moves drift detection onto the `Event` contract, removes the
`missing_config` failure mode that only existed to cover lakehouse-off
projects, and gets the branch onto `main` with one merge commit and a pull
request.

## Decisions

1. **Drift is detected in `Event.from_mapping`.** It works with the Lakehouse
   Pattern on or off, and it keeps the event contract from PR #2 intact.
2. **`missing_config` is removed.** Once drift works in every configuration,
   it has no job left. One failure mode, `schema_drift`, ships everywhere.
3. **`main` is merged into the branch, then the branch is merged by PR.** The
   31 reviewed commits stay as they are, conflicts are resolved once, and the
   PR merges with a merge commit, as PR #2 did.

## Drift detection on the Event contract

`events.py` gains `SchemaDriftError(ValueError)`. `Event.from_mapping` raises
it when a field the contract declares is missing. The message names the
missing fields, any fields the contract does not declare, and the declared
fields, because it is the agent's starting evidence:

```
record lacks field(s) ['name'] and carries undeclared field(s)
['customer_name']; the Event contract declares ['id', 'name']. Either the
producer renamed a field or the contract is out of date.
```

Two rules keep this compatible with what `main` does today:

- **Extra fields alone are accepted.** A record with every declared field plus
  more still becomes an `Event`, exactly as now. Rejecting extra fields would
  break additive schema evolution for the streaming consumer. The demo's rename
  is still caught, because the renamed field is missing.
- **`SchemaDriftError` subclasses `ValueError`.** The streaming consumer
  already skips `KeyError`, `TypeError` and `ValueError` as malformed, so its
  behaviour does not change. `test_events.py` already accepts `ValueError`.

The declared fields come from the dataclass, through `dataclasses.fields(Event)`,
so the contract is the single source. When the Lakehouse Pattern is on, a
generated-project test checks that every `Event` field is a column in
`lakehouse/create_tables.sql`, so the contract and the DDL cannot drift apart.

`lakehouse/writer.py` loses the branch's `SchemaDriftError`, `validate()` and
`declared_columns()`: a typed `Event` can no longer carry an undeclared field
there.

## The demo DAG

`example_dag.py` takes `main`'s shape (`Event.from_mapping`, then
`create_event_sink()` in a `with` block) and adds the branch's agentic wiring:

- `failure_mode` is `["none", "schema_drift"]` in every configuration. With
  `schema_drift`, `extract()` returns `{"id": 1, "customer_name": "example"}`.
- `load` keeps `on_failure_callback=record_failure` and the `heal_on_failure`
  task with `trigger_rule=ALL_FAILED`, gated on `include_agentic`.
- The `include_lakehouse` branches inside `extract` and `load` disappear.

## Removed with `missing_config`

- `batch/config.py`, `batch/pipeline.toml`, `tests/test_batch_config.py`
- `batch/sinks.py` (already deleted on `main`; the branch's change to it goes)
- `evals/cases/missing_config.yaml`, `evals/cassettes/missing_config.json`
- The `missing_config` entries in `evals/conftest.py`
  (`MISSING_CONFIG_DAG`, `PIPELINE_TOML`, `MISSING_CONFIG_LOG`, its
  `FAILURE_EXCEPTIONS` entry)
- The `tests/test_batch_config.py` line in `hooks/post_gen_project.py`, and
  any README text that describes `missing_config`

## Evals

The eval tree in `evals/conftest.py` is synthetic, so it is updated to mirror
the new runtime rather than re-baked:

- The fixture project carries `src/pkg/events.py` (the `Event` contract)
  instead of `lakehouse/create_tables.sql`.
- `SCHEMA_DRIFT_LOG` and the `FAILURE_EXCEPTIONS` entry use the new message.
- Both `schema_drift` cassettes read `src/pkg/events.py` instead of
  `src/pkg/lakehouse/create_tables.sql`, and their diagnosis and rationale
  refer to the Event contract. The cassettes are hand-editable lists of tool
  calls, not HTTP recordings, so no API key is needed. The README's re-record
  instructions are unchanged.
- `expect.patch_targets` stays `src/pkg/batch/dags/example_dag.py`, and
  `exact_tool_sequence` keeps its shape.

## Resolving the 10 conflicts

| File | Resolution |
|---|---|
| `hooks/post_gen_project.py` | Take `main`'s manifest, then add the branch's `include_agentic` entry, its `logs` removal under Batch, and its `agentic/` files. Drop `batch/sinks.py`, `streaming/sinks.py`, `test_streaming_sinks.py` and `test_batch_config.py` from every list. Add `events.py` to the agentic `gated` list. |
| `{{slug}}/README.md` | Keep both sections: `main`'s event-ingestion section and the branch's agentic section, minus `missing_config`. |
| `example_dag.py` | As in "The demo DAG" above. |
| `batch/sinks.py` | Delete (`main` deleted it). |
| `sinks.py` | Take `main`'s version. The branch only split an import line in the old file. |
| `lakehouse/writer.py` | Take `main`'s `DuckDBEventSink`. The validation it held moves to `events.py`. |
| `tests/test_batch_dag.py` | Take `main`'s tests, add the branch's agentic assertions (heal task, trigger rule, failure-mode param). |
| `tests/test_lakehouse_writer.py` | Take `main`'s; delete the branch's `declared_columns` and `validate` tests, which move to `test_events.py`. |
| `tests/test_streaming_consumer.py` | Take `main`'s. The branch only replaced `try/except StopIteration` with `contextlib.suppress`; reapply that wherever `main`'s tests still end the poll loop that way. |
| `tests/test_bake_integration.py` | Take `main`'s replacement of the old sink-delegation test and its slow default-bake test. Keep the branch's `lint_project`, its `include_agentic` parametrisation, and `include_agentic=False` on bakes without Batch, which `tests/conftest.py`'s all-on `DEFAULT_FLAGS` still needs. |

## Testing

- `test_events.py`: drift raises `SchemaDriftError` naming missing, undeclared
  and declared fields; extra fields alone still produce an `Event`.
- A lakehouse-on test that `Event` fields are a subset of the DDL's columns.
- The template suite (`uv run pytest`, fast and slow tiers) and `ruff check`.
- Bake with `include_agentic` on, crossed with `include_lakehouse` on and off;
  in each, run the generated `uv run pytest` (unit and replay tiers) and
  `ruff check`.
- `test_bake_agentic.py`: no `missing_config` or `pipeline.toml` anywhere in a
  bake.
- `git diff --check` and a `{%-`/blank-line review of the edited templates,
  the defect class PR #2 hit.

## Shipping

1. Push `agentic-self-healing-pattern` (it has never been pushed).
2. Open a PR to `main` with a summary of the Pattern, these decisions, and the
   test evidence. `gh` is not logged in on this machine, so either log in or
   open the PR from the branch's compare page.
3. Merge with a merge commit after review.

## Out of scope

- The talk-demo features from the 2026-10-12 assessment (patch validation,
  Airflow REST tools, dbt tools). Each is a follow-up on top of this merge.
- Making generated pipelines safe to rerun, which must happen before any agent
  gets a rerun tool.
- The pre-existing `ruff format` drift in generated files.

## Supersedes

The 2026-07-29 design's `missing_config` section and its claim that
`create_tables.sql` is the drift ground truth. That design is otherwise
unchanged.
