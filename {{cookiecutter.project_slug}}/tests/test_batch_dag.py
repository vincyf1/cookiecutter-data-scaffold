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
