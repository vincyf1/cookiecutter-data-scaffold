import pytest
from airflow.sdk.exceptions import ParamValidationError

from {{ cookiecutter.project_slug }}.batch.dags.example_dag import (
    FAILURE_MODES,
    example_events_dag,
)

{%- if cookiecutter.include_lakehouse %}

# The demo's diagnosable failure. With the Lakehouse Pattern the record
# drifts from the DDL; without it there is no DDL, so the demo ships a
# config-lookup failure instead.
FAILING_MODE = "schema_drift"
{%- else %}

FAILING_MODE = "missing_config"
{%- endif %}


def test_dag_has_no_import_errors():
    dag = example_events_dag()
    assert dag.dag_id == "example_events_dag"
    assert {"extract", "load"} <= {task.task_id for task in dag.tasks}


def test_dag_exposes_a_failure_mode_param():
    dag = example_events_dag()
    assert "failure_mode" in dag.params


def test_the_demo_offers_a_failure_mode_that_this_bake_can_actually_raise():
    # A mode the shipped code cannot raise is worse than no mode at all: the
    # README promises the failure fires, and with ALL_FAILED wiring a load
    # task that quietly succeeds means the healer is skipped entirely.
    assert FAILURE_MODES == ["none", FAILING_MODE]


@pytest.mark.parametrize("mode", ["none", FAILING_MODE])
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
{%- if not cookiecutter.include_lakehouse %}


def test_the_failing_mode_raises_a_diagnosable_error():
    # Exercises the real path the DAG takes, not a stand-in: load() asks
    # batch/sinks.write_events for a setting name that pipeline.toml does
    # not declare.
    from {{ cookiecutter.project_slug }}.batch.config import MissingConfigError
    from {{ cookiecutter.project_slug }}.batch.sinks import write_events

    with pytest.raises(MissingConfigError) as excinfo:
        write_events([{"id": 1, "name": "example"}], output_setting="output_file")

    assert "output_file" in str(excinfo.value)
    assert "output_path" in str(excinfo.value)
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
