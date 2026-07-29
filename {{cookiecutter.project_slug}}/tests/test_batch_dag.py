import pytest
from airflow.sdk.exceptions import ParamValidationError

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
