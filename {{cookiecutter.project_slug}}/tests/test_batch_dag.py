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
    # dag.params[...] resolves to the bare value; the Param wrapper (with
    # its enum validation) is reached via get_param().
    dag.params.get_param("failure_mode").value = mode
    assert dag.params["failure_mode"] == mode
