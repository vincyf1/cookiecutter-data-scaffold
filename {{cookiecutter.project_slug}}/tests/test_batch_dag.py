from {{ cookiecutter.project_slug }}.batch.dags.example_dag import example_events_dag


def test_dag_has_no_import_errors():
    dag = example_events_dag()
    assert dag.dag_id == "example_events_dag"
    assert len(dag.tasks) == 2


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
