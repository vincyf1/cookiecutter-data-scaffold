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
