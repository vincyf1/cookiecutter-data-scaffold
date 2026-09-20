from datetime import UTC, datetime
from pathlib import Path

from airflow.sdk import dag, task

from {{ cookiecutter.project_slug }}.events import Event
from {{ cookiecutter.project_slug }}.sinks import create_event_sink
{% if cookiecutter.include_lakehouse %}
DEFAULT_DESTINATION = Path("lakehouse.duckdb")
{%- else %}
DEFAULT_DESTINATION = Path("data/events/batch")
{%- endif %}



@dag(schedule="@daily", start_date=datetime(2024, 1, 1, tzinfo=UTC), catchup=False)
def example_events_dag():
    @task
    def extract() -> list[dict]:
        return [{"id": 1, "name": "example"}]

    @task
    def load(records: list[dict]) -> None:
        events = [Event.from_mapping(record) for record in records]
        with create_event_sink(DEFAULT_DESTINATION) as sink:
            sink.write(events)

    load(extract())


example_events_dag()
