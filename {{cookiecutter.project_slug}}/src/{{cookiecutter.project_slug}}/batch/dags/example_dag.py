from datetime import UTC, datetime

from airflow.sdk import Param, dag, task

from {{ cookiecutter.project_slug }}.batch.sinks import write_events

# Off by default: the shipped demo behaves exactly as it did before the
# agentic Pattern existed. Set failure_mode=schema_drift to trigger the
# healing task.
FAILURE_MODES = ["none", "schema_drift"]


# Airflow expects a timezone-aware start_date; a naive datetime here is a
# latent bug, not just a lint finding.
@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    params={"failure_mode": Param("none", type="string", enum=FAILURE_MODES)},
)
def example_events_dag():
    @task
    def extract(**context) -> list[dict]:
        mode = context["params"]["failure_mode"]
        if mode == "schema_drift":
            # `customer_name` is not a column in lakehouse/create_tables.sql.
            return [{"id": 1, "customer_name": "example"}]
        return [{"id": 1, "name": "example"}]

    @task
    def load(records: list[dict]) -> None:
        write_events(records)

    load(extract())


example_events_dag()
