from datetime import UTC, datetime
from pathlib import Path

from airflow.sdk import Param, dag, task
{%- if cookiecutter.include_agentic %}
from airflow.task.trigger_rule import TriggerRule
{%- endif %}

{% if cookiecutter.include_agentic -%}
from {{ cookiecutter.project_slug }}.agentic.dags.healing_dag import (
    heal_on_failure,
    record_failure,
)
{% endif -%}
from {{ cookiecutter.project_slug }}.events import Event
from {{ cookiecutter.project_slug }}.sinks import create_event_sink
{% if cookiecutter.include_lakehouse %}
DEFAULT_DESTINATION = Path("lakehouse.duckdb")
{%- else %}
DEFAULT_DESTINATION = Path("data/events/batch")
{%- endif %}

# Off by default: the shipped demo behaves exactly as it did before the
# self-healing loop existed. Set failure_mode=schema_drift to make extract()
# emit a record that no longer matches the Event contract in events.py.
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
        if context["params"]["failure_mode"] == "schema_drift":
            # The producer renamed `name`; the Event contract still declares it.
            return [{"id": 1, "customer_name": "example"}]
        return [{"id": 1, "name": "example"}]

{%- if cookiecutter.include_agentic %}

    @task(on_failure_callback=record_failure)
{%- else %}

    @task
{%- endif %}
    def load(records: list[dict]) -> None:
        events = [Event.from_mapping(record) for record in records]
        with create_event_sink(DEFAULT_DESTINATION) as sink:
            sink.write(events)
{%- if cookiecutter.include_agentic %}

    loaded = load(extract())
    # The healer is told which task failed. It cannot read that off its own
    # TaskInstance -- Airflow injects the healing task's own `ti`, so
    # `ti.task_id` there is "heal_on_failure".
    heal_on_failure.override(trigger_rule=TriggerRule.ALL_FAILED)(
        failed_task_id="load"
    ) << loaded
{%- else %}

    load(extract())
{%- endif %}


example_events_dag()
