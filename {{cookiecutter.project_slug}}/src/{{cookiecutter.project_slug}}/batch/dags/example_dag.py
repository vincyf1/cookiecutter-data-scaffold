from datetime import UTC, datetime

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
from {{ cookiecutter.project_slug }}.batch.sinks import write_events

{%- if cookiecutter.include_lakehouse %}

# Off by default: the shipped demo behaves exactly as it did before the
# self-healing loop existed. Set failure_mode=schema_drift to trigger the
# healing task.
FAILURE_MODES = ["none", "schema_drift"]
{%- else %}

# Off by default: the shipped demo behaves exactly as it did before the
# self-healing loop existed. Set failure_mode=missing_config to trigger the
# healing task.
#
# Without the Lakehouse Pattern there is no DDL for a record to drift away
# from, so the demo's diagnosable failure is a config lookup that names a
# setting batch/pipeline.toml does not declare.
FAILURE_MODES = ["none", "missing_config"]
{%- endif %}


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
{%- if cookiecutter.include_lakehouse %}
        mode = context["params"]["failure_mode"]
        if mode == "schema_drift":
            # `customer_name` is not a column in lakehouse/create_tables.sql.
            return [{"id": 1, "customer_name": "example"}]
{%- endif %}
        return [{"id": 1, "name": "example"}]

{%- if cookiecutter.include_agentic %}

    @task(on_failure_callback=record_failure)
{%- else %}

    @task
{%- endif %}
    def load(records: list[dict], **context) -> None:
{%- if cookiecutter.include_lakehouse %}
        write_events(records)
{%- else %}
        mode = context["params"]["failure_mode"]
        if mode == "missing_config":
            # `output_file` is not declared in batch/pipeline.toml; the
            # setting that names the destination is `output_path`.
            write_events(records, output_setting="output_file")
            return
        write_events(records)
{%- endif %}
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
