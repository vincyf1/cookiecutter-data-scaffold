"""Output sink for the streaming consumer."""
{%- if cookiecutter.include_lakehouse %}
from {{ cookiecutter.project_slug }}.sinks import write_events  # noqa: F401
{%- else %}
from pathlib import Path

from {{ cookiecutter.project_slug }}.sinks import write_events as _write_events

OUTPUT_DIR = Path("data/events")


def write_events(records: list[dict]) -> None:
    _write_events(records, OUTPUT_DIR / "events_stream.parquet", accumulate=True)
{%- endif %}
