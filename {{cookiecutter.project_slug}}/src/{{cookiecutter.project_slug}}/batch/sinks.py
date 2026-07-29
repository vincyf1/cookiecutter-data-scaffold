"""Output sink for the batch pipeline."""
{%- if cookiecutter.include_lakehouse %}
from {{ cookiecutter.project_slug }}.sinks import write_events  # noqa: F401
{%- else %}
from pathlib import Path

from {{ cookiecutter.project_slug }}.batch.config import get_setting
from {{ cookiecutter.project_slug }}.sinks import write_events as _write_events


def write_events(records: list[dict], output_setting: str = "output_path") -> None:
    """Write to whichever destination `output_setting` names in pipeline.toml.

    The setting name is a parameter rather than a constant so the demo DAG
    can inject a lookup that is not declared -- see failure_mode in
    batch/dags/example_dag.py.
    """
    _write_events(records, Path(get_setting(output_setting)))
{%- endif %}
