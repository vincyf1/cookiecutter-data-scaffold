"""Shared write_events sink used by the Batch and Streaming patterns."""
{%- if cookiecutter.include_lakehouse %}
from {{ cookiecutter.project_slug }}.lakehouse.writer import get_connection, write_events as _write_to_lakehouse


def write_events(records: list[dict]) -> None:
    con = get_connection()
    _write_to_lakehouse(con, records)
{%- else %}
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def write_events(records: list[dict], path: Path, *, accumulate: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records)
    if accumulate and path.exists():
        table = pa.concat_tables([pq.read_table(path), table])
    pq.write_table(table, path, compression="snappy")
{%- endif %}
