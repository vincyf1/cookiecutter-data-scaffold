"""Output sink for the streaming consumer."""
{%- if cookiecutter.include_lakehouse %}
from {{ cookiecutter.project_slug }}.lakehouse.writer import get_connection, write_events as _write_to_lakehouse


def write_events(records: list[dict]) -> None:
    con = get_connection()
    _write_to_lakehouse(con, records)
{%- else %}
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_DIR = Path("data/events")


def write_events(records: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / "events_stream.parquet"
    new_table = pa.Table.from_pylist(records)
    if path.exists():
        new_table = pa.concat_tables([pq.read_table(path), new_table])
    pq.write_table(new_table, path, compression="snappy")
{%- endif %}
