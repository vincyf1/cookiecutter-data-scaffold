from collections.abc import Sequence
from pathlib import Path
from types import TracebackType
from typing import Protocol, Self
{%- if not cookiecutter.include_lakehouse %}
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
{%- endif %}

from {{ cookiecutter.project_slug }}.events import Event
{%- if cookiecutter.include_lakehouse %}
from {{ cookiecutter.project_slug }}.lakehouse.writer import DuckDBEventSink
{%- endif %}


class EventSink(Protocol):
    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    def write(self, events: Sequence[Event]) -> None: ...


{%- if not cookiecutter.include_lakehouse %}
class PartitionedParquetEventSink:
    def __init__(self, destination: Path) -> None:
        self.destination = destination

    def __enter__(self) -> Self:
        self.destination.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    def write(self, events: Sequence[Event]) -> None:
        if not events:
            return
        partition_id = uuid4().hex
        final_path = self.destination / f"part-{partition_id}.parquet"
        temporary_path = self.destination / f".{partition_id}.tmp"
        table = pa.Table.from_pylist(
            [{"id": event.id, "name": event.name} for event in events]
        )
        try:
            pq.write_table(table, temporary_path, compression="snappy")
            temporary_path.replace(final_path)
        finally:
            temporary_path.unlink(missing_ok=True)
{%- endif %}


def create_event_sink(destination: Path) -> EventSink:
{%- if cookiecutter.include_lakehouse %}
    return DuckDBEventSink(destination)
{%- else %}
    return PartitionedParquetEventSink(destination)
{%- endif %}
