{%- if not cookiecutter.include_lakehouse %}
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from {{ cookiecutter.project_slug }}.events import Event
from {{ cookiecutter.project_slug }}.sinks import PartitionedParquetEventSink


def test_parquet_sink_writes_an_immutable_partition_per_batch(tmp_path):
    with PartitionedParquetEventSink(tmp_path) as sink:
        sink.write([Event(id=1, name="first")])
        sink.write([Event(id=2, name="second")])

    partitions = sorted(tmp_path.glob("*.parquet"))
    assert len(partitions) == 2
    rows = sorted(
        (row for path in partitions for row in pq.read_table(path).to_pylist()),
        key=lambda row: row["id"],
    )
    assert rows == [{"id": 1, "name": "first"}, {"id": 2, "name": "second"}]


def test_parquet_sink_publishes_no_partial_file_when_write_fails(tmp_path, monkeypatch):
    def fail_write(table, path, **kwargs):
        path.write_bytes(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr("{{ cookiecutter.project_slug }}.sinks.pq.write_table", fail_write)

    with (
        PartitionedParquetEventSink(tmp_path) as sink,
        pytest.raises(OSError, match="disk full"),
    ):
        sink.write([Event(id=1, name="example")])

    assert list(tmp_path.iterdir()) == []


def test_parquet_sink_accepts_path_constructor_argument(tmp_path):
    destination = Path(tmp_path) / "events"
    with PartitionedParquetEventSink(destination) as sink:
        sink.write([])
    assert destination.is_dir()
{%- endif %}
