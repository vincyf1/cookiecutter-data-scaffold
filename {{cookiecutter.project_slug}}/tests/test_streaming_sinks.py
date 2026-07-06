{%- if not cookiecutter.include_lakehouse %}
import pyarrow.parquet as pq

from {{ cookiecutter.project_slug }}.streaming.sinks import OUTPUT_DIR, write_events


def test_write_events_accumulates_across_calls(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    write_events([{"id": 1, "name": "first"}])
    write_events([{"id": 2, "name": "second"}])

    table = pq.read_table(OUTPUT_DIR / "events_stream.parquet")
    rows = sorted(table.to_pylist(), key=lambda r: r["id"])
    assert rows == [
        {"id": 1, "name": "first"},
        {"id": 2, "name": "second"},
    ]
{%- endif %}
