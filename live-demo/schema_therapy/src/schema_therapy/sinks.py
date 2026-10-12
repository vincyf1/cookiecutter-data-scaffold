"""Shared write_events sink used by the Batch and Streaming patterns."""
from schema_therapy.lakehouse.writer import get_connection, write_events as _write_to_lakehouse


def write_events(records: list[dict]) -> None:
    con = get_connection()
    _write_to_lakehouse(con, records)
