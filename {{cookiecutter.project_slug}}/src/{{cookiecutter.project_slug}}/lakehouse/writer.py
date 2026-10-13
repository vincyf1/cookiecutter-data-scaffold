from collections.abc import Sequence
from pathlib import Path
from types import TracebackType
from typing import Self

import duckdb

from {{ cookiecutter.project_slug }}.events import Event

DDL_PATH = Path(__file__).parent / "create_tables.sql"


class DuckDBEventSink:
    def __init__(self, destination: Path) -> None:
        self.destination = destination
        self.connection: duckdb.DuckDBPyConnection

    def __enter__(self) -> Self:
        self.connection = duckdb.connect(str(self.destination))
        try:
            self.connection.execute(DDL_PATH.read_text())
        except BaseException:
            self.connection.close()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.connection.close()

    def write(self, events: Sequence[Event]) -> None:
        if not events:
            return
        self.connection.execute("BEGIN")
        try:
            self.connection.executemany(
                "INSERT INTO events (id, name) VALUES (?, ?)",
                [(event.id, event.name) for event in events],
            )
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        self.connection.execute("COMMIT")
