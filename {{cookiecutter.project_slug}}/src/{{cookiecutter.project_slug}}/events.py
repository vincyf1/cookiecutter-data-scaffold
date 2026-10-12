from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Event:
    id: int
    name: str

    def __post_init__(self) -> None:
        if isinstance(self.id, bool) or not isinstance(self.id, int):
            raise TypeError("event id must be an integer")
        if not isinstance(self.name, str):
            raise TypeError("event name must be a string")
        if not self.name:
            raise ValueError("event name must not be empty")

    @classmethod
    def from_mapping(cls, record: Mapping[str, Any]) -> "Event":
        return cls(id=record["id"], name=record["name"])
