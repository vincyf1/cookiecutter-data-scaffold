from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any

# Record keys are upstream data, and a drift message is the healing agent's
# evidence. Showing only a few short keys keeps a producer from putting a
# paragraph, or thousands of keys, in front of the model.
MAX_REPORTED_KEYS = 5
MAX_REPORTED_KEY_LENGTH = 40


class SchemaDriftError(ValueError):
    """A record lacks a field the Event contract declares.

    Its message is the healing agent's starting evidence, so it names what
    is missing, what arrived instead, and what the contract declares.
    """


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
        declared = [field.name for field in fields(cls)]
        missing = [name for name in declared if name not in record]
        if missing:
            # Extra fields alone are not drift: a producer may add one before
            # the contract learns about it. Only a missing field is.
            undeclared = [key for key in record if key not in declared]
            detail = f"record lacks field(s) {missing}"
            if undeclared:
                shown = [str(key)[:MAX_REPORTED_KEY_LENGTH] for key in undeclared]
                detail += f" and carries undeclared field(s) {shown[:MAX_REPORTED_KEYS]}"
                if len(shown) > MAX_REPORTED_KEYS:
                    detail += f" (+{len(shown) - MAX_REPORTED_KEYS} more)"
            raise SchemaDriftError(
                f"{detail}; the Event contract declares {declared}. "
                "Either the producer renamed a field or the contract is out of date."
            )
        return cls(id=record["id"], name=record["name"])
