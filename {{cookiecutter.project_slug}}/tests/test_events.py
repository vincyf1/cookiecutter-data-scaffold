import pytest

from {{ cookiecutter.project_slug }}.events import Event


def test_event_from_mapping_returns_typed_event():
    assert Event.from_mapping({"id": 1, "name": "example"}) == Event(id=1, name="example")


@pytest.mark.parametrize(
    "record",
    [
        {},
        {"id": True, "name": "example"},
        {"id": 1, "name": None},
        {"id": 1.5, "name": "example"},
        {"id": 1},
        {"name": "example"},
        {"id": "1", "name": "example"},
        {"id": 1, "name": ""},
    ],
)
def test_event_from_mapping_rejects_invalid_records(record):
    with pytest.raises((TypeError, ValueError, KeyError)):
        Event.from_mapping(record)


def test_event_is_immutable():
    from dataclasses import FrozenInstanceError

    event = Event(id=1, name="example")
    with pytest.raises(FrozenInstanceError):
        event.name = "changed"
