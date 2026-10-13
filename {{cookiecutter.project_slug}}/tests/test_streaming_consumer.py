from pathlib import Path

import pytest

from {{ cookiecutter.project_slug }}.events import Event
from {{ cookiecutter.project_slug }}.streaming.consumer import handle_message, run


class Message:
    def __init__(self, value: bytes):
        self._value = value

    def error(self):
        return None

    def value(self):
        return self._value


class Consumer:
    def __init__(self):
        self.messages = iter([Message(b"not json"), Message(b'{"id": 2, "name": "ok"}')])
        self.closed = False

    def subscribe(self, topics):
        self.topics = topics

    def poll(self, timeout):
        return next(self.messages)

    def close(self):
        self.closed = True


class Sink:
    def __init__(self):
        self.enter_count = 0
        self.exit_count = 0
        self.batches = []

    def __enter__(self):
        self.enter_count += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.exit_count += 1

    def write(self, events):
        self.batches.append(list(events))


def test_handle_message_returns_typed_event():
    assert handle_message(b'{"id": 1, "name": "example"}') == Event(id=1, name="example")


def test_handle_message_rejects_invalid_event():
    with pytest.raises((TypeError, ValueError, KeyError)):
        handle_message(b'{"id": "bad", "name": "example"}')


def test_run_owns_one_sink_for_consumer_lifetime():
    consumer = Consumer()
    sink = Sink()

    with pytest.raises(StopIteration):
        run(
            consumer_factory=lambda config: consumer,
            sink_factory=lambda destination: sink,
            destination=Path("unused"),
        )

    assert sink.enter_count == 1
    assert sink.exit_count == 1
    assert sink.batches == [[Event(id=2, name="ok")]]
    assert consumer.closed is True


@pytest.mark.parametrize(
    "payload",
    [None, b"null", b"[]", b'{"id": true, "name": "bad"}', b'{"id": 3, "customer_name": "x"}'],
)
def test_run_skips_invalid_payload_and_continues(payload):
    consumer = Consumer()
    consumer.messages = iter([Message(payload), Message(b'{"id": 2, "name": "ok"}')])
    sink = Sink()
    with pytest.raises(StopIteration):
        run(consumer_factory=lambda config: consumer, sink_factory=lambda destination: sink)
    assert sink.batches == [[Event(id=2, name="ok")]]
    assert consumer.closed


def test_storage_failure_closes_resources_and_propagates():
    consumer = Consumer()

    class FailingSink(Sink):
        def write(self, events):
            raise OSError("storage unavailable")

    sink = FailingSink()
    with pytest.raises(OSError, match="storage unavailable"):
        run(consumer_factory=lambda config: consumer, sink_factory=lambda destination: sink)
    assert sink.exit_count == 1
    assert consumer.closed
