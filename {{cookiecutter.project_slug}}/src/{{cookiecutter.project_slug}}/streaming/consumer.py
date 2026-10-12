import json
import logging
from pathlib import Path

from confluent_kafka import Consumer

from {{ cookiecutter.project_slug }}.events import Event
from {{ cookiecutter.project_slug }}.sinks import create_event_sink

TOPIC = "events"
logger = logging.getLogger(__name__)

{%- if cookiecutter.include_lakehouse %}
DEFAULT_DESTINATION = Path("lakehouse.duckdb")
{%- else %}
DEFAULT_DESTINATION = Path("data/events/streaming")
{%- endif %}


def handle_message(raw_value: bytes | None) -> Event:
    return Event.from_mapping(json.loads(raw_value))


def run(
    bootstrap_servers: str = "localhost:9092",
    *,
    destination: Path = DEFAULT_DESTINATION,
    consumer_factory=Consumer,
    sink_factory=create_event_sink,
) -> None:
    consumer = consumer_factory({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "{{ cookiecutter.project_slug }}-consumer",
        "auto.offset.reset": "earliest",
    })
    try:
        consumer.subscribe([TOPIC])
        with sink_factory(destination) as sink:
            while True:
                msg = consumer.poll(1.0)
                if msg is None or msg.error():
                    continue
                raw_value = msg.value()
                try:
                    event = handle_message(raw_value)
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    logger.warning("Skipping malformed event (%d bytes)", len(raw_value or b""))
                    continue
                sink.write([event])
    finally:
        consumer.close()
