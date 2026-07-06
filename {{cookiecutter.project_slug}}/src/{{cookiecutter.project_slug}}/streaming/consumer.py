import json
import logging

from confluent_kafka import Consumer

from {{ cookiecutter.project_slug }}.streaming.sinks import write_events

TOPIC = "events"
logger = logging.getLogger(__name__)


def handle_message(raw_value: bytes) -> dict:
    return json.loads(raw_value)


def run(bootstrap_servers: str = "localhost:9092") -> None:
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "{{ cookiecutter.project_slug }}-consumer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                record = handle_message(msg.value())
            except json.JSONDecodeError:
                logger.warning("Skipping malformed message (%d bytes)", len(msg.value()))
                continue
            write_events([record])
    finally:
        consumer.close()
