from conftest import bake_with


def test_streaming_consumer_present_when_enabled(cookies):
    result = bake_with(cookies, include_streaming=True)
    slug = result.context["project_slug"]
    consumer_path = result.project_path / "src" / slug / "streaming" / "consumer.py"
    assert consumer_path.is_file()
    assert "confluent_kafka" in consumer_path.read_text()
