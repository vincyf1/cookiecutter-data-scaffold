from {{ cookiecutter.project_slug }}.streaming.consumer import handle_message


def test_handle_message_parses_json():
    raw = b'{"id": 1, "name": "example"}'
    assert handle_message(raw) == {"id": 1, "name": "example"}
