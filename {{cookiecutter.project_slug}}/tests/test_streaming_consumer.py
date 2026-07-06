from unittest.mock import MagicMock, patch

from {{ cookiecutter.project_slug }}.streaming.consumer import handle_message, run


def test_handle_message_parses_json():
    raw = b'{"id": 1, "name": "example"}'
    assert handle_message(raw) == {"id": 1, "name": "example"}


@patch("{{ cookiecutter.project_slug }}.streaming.consumer.write_events")
@patch("{{ cookiecutter.project_slug }}.streaming.consumer.Consumer")
def test_run_skips_malformed_message_without_crashing(mock_consumer_cls, mock_write_events):
    bad_msg = MagicMock()
    bad_msg.error.return_value = None
    bad_msg.value.return_value = b"not json"

    good_msg = MagicMock()
    good_msg.error.return_value = None
    good_msg.value.return_value = b'{"id": 2, "name": "ok"}'

    mock_consumer = mock_consumer_cls.return_value
    mock_consumer.poll.side_effect = [bad_msg, good_msg, StopIteration()]

    try:
        run()
    except StopIteration:
        pass

    mock_write_events.assert_called_once_with([{"id": 2, "name": "ok"}])
    mock_consumer.close.assert_called_once()
