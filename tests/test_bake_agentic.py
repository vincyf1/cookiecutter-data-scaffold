import pytest

from conftest import bake_with


def test_agentic_flag_is_in_context(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    assert result.exit_code == 0
    assert result.context["include_agentic"] is True
    assert result.context["agentic_model"] == "claude-haiku-4-5"
    assert result.context["agentic_grader_model"] == "claude-haiku-4-5"


def test_agentic_without_batch_is_rejected(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=False)
    assert result.exit_code != 0


def test_agentic_with_batch_bakes_cleanly(cookies):
    result = bake_with(
        cookies,
        include_agentic=True,
        include_batch=True,
        include_streaming=False,
        include_lakehouse=False,
        include_dbt=False,
    )
    assert result.exit_code == 0


@pytest.mark.parametrize("agentic", [True, False])
def test_bake_succeeds_with_flag_either_way(cookies, agentic):
    result = bake_with(cookies, include_agentic=agentic, include_batch=True)
    assert result.exit_code == 0
