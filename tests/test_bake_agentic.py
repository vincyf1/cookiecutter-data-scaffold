import pytest

from conftest import bake_with


def test_agentic_flag_is_in_context(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    assert result.exit_code == 0
    assert result.context["include_agentic"] is True
    assert result.context["agentic_model"] == "claude-haiku-4-5"
    assert result.context["agentic_grader_model"] == "claude-haiku-4-5"


def test_agentic_without_batch_is_rejected(cookies, capfd):
    result = bake_with(cookies, include_agentic=True, include_batch=False)
    assert result.exit_code != 0
    assert result.exception is not None
    # pre_gen_project.py's guard runs in a subprocess and prints straight to
    # the inherited fd 1 (cookiecutter's FailedHookException carries no
    # message of its own beyond "exit status: 1"), so capfd -- not capsys --
    # is what sees it. Asserting on this text is what distinguishes "the
    # include_agentic-requires-include_batch guard fired" from "the bake
    # failed for some unrelated reason".
    out, _ = capfd.readouterr()
    assert "include_agentic requires include_batch" in out


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


def test_agentic_module_present_when_enabled(cookies):
    result = bake_with(cookies, include_agentic=True, include_batch=True)
    slug = result.context["project_slug"]
    agentic = result.project_path / "src" / slug / "agentic"
    assert (agentic / "contracts.py").is_file()
    assert (result.project_path / "tests" / "test_agentic_contracts.py").is_file()


def test_agentic_module_absent_when_disabled(cookies):
    result = bake_with(cookies, include_agentic=False, include_batch=True)
    slug = result.context["project_slug"]
    assert not (result.project_path / "src" / slug / "agentic").exists()
    assert not (result.project_path / "tests" / "test_agentic_contracts.py").exists()


def test_pydantic_dependency_tracks_the_flag(cookies):
    on = bake_with(cookies, include_agentic=True, include_batch=True)
    off = bake_with(cookies, include_agentic=False, include_batch=True)
    assert "pydantic" in (on.project_path / "pyproject.toml").read_text()
    assert "pydantic" not in (off.project_path / "pyproject.toml").read_text()
