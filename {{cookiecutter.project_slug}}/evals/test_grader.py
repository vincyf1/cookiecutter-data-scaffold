import json
from typing import ClassVar
from unittest.mock import MagicMock, patch

from evals.grader import (
    GRADER_MODEL,
    ClaudeGrader,
    CriterionVerdict,
    FakeGrader,
    GradeReport,
    render_markdown,
    write_report,
)
from evals.harness import load_cases


def _report() -> GradeReport:
    return GradeReport(
        case_id="schema_drift",
        model="test-model",
        overall=4,
        verdicts=[
            CriterionVerdict(
                criterion="tools_called",
                score=5,
                passed=True,
                feedback="Read the log before patching.",
            ),
            CriterionVerdict(
                criterion="patch_targets",
                score=3,
                passed=True,
                feedback="Patched the DAG, though the DDL was also a valid target.",
            ),
        ],
        summary="Correct diagnosis, minimal patch.",
        suggested_rubric_gaps=["no criterion checks the patch is minimal"],
    )


def test_grader_model_comes_from_the_cookiecutter_variable():
    assert GRADER_MODEL == "{{ cookiecutter.agentic_grader_model }}"


def test_fake_grader_returns_its_fixture():
    case = load_cases()[0]
    report = FakeGrader(_report()).grade(case, trace=None, diff="")
    assert report.overall == 4
    assert report.verdicts[0].criterion == "tools_called"


def test_render_markdown_includes_every_criterion():
    markdown = render_markdown(_report())
    assert "| tools_called | 5 | pass |" in markdown
    assert "| patch_targets | 3 | pass |" in markdown
    assert "Correct diagnosis" in markdown
    assert "no criterion checks the patch is minimal" in markdown


def test_write_report_emits_json_and_markdown(tmp_path):
    path = write_report(_report(), tmp_path, timestamp="20260729T000000Z")

    assert path.name == "schema_drift-20260729T000000Z.json"
    saved = json.loads(path.read_text())
    assert saved["overall"] == 4
    assert saved["model"] == "test-model"

    markdown = (tmp_path / "schema_drift-20260729T000000Z.md").read_text()
    assert "tools_called" in markdown


def test_report_records_the_grader_model_for_cross_version_comparison():
    # Grader scores are not comparable across model versions; recording the
    # model is what lets a reader tell that. Assert the value round-trips,
    # not merely that the field is declared.
    report = _report()
    written = json.loads(report.model_dump_json())
    assert written["model"] == "test-model"
    assert "test-model" in render_markdown(report)


def test_render_markdown_marks_a_failed_criterion_as_fail():
    report = _report()
    report.verdicts[1].passed = False
    assert "| patch_targets | 3 | fail |" in render_markdown(report)


class _FakeTrace:
    # ClassVar, not a bare `list = []`: a mutable class attribute is shared by
    # every instance, so one test appending to it would leak into the next.
    elements: ClassVar[list] = []


def _mock_response(payload: dict):
    block = MagicMock()
    block.type = "text"
    block.text = json.dumps(payload)
    response = MagicMock()
    response.content = [block]
    return response


def test_claude_grader_does_not_collide_on_output_config_for_a_non_haiku_model():
    # claude-opus-5/claude-sonnet-5 get output_config={"effort": ...} back
    # from _model_kwargs, which would collide with the explicit
    # output_config= this call already passes for the json_schema format --
    # splatting both in the same call raises
    # `TypeError: got multiple values for keyword argument 'output_config'`.
    # The default claude-haiku-4-5 model never exercises this path (it gets
    # temperature=0 instead), so a test using only the default would not
    # catch a regression here.
    case = load_cases()[0]
    payload = {
        "overall": 3,
        "verdicts": [
            {"criterion": "tools_called", "score": 3, "passed": True, "feedback": "ok"}
        ],
        "summary": "fine",
        "suggested_rubric_gaps": [],
    }
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _mock_response(payload)

    with patch("anthropic.Anthropic", return_value=mock_client):
        report = ClaudeGrader(model="claude-opus-5").grade(case, trace=_FakeTrace(), diff="")

    assert report.overall == 3
    kwargs = mock_client.messages.create.call_args.kwargs
    assert kwargs["output_config"]["format"]["type"] == "json_schema"
    assert kwargs["output_config"]["effort"] == "medium"
