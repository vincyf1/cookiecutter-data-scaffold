import json
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from evals.grader import (
    GRADER_MODEL,
    ClaudeGrader,
    CriterionVerdict,
    FakeGrader,
    GradeReport,
    grade_schema,
    render_markdown,
    write_report,
)
from evals.harness import load_cases
from {{ cookiecutter.project_slug }}.agentic.runner import DIAGNOSIS_SCHEMA

# Keywords the structured-output schema subset rejects outright: numerical and
# string constraints, and array constraints beyond minItems 0/1. Sending one is
# a 400 before the model is ever consulted.
#
# This test exists because every other test of the grader mocks the client, so
# the schema is never seen by the API's validator -- the grade schema shipped
# with `minimum`/`maximum` on its integer scores and stayed green until someone ran
# `-m graded`, which needs a key and is deselected by default. Express a bounded
# integer as an `enum` of its allowed values instead; that keyword is supported.
UNSUPPORTED_SCHEMA_KEYWORDS = frozenset(
    {
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        "maxItems",
        "uniqueItems",
    }
)


def _schema_keywords(schema) -> set[str]:
    """Every schema keyword appearing anywhere in a nested JSON Schema.

    Descends into the *values* under `properties` but never collects its keys:
    those are field names, so a field legitimately called "maximum" must not
    read as the constraint keyword of the same name.
    """
    found: set[str] = set()
    if not isinstance(schema, dict):
        return found
    for key, value in schema.items():
        found.add(key)
        if key == "properties" and isinstance(value, dict):
            for subschema in value.values():
                found |= _schema_keywords(subschema)
        elif isinstance(value, dict):
            found |= _schema_keywords(value)
        elif isinstance(value, list):
            for item in value:
                found |= _schema_keywords(item)
    return found


@pytest.mark.parametrize(
    ("name", "schema"),
    [
        ("grade_schema", grade_schema(["tools_called", "patch_targets"])),
        ("DIAGNOSIS_SCHEMA", DIAGNOSIS_SCHEMA),
    ],
)
def test_output_schemas_use_only_supported_keywords(name, schema):
    assert not _schema_keywords(schema) & UNSUPPORTED_SCHEMA_KEYWORDS


def test_the_keyword_walker_would_actually_catch_a_bad_schema():
    # Guard the guard: a walker that silently returned nothing would make the
    # test above pass on every schema, including the one that caused the 400.
    bad = {
        "type": "object",
        "properties": {"score": {"type": "integer", "minimum": 0, "maximum": 5}},
    }

    assert _schema_keywords(bad) & UNSUPPORTED_SCHEMA_KEYWORDS == {"minimum", "maximum"}


def test_a_field_named_like_a_constraint_is_not_mistaken_for_one():
    # `properties` keys are field names. A rubric criterion called "maximum"
    # is not the `maximum` keyword, and must not trip the check.
    fine = {"type": "object", "properties": {"maximum": {"type": "string"}}}

    assert not _schema_keywords(fine) & UNSUPPORTED_SCHEMA_KEYWORDS


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


def _criterion_enum(schema: dict) -> list[str]:
    return schema["properties"]["verdicts"]["items"]["properties"]["criterion"]["enum"]


@pytest.mark.parametrize("case", load_cases(), ids=lambda c: c.id)
def test_every_scored_criterion_is_nameable_by_the_grader(case):
    # The graded tier asserts every criterion in score.required appears in the
    # report. The grader can only name what the enum permits, so a criterion
    # listed under score but absent from expect would be unreportable -- and
    # would fail live, per case, at API cost. This catches it offline.
    nameable = set(_criterion_enum(grade_schema(sorted(case.expect))))
    scored = {name for tier in case.score.values() for name in tier}

    assert scored <= nameable, f"not nameable by the grader: {scored - nameable}"


def test_criterion_is_an_enum_of_the_rubrics_own_keys():
    # A free-form `criterion` let the grader return a label rather than a key:
    # `tools_called` came back as 'tools_called must_call' on one run and
    # 'tools_called: must_call all of [read_task_log, ...]' on the next, and a
    # key with two sub-constraints was split across two verdicts. Reports are
    # joined to the rubric by this exact string, so it cannot be free text.
    schema = grade_schema(["patch_targets", "tools_called"])

    assert _criterion_enum(schema) == ["patch_targets", "tools_called"]


def test_grade_sends_the_schema_built_from_that_cases_rubric():
    # Per-case, not module-level: two cases with different rubrics must not
    # share one enum, or each grader could name the other case's criteria.
    case = load_cases()[0]
    payload = {
        "overall": 5,
        "verdicts": [],
        "summary": "s",
        "suggested_rubric_gaps": [],
    }
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _mock_response(payload)

    with patch("anthropic.Anthropic", return_value=mock_client):
        ClaudeGrader().grade(case, trace=_FakeTrace(), diff="")

    sent = mock_client.messages.create.call_args.kwargs["output_config"]["format"]["schema"]
    assert _criterion_enum(sent) == sorted(case.expect)
