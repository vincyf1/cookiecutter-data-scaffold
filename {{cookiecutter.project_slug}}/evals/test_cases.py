import json
import os

import pytest

from evals.grader import GRADER_MODEL, ClaudeGrader, write_report
from evals.harness import load_cases, patch_text, score_case
from evals.replay import ReplayRunner, cassette_for

from {{ cookiecutter.project_slug }}.agentic.dags.healing_dag import run_healing
from {{ cookiecutter.project_slug }}.agentic.publisher import LocalPatchPublisher
from {{ cookiecutter.project_slug }}.agentic.runner import ClaudeRunner
from {{ cookiecutter.project_slug }}.agentic.tracing import JsonlSink

from .conftest import build_case_project

requires_api_key = pytest.mark.skipif(
    not os.environ.get("ANTHROPIC_API_KEY"),
    reason="ANTHROPIC_API_KEY is not set",
)

CASES = load_cases()
CASE_IDS = [case.id for case in CASES]


def test_at_least_one_case_is_defined():
    assert CASES, "evals/cases/ contains no YAML cases"


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_case_declares_required_and_advisory_criteria(case):
    assert case.score["required"], f"{case.id} gates on nothing"
    tiers = {
        "required": set(case.score["required"]),
        "replay_only": set(case.score.get("replay_only", [])),
        "advisory": set(case.score["advisory"]),
    }
    for first, second in [
        ("required", "advisory"),
        ("required", "replay_only"),
        ("replay_only", "advisory"),
    ]:
        overlap = tiers[first] & tiers[second]
        assert not overlap, f"{case.id} lists {overlap} as both {first} and {second}"


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_case_gates_the_live_tier_on_intent_not_on_an_exact_transcript(case):
    # An exact tool-call list is a property of the cassette, not of correct
    # behaviour: a live model that opens one source file, or three, is not
    # wrong. Keeping exact_tool_sequence out of `required` is what stops the
    # live tier from failing on that. The ordering constraint below is what
    # the criterion is actually for.
    assert "exact_tool_sequence" not in case.score["required"]
    expected = case.expect["tools_called"]
    assert "read_task_log" in expected["must_call"]
    assert ["read_task_log", "propose_patch"] in [
        list(pair) for pair in expected["must_precede"]
    ]


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_case_passes_under_replay(case, tmp_path):
    project, ctx = build_case_project(tmp_path, case)
    trace, published = run_healing(
        ctx,
        runner=ReplayRunner(cassette_for(case.id)),
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    scored = score_case(case, trace, published, replay=True)

    assert scored["passed"], scored["required"]


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_tools_called_still_fails_an_agent_that_skips_the_log(case, tmp_path):
    # The looser criterion has to keep catching what it was there to catch:
    # a "diagnosis" that pattern-matched the exception name without ever
    # opening the log. Replay the cassette with that one call dropped.
    project, ctx = build_case_project(tmp_path, case)
    cassette = json.loads(cassette_for(case.id).read_text())
    cassette["tool_calls"] = [
        call for call in cassette["tool_calls"] if call["name"] != "read_task_log"
    ]
    path = tmp_path / "no_log_read.json"
    path.write_text(json.dumps(cassette))

    trace, published = run_healing(
        ctx,
        runner=ReplayRunner(path),
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    scored = score_case(case, trace, published)

    assert scored["required"]["tools_called"] is False
    assert scored["passed"] is False


@requires_api_key
@pytest.mark.live
@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_case_passes_against_the_live_model(case, tmp_path):
    project, ctx = build_case_project(tmp_path, case)
    trace, published = run_healing(
        ctx,
        runner=ClaudeRunner(),
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    scored = score_case(case, trace, published)

    assert scored["passed"], scored["required"]


@requires_api_key
@pytest.mark.graded
@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_case_is_graded_and_reported(case, tmp_path):
    project, ctx = build_case_project(tmp_path, case)
    trace, published = run_healing(
        ctx,
        runner=ClaudeRunner(),
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    report = ClaudeGrader().grade(case, trace, patch_text(published))
    path = write_report(report)

    print(f"\ngrade report: {path}")
    # These assert the grader produced a *usable* report, not that the agent
    # scored well -- this tier reports, it does not gate on quality.
    assert 0 <= report.overall <= 5
    assert report.model == GRADER_MODEL
    graded = {v.criterion for v in report.verdicts}
    assert set(case.score["required"]) <= graded, (
        f"grader skipped required criteria: {set(case.score['required']) - graded}"
    )
    assert path.is_file() and path.with_suffix(".md").is_file()
