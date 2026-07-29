import os

import pytest

from evals.harness import load_cases, score_case
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
    overlap = set(case.score["required"]) & set(case.score["advisory"])
    assert not overlap, f"{case.id} lists {overlap} as both required and advisory"


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_case_passes_under_replay(case, tmp_path):
    project, ctx = build_case_project(tmp_path, case)
    trace, published = run_healing(
        ctx,
        runner=ReplayRunner(cassette_for(case.id)),
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    scored = score_case(case, trace, published)

    assert scored["passed"], scored["required"]


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
