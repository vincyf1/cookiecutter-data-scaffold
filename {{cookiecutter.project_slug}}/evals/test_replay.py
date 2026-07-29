"""Regression coverage for ReplayRunner surfacing a dispatch failure honestly.

ReplayRunner drives real tool dispatch from a recorded call list, so a
cassette's `propose_patch` can fail for real (e.g. its `old_str` no longer
matches the fixture). Earlier, AgentResult.edits was built from the
cassette's own hand-authored `edits` field rather than from what dispatch
actually staged onto ToolContext.edits -- so a failed dispatch and a
"successful" scored result could drift apart with nothing to catch it. This
file is the test that would have caught that: it points ReplayRunner at a
cassette whose propose_patch cannot apply and asserts the failure is
visible in the trace and the case is not scored as a pass.
"""

from pathlib import Path

from evals.harness import load_cases, score_case
from evals.replay import ReplayRunner

from {{ cookiecutter.project_slug }}.agentic.dags.healing_dag import run_healing
from {{ cookiecutter.project_slug }}.agentic.publisher import LocalPatchPublisher
from {{ cookiecutter.project_slug }}.agentic.tracing import JsonlSink

from .conftest import build_case_project

BROKEN_CASSETTE = Path(__file__).parent / "cassettes" / "schema_drift_broken_patch.json"
SCHEMA_DRIFT_CASE = next(case for case in load_cases() if case.id == "schema_drift")


def test_replay_marks_a_failed_dispatch_as_an_error_in_the_trace(tmp_path):
    project, ctx = build_case_project(tmp_path, SCHEMA_DRIFT_CASE)

    trace, published = run_healing(
        ctx,
        runner=ReplayRunner(BROKEN_CASSETTE),
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    tool_calls = [e for e in trace.elements if e.kind == "tool_call"]
    propose_patch_call = next(e for e in tool_calls if e.name == "propose_patch")

    # The failure must be visible on the element itself, not just buried in
    # a text summary that looks the same shape as a success.
    assert propose_patch_call.status == "error"
    assert propose_patch_call.error is not None

    # Nothing was actually staged, so no patch was rendered or published.
    assert published.patch_path is None


def test_replay_failure_is_not_scored_as_a_pass(tmp_path):
    project, ctx = build_case_project(tmp_path, SCHEMA_DRIFT_CASE)

    trace, published = run_healing(
        ctx,
        runner=ReplayRunner(BROKEN_CASSETTE),
        sink=JsonlSink(project / "traces"),
        publisher=LocalPatchPublisher(project / "artifacts"),
    )

    scored = score_case(SCHEMA_DRIFT_CASE, trace, published, replay=True)

    # patch_targets fails naturally: AgentResult.edits is derived from
    # ToolContext.edits (what dispatch actually staged), and dispatch never
    # staged anything because propose_patch raised. required["tools_called"]
    # can still be true (the four tool calls were attempted in order,
    # regardless of the last one's outcome) -- it is patch_targets, the
    # objectively-checkable criterion, that catches this.
    assert scored["required"]["patch_targets"] is False
    assert scored["passed"] is False
