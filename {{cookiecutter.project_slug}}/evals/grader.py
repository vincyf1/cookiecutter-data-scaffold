"""LLM grading of a healing run, with the same seam treatment as AgentRunner.

The grader is a second agent, so it gets FakeGrader/ClaudeGrader for the
same reason the runner does: the eval suite must not acquire a
non-deterministic dependency in the layer whose job is determinism.

It lives under evals/ rather than agentic/ because it is test
infrastructure -- the healing DAG never imports it.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, Field

GRADER_MODEL = "{{ cookiecutter.agentic_grader_model }}"
REPORTS_DIR = Path(__file__).parent / "reports"


# Scores are 0-5. Expressed as an enum rather than minimum/maximum because the
# structured-output schema subset rejects numerical constraints outright --
# `minimum`/`maximum`/`multipleOf` are a 400, while `enum` over numbers is
# supported. Same range, a keyword the API's validator accepts.
SCORE_VALUES = [0, 1, 2, 3, 4, 5]


class CriterionVerdict(BaseModel):
    criterion: str
    # Bounded here as well as in the wire schema. The enum constrains what the
    # grader can generate; this catches a hand-built or replayed verdict that
    # never went through the API at all.
    score: int = Field(ge=0, le=5)
    passed: bool
    feedback: str


class GradeReport(BaseModel):
    case_id: str
    model: str
    overall: int = Field(ge=0, le=5)
    verdicts: list[CriterionVerdict]
    summary: str
    suggested_rubric_gaps: list[str] = []


def grade_schema(criteria: list[str]) -> dict:
    """Build the wire schema for one case, keyed to that case's own rubric.

    `criterion` is an enum of the rubric's keys rather than a free string,
    because a free string invites the grader to write a *label* instead of an
    identifier. The same key came back as `tools_called must_call` on one run
    and `tools_called: must_call all of [read_task_log, read_repo_file,
    propose_patch]` on the next, and a key whose expectation holds two
    sub-constraints got split into two verdicts. Every one of those is a
    defensible thing to call a criterion; none of them can be joined back to
    the rubric that asked for it, which is what a report is for.

    Constraining the name also removes the grader's only outlet for noticing
    something the rubric missed -- so that observation belongs in
    `suggested_rubric_gaps`, which is required precisely so it has somewhere
    to go.
    """
    return {
        "type": "object",
        "properties": {
            "overall": {"type": "integer", "enum": SCORE_VALUES},
            "verdicts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "criterion": {"type": "string", "enum": list(criteria)},
                        "score": {"type": "integer", "enum": SCORE_VALUES},
                        "passed": {"type": "boolean"},
                        "feedback": {"type": "string"},
                    },
                    "required": ["criterion", "score", "passed", "feedback"],
                    "additionalProperties": False,
                },
            },
            "summary": {"type": "string"},
            "suggested_rubric_gaps": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["overall", "verdicts", "summary", "suggested_rubric_gaps"],
        "additionalProperties": False,
    }

GRADER_SYSTEM = """You grade an automated debugging agent's work against a rubric.

You receive the rubric, the agent's full execution trace, and the patch it
produced. You do NOT receive the agent's own instructions -- grading against
those would measure compliance, not correctness.

Score each rubric criterion independently from 0 to 5 and say specifically
what was right or wrong. A single overall number hides which criterion moved;
per-criterion feedback is the point.

Emit exactly one verdict per rubric criterion, and set `criterion` to that
criterion's key verbatim -- not a prettier restatement of it, and not one
verdict per sub-constraint of a key. Reports are joined back to the rubric by
that string. Where a key holds several sub-constraints, weigh them together
into a single verdict and use the feedback to say which one moved the score.

Also list criteria the rubric should have had but did not. If the agent did
something notably good or bad that no criterion covers, that is a rubric gap:
put it in `suggested_rubric_gaps`, which is where a missing criterion belongs.
Do not invent a verdict for it.
"""


class Grader(Protocol):
    def grade(self, case, trace, diff: str) -> GradeReport: ...


class FakeGrader:
    """Returns a fixture. Keeps report rendering testable without an API key."""

    def __init__(self, report: GradeReport) -> None:
        self.report = report

    def grade(self, case, trace, diff: str) -> GradeReport:
        return self.report


class ClaudeGrader:
    def __init__(self, model: str = GRADER_MODEL) -> None:
        self.model = model

    def grade(self, case, trace, diff: str) -> GradeReport:
        import anthropic

        from {{ cookiecutter.project_slug }}.agentic.runner import _model_kwargs

        elements = json.dumps(
            [e.model_dump(exclude_none=True) for e in trace.elements], indent=2
        )
        prompt = (
            f"# Rubric for case `{case.id}`\n\n"
            f"```yaml\n{json.dumps(case.expect, indent=2)}\n```\n\n"
            f"# Agent execution trace\n\n```json\n{elements}\n```\n\n"
            f"# Proposed patch\n\n```diff\n{diff or '(no patch produced)'}\n```\n\n"
            "Grade each rubric criterion."
        )

        # _model_kwargs(model) returns {"output_config": {...}} for
        # Sonnet/Opus and {"temperature": 0} for Haiku (see runner.py). This
        # call already passes its own `output_config` (the json_schema
        # format) explicitly, so blindly splatting **_model_kwargs(...)
        # alongside it would collide on the `output_config` keyword for any
        # non-Haiku model and raise `TypeError: got multiple values for
        # keyword argument 'output_config'`. runner.py's own
        # `_final_diagnosis` sidesteps this by not calling _model_kwargs at
        # all when it needs a structured-output call. Here we still want
        # per-tier tuning (e.g. Opus/Sonnet's `effort`), so instead of
        # dropping it we pop `output_config` out of the kwargs and merge it
        # into the schema's output_config rather than passing it twice.
        model_kwargs = _model_kwargs(self.model)
        extra_output_config = model_kwargs.pop("output_config", {})
        output_config = {
            "format": {
                "type": "json_schema",
                "schema": grade_schema(sorted(case.expect)),
            },
            **extra_output_config,
        }

        response = anthropic.Anthropic().messages.create(
            model=self.model,
            max_tokens=4000,
            system=GRADER_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
            output_config=output_config,
            **model_kwargs,
        )
        text = next(b.text for b in response.content if b.type == "text")
        payload = json.loads(text)
        return GradeReport(case_id=case.id, model=self.model, **payload)


def render_markdown(report: GradeReport) -> str:
    lines = [
        f"# Grade report — `{report.case_id}`",
        "",
        f"**Overall:** {report.overall}/5 · **Grader:** `{report.model}`",
        "",
        "| Criterion | Score | Result | Feedback |",
        "| --- | --- | --- | --- |",
    ]
    for verdict in report.verdicts:
        outcome = "pass" if verdict.passed else "fail"
        lines.append(
            f"| {verdict.criterion} | {verdict.score} | {outcome} | {verdict.feedback} |"
        )
    lines += ["", "## Summary", "", report.summary]
    if report.suggested_rubric_gaps:
        lines += ["", "## Suggested rubric gaps", ""]
        lines += [f"- {gap}" for gap in report.suggested_rubric_gaps]
        lines += [
            "",
            "_Advisory only. These are never assertions — they are how the rubric improves._",
        ]
    return "\n".join(lines) + "\n"


def write_report(
    report: GradeReport, reports_dir: Path = REPORTS_DIR, timestamp: str | None = None
) -> Path:
    """Feedback that only appears in pytest output gets ignored."""
    stamp = timestamp or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / f"{report.case_id}-{stamp}.json"
    json_path.write_text(report.model_dump_json(indent=2))
    (reports_dir / f"{report.case_id}-{stamp}.md").write_text(render_markdown(report))
    return json_path
