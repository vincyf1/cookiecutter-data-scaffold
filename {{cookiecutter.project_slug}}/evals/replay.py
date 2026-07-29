"""Deterministic replay of a recorded agent run.

This replays the tool sequence and final diagnosis rather than mocking the
runner outright, so tool dispatch, path confinement, and diff rendering all
execute for real. Only the model's decisions are canned.
"""

import json
from pathlib import Path

from {{ cookiecutter.project_slug }}.agentic.contracts import AgentDiagnosis, AgentResult
from {{ cookiecutter.project_slug }}.agentic.runner import dispatch_tool
from {{ cookiecutter.project_slug }}.agentic.tools import ToolContext, ToolError

CASSETTE_DIR = Path(__file__).parent / "cassettes"


def cassette_for(case_id: str) -> Path:
    return CASSETTE_DIR / f"{case_id}.json"


class ReplayRunner:
    """Drives real tool execution from a recorded call list."""

    def __init__(self, cassette_path: Path) -> None:
        self.cassette = json.loads(Path(cassette_path).read_text())

    def run(self, ctx, recorder) -> AgentResult:
        tool_ctx = ToolContext(
            project_root=Path(ctx.project_root), log_path=Path(ctx.log_path)
        )
        for call in self.cassette["tool_calls"]:
            try:
                with recorder.element(
                    "tool_call", call["name"], input_summary=call["input"]
                ) as box:
                    output = dispatch_tool(tool_ctx, call["name"], call["input"])
                    box["output_summary"] = {"chars": len(output)}
            except ToolError:
                # recorder.element's own `except Exception` branch already
                # marked this element status="error" (with the error text)
                # before re-raising -- that's what makes the failure visible
                # in the persisted trace instead of looking like a clean
                # tool call. Catching it here only stops one failed dispatch
                # from aborting the rest of a scripted replay: a cassette
                # has no model to consult for a retry, so there's nothing
                # useful to do with the failure except record it and move
                # on. `edits` below is built from `tool_ctx.edits` -- what
                # dispatch actually staged -- so a `propose_patch` that
                # failed here simply does not appear in it.
                continue

        diagnosis = AgentDiagnosis.model_validate(self.cassette["diagnosis"])
        recorder.add("result", "diagnosis", output_summary={"confidence": diagnosis.confidence})
        return AgentResult(
            diagnosis=diagnosis,
            edits=list(tool_ctx.edits),
            elements=list(recorder._elements),
        )
