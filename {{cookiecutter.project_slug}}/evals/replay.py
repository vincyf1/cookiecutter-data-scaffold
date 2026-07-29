"""Deterministic replay of a recorded agent run.

This replays the tool sequence and final diagnosis rather than mocking the
runner outright, so tool dispatch, path confinement, and diff rendering all
execute for real. Only the model's decisions are canned.
"""

import json
from pathlib import Path

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    AgentResult,
    ProposedEdit,
)
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
            with recorder.element(
                "tool_call", call["name"], input_summary=call["input"]
            ) as box:
                try:
                    output = dispatch_tool(tool_ctx, call["name"], call["input"])
                    box["output_summary"] = {"chars": len(output)}
                except ToolError as exc:
                    box["output_summary"] = {"error": str(exc)}

        diagnosis = AgentDiagnosis.model_validate(self.cassette["diagnosis"])
        recorder.add("result", "diagnosis", output_summary={"confidence": diagnosis.confidence})
        return AgentResult(
            diagnosis=diagnosis,
            edits=[ProposedEdit.model_validate(e) for e in self.cassette["edits"]],
            elements=list(recorder._elements),
        )
