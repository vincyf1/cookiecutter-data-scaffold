"""The agent's entire capability surface: three tools, no bash, no write.

propose_patch uses str_replace semantics but *stages* rather than applies.
Nothing on disk changes during the agent run, so a bad run costs an unused
.patch file -- and an eval can score a patch that was never applied.
"""

import difflib
from dataclasses import dataclass, field
from pathlib import Path

from {{ cookiecutter.project_slug }}.agentic.contracts import ProposedEdit

DEFAULT_TAIL_LINES = 200
DEFAULT_MAX_LOG_CHARS = 20000


class ToolError(Exception):
    """Raised for any rejected tool call. Returned to the model as a tool error."""


@dataclass
class ToolContext:
    project_root: Path
    log_path: Path
    max_log_chars: int = DEFAULT_MAX_LOG_CHARS
    edits: list[ProposedEdit] = field(default_factory=list)

    def resolve(self, path: str) -> Path:
        """Confine a model-supplied path to the project root.

        The input is model output, so this is load-bearing rather than
        defensive: resolve() collapses `..` and follows symlinks, then the
        containment check rejects anything that escaped.
        """
        root = self.project_root.resolve()
        candidate = (root / path).resolve()
        if not candidate.is_relative_to(root):
            raise ToolError(f"path '{path}' resolves outside the project root")
        return candidate


def read_task_log(ctx: ToolContext, task_id: str, tail_lines: int = DEFAULT_TAIL_LINES) -> str:
    """Return the tail of the failing task's log."""
    if not ctx.log_path.exists():
        raise ToolError(f"no log found for task '{task_id}' at {ctx.log_path}")
    lines = ctx.log_path.read_text(errors="replace").splitlines()
    text = "\n".join(lines[-tail_lines:])
    if len(text) > ctx.max_log_chars:
        text = text[-ctx.max_log_chars :]
    return text


def read_repo_file(ctx: ToolContext, path: str) -> str:
    target = ctx.resolve(path)
    if not target.is_file():
        raise ToolError(f"no such file: {path}")
    return target.read_text(errors="replace")


def propose_patch(
    ctx: ToolContext, path: str, old_str: str, new_str: str, rationale: str
) -> str:
    """Stage a single unambiguous replacement. Does not write to disk."""
    target = ctx.resolve(path)
    if not target.is_file():
        raise ToolError(f"no such file: {path}")
    occurrences = target.read_text().count(old_str)
    if occurrences != 1:
        raise ToolError(
            f"old_str occurs {occurrences} times in {path}; it must occur exactly once. "
            "Include more surrounding context to make it unique."
        )
    ctx.edits.append(
        ProposedEdit(path=path, old_str=old_str, new_str=new_str, rationale=rationale)
    )
    return f"staged edit {len(ctx.edits)} for {path}"


def render_diff(ctx: ToolContext) -> str:
    """Render all staged edits as a unified diff, still without writing."""
    chunks: list[str] = []
    by_path: dict[str, list[ProposedEdit]] = {}
    for edit in ctx.edits:
        by_path.setdefault(edit.path, []).append(edit)

    for path, edits in by_path.items():
        original = ctx.resolve(path).read_text()
        patched = original
        for edit in edits:
            patched = patched.replace(edit.old_str, edit.new_str, 1)
        diff = difflib.unified_diff(
            original.splitlines(keepends=True),
            patched.splitlines(keepends=True),
            fromfile=f"a/{path}",
            tofile=f"b/{path}",
        )
        chunks.append("".join(diff))
    return "".join(chunks)


TOOL_SPECS = [
    {
        "name": "read_task_log",
        "description": (
            "Read the tail of the failing Airflow task's log. Call this first -- "
            "the exception message names the offending field."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "The failing task id."},
                "tail_lines": {
                    "type": "integer",
                    "description": "How many trailing lines to return. Defaults to 200.",
                },
            },
            "required": ["task_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "read_repo_file",
        "description": (
            "Read a source file, relative to the project root. Use this to inspect "
            "the DAG that produced the bad record and the DDL that defines the schema."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path relative to the project root."}
            },
            "required": ["path"],
            "additionalProperties": False,
        },
    },
    {
        "name": "propose_patch",
        "description": (
            "Stage a single replacement in a file. old_str must occur exactly once. "
            "Nothing is written to disk; the staged edits become a reviewable diff."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "old_str": {"type": "string", "description": "Exact text to replace, once."},
                "new_str": {"type": "string", "description": "Replacement text."},
                "rationale": {"type": "string", "description": "Why this fixes the failure."},
            },
            "required": ["path", "old_str", "new_str", "rationale"],
            "additionalProperties": False,
        },
    },
]
