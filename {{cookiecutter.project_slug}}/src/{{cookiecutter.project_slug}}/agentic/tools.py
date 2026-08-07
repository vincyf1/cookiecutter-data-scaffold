"""The agent's entire capability surface: four tools, no bash, no write.

propose_patch uses str_replace semantics but *stages* rather than applies.
Nothing on disk changes during the agent run, so a bad run costs an unused
.patch file -- and an eval can score a patch that was never applied.

Three of the four read; only one of them can *find* anything. That is
deliberate: a FailureContext names a dag_id and a task_id, never a file
path, so without list_repo_files an agent has nothing to turn the former
into the latter but guesswork.
"""

import difflib
from dataclasses import dataclass, field
from pathlib import Path

from {{ cookiecutter.project_slug }}.agentic.contracts import ProposedEdit

DEFAULT_TAIL_LINES = 200
DEFAULT_MAX_LOG_CHARS = 20000
DEFAULT_MAX_ENTRIES = 200

# Directories that are either machine-generated or this Pattern's own output.
# Listing them buries the handful of files the agent actually needs, and
# `artifacts`/`traces` would show it the wreckage of previous healing runs.
SKIP_DIRS = frozenset(
    {
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
        "node_modules",
        "build",
        "dist",
        "artifacts",
        "traces",
        "logs",
        "data",
    }
)


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


def list_repo_files(
    ctx: ToolContext, subdir: str = ".", max_entries: int = DEFAULT_MAX_ENTRIES
) -> str:
    """List source files under `subdir`, as paths read_repo_file accepts.

    Paths come back relative to the project root, not to `subdir`, so the
    agent can feed a line of this output straight into read_repo_file
    without doing path arithmetic on it.

    The containment check is the same one read_repo_file uses, and matters
    more here: this walks directories, so a symlink out of the tree would
    disclose host filenames even though the agent could never open them.
    """
    root = ctx.project_root.resolve()
    target = ctx.resolve(subdir)
    if not target.is_dir():
        raise ToolError(f"not a directory: {subdir}")

    found: list[str] = []
    for path in sorted(target.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if SKIP_DIRS.intersection(relative.parts[:-1]):
            continue
        # rglob follows directory symlinks, so re-check containment per hit
        # rather than trusting that a walk rooted inside the tree stays there.
        if not path.resolve().is_relative_to(root):
            continue
        found.append(relative.as_posix())
        if len(found) > max_entries:
            break

    if not found:
        return f"no files found under '{subdir}'"
    if len(found) > max_entries:
        return "\n".join(found[:max_entries] + [f"... truncated at {max_entries} entries"])
    return "\n".join(found)


def read_repo_file(ctx: ToolContext, path: str) -> str:
    target = ctx.resolve(path)
    if not target.is_file():
        raise ToolError(f"no such file: {path}")
    return target.read_text(errors="replace")


def _apply_staged_edits(content: str, edits: list[ProposedEdit], path: str) -> str:
    """Apply edits in order, each against the content as patched so far.

    Shared by propose_patch (to validate a new edit against what's already
    staged for the same path) and render_diff (to actually produce the
    patched text). Keeping the derivation in one place is what prevents the
    two views from drifting apart: propose_patch validates a candidate edit
    against exactly the content render_diff will apply it to.

    Raises ToolError, naming the path and edit index, if an edit's old_str
    is not unambiguously present in the content as of its turn. In normal
    operation propose_patch's own check makes this unreachable; it exists so
    a ProposedEdit appended directly to ctx.edits (bypassing propose_patch)
    fails loudly instead of being silently dropped.
    """
    result = content
    for index, edit in enumerate(edits):
        occurrences = result.count(edit.old_str)
        if occurrences != 1:
            raise ToolError(
                f"staged edit {index} for {path} could not be applied: old_str "
                f"occurs {occurrences} times in the content as patched so far"
            )
        result = result.replace(edit.old_str, edit.new_str, 1)
    return result


def propose_patch(
    ctx: ToolContext, path: str, old_str: str, new_str: str, rationale: str
) -> str:
    """Stage a single unambiguous replacement. Does not write to disk.

    Validated against the content as it would look after edits already
    staged for this same path -- not the on-disk original -- so a second
    edit that overlaps a first one is rejected here rather than silently
    dropped later by render_diff.
    """
    target = ctx.resolve(path)
    if not target.is_file():
        raise ToolError(f"no such file: {path}")
    prior_edits = [edit for edit in ctx.edits if edit.path == path]
    accumulated = _apply_staged_edits(target.read_text(), prior_edits, path)
    occurrences = accumulated.count(old_str)
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
        try:
            original = ctx.resolve(path).read_text()
        except FileNotFoundError as exc:
            raise ToolError(f"no such file: {path}") from exc
        patched = _apply_staged_edits(original, edits, path)
        diff = difflib.unified_diff(
            original.splitlines(keepends=True),
            patched.splitlines(keepends=True),
            fromfile=f"a/{path}",
            tofile=f"b/{path}",
        )
        chunks.append("".join(diff))
    return "".join(chunks)


@dataclass(frozen=True)
class ToolDefinition:
    """One tool, described without naming a model provider.

    `parameters` is plain JSON Schema. Rendering a definition into a
    particular provider's tool format belongs to that provider's runner --
    Anthropic nests the schema under `input_schema`, OpenAI under
    `function.parameters`. Keeping that rename out of this module is what
    lets a second provider reuse these tools rather than fork them.
    """

    name: str
    description: str
    parameters: dict


TOOL_DEFINITIONS = [
    ToolDefinition(
        name="read_task_log",
        description=(
            "Read the tail of the failing Airflow task's log. Call this first -- "
            "the exception message names the offending field."
        ),
        parameters={
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
    ),
    ToolDefinition(
        name="list_repo_files",
        description=(
            "List the project's source files, as paths you can pass straight to "
            "read_repo_file. Call this before reading anything: the failure names a "
            "DAG id and a task id, never a file path, so this is the only way to "
            "learn where the code lives. Do not guess filenames."
        ),
        parameters={
            "type": "object",
            "properties": {
                "subdir": {
                    "type": "string",
                    "description": (
                        "Directory to list, relative to the project root. "
                        "Defaults to the whole project."
                    ),
                }
            },
            "required": [],
            "additionalProperties": False,
        },
    ),
    ToolDefinition(
        name="read_repo_file",
        description=(
            "Read a source file, relative to the project root. Use this to inspect "
            "the DAG that produced the bad record and the DDL that defines the schema."
        ),
        parameters={
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path relative to the project root."}
            },
            "required": ["path"],
            "additionalProperties": False,
        },
    ),
    ToolDefinition(
        name="propose_patch",
        description=(
            "Stage a single replacement in a file. old_str must occur exactly once. "
            "Nothing is written to disk; the staged edits become a reviewable diff."
        ),
        parameters={
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
    ),
]
