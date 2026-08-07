"""The AgentRunner seam -- where all non-determinism is isolated.

Above this boundary everything is ordinary Python. FakeRunner returns a
fixture, ClaudeRunner calls the live API, and nothing downstream knows
which one ran. That is what makes the default test suite deterministic:
temperature=0 does not guarantee identical output, this seam does.
"""

import copy
import json
from pathlib import Path
from typing import Protocol

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    AgentResult,
    FailureContext,
    ProposedEdit,
)
from {{ cookiecutter.project_slug }}.agentic.tools import (
    TOOL_DEFINITIONS,
    ToolContext,
    ToolDefinition,
    ToolError,
    list_repo_files,
    propose_patch,
    read_repo_file,
    read_task_log,
)
from {{ cookiecutter.project_slug }}.agentic.tracing import TraceRecorder, truncate

MODEL = "{{ cookiecutter.agentic_model }}"
MAX_ITERATIONS = 12
MAX_TOKENS = 8000

SYSTEM_PROMPT = (Path(__file__).parent / "prompts" / "diagnose.md").read_text()

DIAGNOSIS_SCHEMA = {
    "type": "object",
    "properties": {
        "root_cause": {"type": "string"},
        "offending_field": {"type": ["string", "null"]},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "patch_summary": {"type": "string"},
    },
    "required": ["root_cause", "offending_field", "confidence", "patch_summary"],
    "additionalProperties": False,
}

# Request parameters are tier-dependent, so they cannot be interpolated
# blindly from the cookiecutter variable. temperature is valid on Haiku 4.5
# and returns 400 on claude-opus-5; output_config.effort is the reverse.
# Unknown models get the intersection that is valid everywhere.
_MODEL_PARAMS: dict[str, dict] = {
    "claude-haiku-4-5": {"temperature": 0},
    "claude-sonnet-5": {"output_config": {"effort": "medium"}},
    "claude-opus-5": {"output_config": {"effort": "medium"}},
}


def anthropic_tool_specs(
    definitions: list[ToolDefinition] | None = None,
) -> list[dict]:
    """Render neutral tool definitions into Anthropic's `tools=` shape.

    This is the provider seam for tool descriptions. `tools.py` says what a
    tool is and does; this says how to describe it to one provider, which
    is why the schema gets renamed here rather than there. A second runner
    renders the same definitions differently -- OpenAI, for instance, wraps
    each in `{"type": "function", "function": {...}}` with the schema under
    `parameters` -- and reuses the tools themselves untouched.
    """
    return [
        {
            "name": definition.name,
            "description": definition.description,
            # deepcopy for the same reason _model_kwargs does it: these
            # definitions are module-level, and handing out a reference lets
            # one caller's edit corrupt every later render.
            "input_schema": copy.deepcopy(definition.parameters),
        }
        for definition in (TOOL_DEFINITIONS if definitions is None else definitions)
    ]


def _model_kwargs(model: str) -> dict:
    # deepcopy, not dict(...): _MODEL_PARAMS has one level of nesting
    # (output_config), and a shallow copy shares that nested dict with the
    # module-level table -- a caller mutating it (e.g. Task 10's
    # ClaudeGrader tweaking effort) would corrupt every later call.
    return copy.deepcopy(_MODEL_PARAMS.get(model, {}))


def _required(arguments: dict, name: str, tool: str) -> str:
    """Fetch a required tool argument, or raise something the model can fix.

    Tool arguments are model output, so a missing key is ordinary bad input,
    not a bug. A bare KeyError is not a ToolError, so it would escape
    ClaudeRunner.run's `except ToolError` handler, propagate out of the
    recorder's context manager, and fail the whole healing task -- turning a
    correctable mistake into a fatal one. Naming the missing key gives the
    model enough to retry.
    """
    try:
        return arguments[name]
    except KeyError:
        raise ToolError(
            f"tool '{tool}' requires argument '{name}', which was not provided; "
            f"received arguments: {sorted(arguments)}"
        ) from None


def dispatch_tool(tool_ctx: ToolContext, name: str, arguments: dict) -> str:
    """Route one tool call. The allowlist is this dict and nothing else."""
    if name == "read_task_log":
        return read_task_log(
            tool_ctx,
            task_id=_required(arguments, "task_id", name),
            tail_lines=arguments.get("tail_lines", 200),
        )
    if name == "list_repo_files":
        # subdir is optional: an agent that has no idea where to start should
        # be able to call this with no arguments at all.
        return list_repo_files(tool_ctx, subdir=arguments.get("subdir", "."))
    if name == "read_repo_file":
        return read_repo_file(tool_ctx, path=_required(arguments, "path", name))
    if name == "propose_patch":
        return propose_patch(
            tool_ctx,
            path=_required(arguments, "path", name),
            old_str=_required(arguments, "old_str", name),
            new_str=_required(arguments, "new_str", name),
            rationale=_required(arguments, "rationale", name),
        )
    raise ToolError(f"unknown tool: {name}")


def render_context(ctx: FailureContext) -> str:
    return (
        f"Task `{ctx.task_id}` in DAG `{ctx.dag_id}` (run `{ctx.dag_run_id}`, "
        f"try {ctx.try_number}) failed.\n\n"
        f"Exception: {ctx.exception_type}: {ctx.exception_message}\n\n"
        "Read the task log and the relevant source, then propose a patch."
    )


class AgentRunner(Protocol):
    def run(self, ctx: FailureContext, recorder: TraceRecorder) -> AgentResult: ...


class FakeRunner:
    """Replays a fixture. The default test tier runs entirely on this."""

    def __init__(
        self,
        diagnosis: AgentDiagnosis,
        edits: list[ProposedEdit],
        tool_sequence: list[str],
    ) -> None:
        self.diagnosis = diagnosis
        self.edits = edits
        self.tool_sequence = tool_sequence

    def run(self, ctx: FailureContext, recorder: TraceRecorder) -> AgentResult:
        for name in self.tool_sequence:
            with recorder.element("tool_call", name) as box:
                box["output_summary"] = {"replayed": True}
        recorder.add(
            "result",
            "diagnosis",
            output_summary={"confidence": self.diagnosis.confidence},
        )
        return AgentResult(
            diagnosis=self.diagnosis,
            edits=list(self.edits),
            elements=recorder.elements,
        )


class ClaudeRunner:
    """Drives the real tool loop against the Anthropic API."""

    def __init__(self, model: str = MODEL, max_iterations: int = MAX_ITERATIONS) -> None:
        self.model = model
        self.max_iterations = max_iterations

    def _client(self):
        import anthropic

        return anthropic.Anthropic()

    def run(self, ctx: FailureContext, recorder: TraceRecorder) -> AgentResult:
        client = self._client()
        tool_ctx = ToolContext(
            project_root=Path(ctx.project_root), log_path=Path(ctx.log_path)
        )
        messages: list[dict] = [{"role": "user", "content": render_context(ctx)}]
        tool_specs = anthropic_tool_specs()

        for _ in range(self.max_iterations):
            with recorder.element("llm_turn", f"turn_{len(messages)}") as box:
                response = client.messages.create(
                    model=self.model,
                    max_tokens=MAX_TOKENS,
                    system=SYSTEM_PROMPT,
                    tools=tool_specs,
                    messages=messages,
                    **_model_kwargs(self.model),
                )
                box["input_tokens"] = response.usage.input_tokens
                box["output_tokens"] = response.usage.output_tokens

            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason != "tool_use":
                break

            results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                with recorder.element(
                    "tool_call", block.name, input_summary=dict(block.input)
                ) as box:
                    try:
                        output = dispatch_tool(tool_ctx, block.name, dict(block.input))
                        is_error = False
                    except ToolError as exc:
                        output = str(exc)
                        is_error = True
                    box["output_summary"] = {"text": truncate(output, 500)}
                results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": truncate(output),
                        "is_error": is_error,
                    }
                )
            messages.append({"role": "user", "content": results})

        diagnosis = self._final_diagnosis(client, messages, recorder)
        return AgentResult(
            diagnosis=diagnosis,
            edits=list(tool_ctx.edits),
            elements=recorder.elements,
        )

    def _final_diagnosis(self, client, messages: list[dict], recorder) -> AgentDiagnosis:
        """One schema-constrained turn, so parsing cannot fail.

        This call already passes its own `output_config` (the json_schema
        format), and _model_kwargs(model) returns {"output_config": {...}}
        for Sonnet/Opus -- splatting both would collide on the
        `output_config` keyword. Mirror evals/grader.py's ClaudeGrader: pop
        `output_config` out of the tier kwargs and merge it into the schema's
        output_config, rather than dropping the tier tuning altogether.
        """
        model_kwargs = _model_kwargs(self.model)
        extra_output_config = model_kwargs.pop("output_config", {})
        output_config = {
            "format": {"type": "json_schema", "schema": DIAGNOSIS_SCHEMA},
            **extra_output_config,
        }
        with recorder.element("llm_turn", "diagnosis") as box:
            response = client.messages.create(
                model=self.model,
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                messages=[
                    *messages,
                    {
                        "role": "user",
                        "content": "Return your final diagnosis in the required schema.",
                    },
                ],
                output_config=output_config,
                **model_kwargs,
            )
            box["input_tokens"] = response.usage.input_tokens
            box["output_tokens"] = response.usage.output_tokens
        text = next(b.text for b in response.content if b.type == "text")
        parsed = AgentDiagnosis.model_validate(json.loads(text))
        recorder.add("result", "diagnosis", output_summary={"confidence": parsed.confidence})
        return parsed
