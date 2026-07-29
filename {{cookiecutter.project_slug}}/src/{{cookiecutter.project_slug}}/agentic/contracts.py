"""Typed records shared across the agentic Pattern. No behavior lives here.

AgentResult is produced once per healing run and read by three consumers:
publisher.py renders `edits` into a diff, tracing.py persists `elements`,
and the eval suite asserts on all three fields.
"""

import json
from typing import Literal

from pydantic import BaseModel

ElementKind = Literal["agent_run", "llm_turn", "tool_call", "result"]
Confidence = Literal["high", "medium", "low"]


class FailureContext(BaseModel):
    """Everything the agent is told about the failure, assembled from Airflow."""

    dag_id: str
    dag_run_id: str
    task_id: str
    try_number: int
    exception_type: str
    exception_message: str
    log_path: str
    project_root: str


class ProposedEdit(BaseModel):
    """A staged str_replace. Never applied to disk during the agent run."""

    path: str
    old_str: str
    new_str: str
    rationale: str


class AgentDiagnosis(BaseModel):
    """The schema the model's final turn is constrained to."""

    root_cause: str
    offending_field: str | None = None
    confidence: Confidence
    patch_summary: str


class TraceElement(BaseModel):
    """One entry in the run's JSON array. Order is causal; `seq` replaces a span tree."""

    seq: int
    kind: ElementKind
    name: str
    status: str
    started_at: str
    ended_at: str | None = None
    duration_ms: int | None = None
    input_summary: dict | None = None
    output_summary: dict | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    error: str | None = None


class AgentResult(BaseModel):
    diagnosis: AgentDiagnosis
    edits: list[ProposedEdit]
    elements: list[TraceElement]


class Trace(BaseModel):
    """One document per failure event: correlation metadata plus the element array."""

    trace_id: str
    dag_id: str
    dag_run_id: str
    task_id: str
    try_number: int
    occurred_at: str
    status: str
    elements: list[TraceElement]

    def to_json_array(self) -> str:
        """Serialize the elements as a bare JSON array.

        This is the canonical artifact. Both sinks persist byte-identical
        output, so an eval asserting on a trace does not care which ran.
        """
        return json.dumps([e.model_dump(exclude_none=True) for e in self.elements])
