import json

import pytest

from {{ cookiecutter.project_slug }}.agentic.contracts import (
    AgentDiagnosis,
    AgentResult,
    ProposedEdit,
)
from {{ cookiecutter.project_slug }}.agentic.publisher import (
    LocalPatchPublisher,
    default_publisher,
)
from {{ cookiecutter.project_slug }}.agentic.tools import ToolContext, propose_patch


@pytest.fixture()
def tool_ctx(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "dag.py").write_text('record = {"id": 1, "customer_name": "x"}\n')
    ctx = ToolContext(project_root=tmp_path, log_path=tmp_path / "load.log")
    propose_patch(ctx, "src/dag.py", "customer_name", "name", "matches DDL column")
    return ctx


@pytest.fixture()
def result():
    return AgentResult(
        diagnosis=AgentDiagnosis(
            root_cause="extract() emits customer_name but the DDL declares name",
            offending_field="customer_name",
            confidence="high",
            patch_summary="rename customer_name to name in extract()",
        ),
        edits=[
            ProposedEdit(
                path="src/dag.py",
                old_str="customer_name",
                new_str="name",
                rationale="matches DDL column",
            )
        ],
        elements=[],
    )


def test_local_publisher_writes_a_patch_file(tmp_path, tool_ctx, result):
    published = LocalPatchPublisher(tmp_path).publish("trace1", result, tool_ctx)

    patch = (tmp_path / "trace1.patch").read_text()
    assert published.patch_path.endswith("trace1.patch")
    assert "--- a/src/dag.py" in patch
    assert '+record = {"id": 1, "name": "x"}' in patch


def test_local_publisher_writes_reviewable_metadata(tmp_path, tool_ctx, result):
    published = LocalPatchPublisher(tmp_path).publish("trace1", result, tool_ctx)

    meta = json.loads((tmp_path / "trace1.json").read_text())
    assert meta["title"] == published.title
    assert "customer_name" in meta["body"]
    assert meta["confidence"] == "high"
    assert meta["trace_id"] == "trace1"


def test_local_publisher_opens_no_pull_request(tmp_path, tool_ctx, result):
    published = LocalPatchPublisher(tmp_path).publish("trace1", result, tool_ctx)
    assert published.url is None


def test_local_publisher_handles_a_run_with_no_edits(tmp_path, result):
    empty_ctx = ToolContext(project_root=tmp_path, log_path=tmp_path / "load.log")
    result.edits = []

    published = LocalPatchPublisher(tmp_path).publish("trace2", result, empty_ctx)

    assert published.patch_path is None
    assert not (tmp_path / "trace2.patch").exists()
    assert (tmp_path / "trace2.json").is_file()


def test_default_publisher_is_local_without_configuration(monkeypatch):
    monkeypatch.delenv("AGENTIC_GITHUB_REPO", raising=False)
    assert isinstance(default_publisher(), LocalPatchPublisher)
