import pytest

from {{ cookiecutter.project_slug }}.agentic.contracts import ProposedEdit
from {{ cookiecutter.project_slug }}.agentic.tools import (
    TOOL_SPECS,
    ToolContext,
    ToolError,
    propose_patch,
    read_repo_file,
    read_task_log,
    render_diff,
)


@pytest.fixture()
def ctx(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "dag.py").write_text('record = {"id": 1, "customer_name": "x"}\n')
    log = tmp_path / "load.log"
    log.write_text("\n".join(f"line {i}" for i in range(500)) + "\n")
    return ToolContext(project_root=tmp_path, log_path=log)


def test_read_task_log_returns_the_tail(ctx):
    out = read_task_log(ctx, task_id="load", tail_lines=3)
    assert out.strip().splitlines() == ["line 497", "line 498", "line 499"]


def test_read_task_log_is_character_capped(tmp_path):
    log = tmp_path / "load.log"
    log.write_text("x" * 100_000)
    ctx = ToolContext(project_root=tmp_path, log_path=log, max_log_chars=1000)
    assert len(read_task_log(ctx, task_id="load")) <= 1100


def test_read_repo_file_reads_within_the_root(ctx):
    assert "customer_name" in read_repo_file(ctx, "src/dag.py")


@pytest.mark.parametrize("bad", ["../etc/passwd", "/etc/passwd", "src/../../etc/passwd"])
def test_read_repo_file_rejects_paths_outside_the_root(ctx, bad):
    with pytest.raises(ToolError, match="outside the project root"):
        read_repo_file(ctx, bad)


def test_read_repo_file_rejects_a_symlink_escape(ctx, tmp_path):
    outside = tmp_path.parent / "outside.txt"
    outside.write_text("secret")
    (tmp_path / "link.txt").symlink_to(outside)
    with pytest.raises(ToolError, match="outside the project root"):
        read_repo_file(ctx, "link.txt")


def test_propose_patch_stages_an_edit_without_touching_disk(ctx):
    before = (ctx.project_root / "src" / "dag.py").read_text()

    propose_patch(ctx, "src/dag.py", "customer_name", "name", "matches DDL column")

    assert (ctx.project_root / "src" / "dag.py").read_text() == before
    assert len(ctx.edits) == 1
    assert ctx.edits[0].new_str == "name"


def test_propose_patch_rejects_a_string_that_does_not_occur(ctx):
    with pytest.raises(ToolError, match="0 times"):
        propose_patch(ctx, "src/dag.py", "nonexistent", "x", "why")


def test_propose_patch_rejects_an_ambiguous_string(ctx):
    (ctx.project_root / "src" / "dup.py").write_text("a = 1\na = 1\n")
    with pytest.raises(ToolError, match="2 times"):
        propose_patch(ctx, "src/dup.py", "a = 1", "a = 2", "why")


def test_render_diff_produces_a_unified_diff(ctx):
    propose_patch(ctx, "src/dag.py", "customer_name", "name", "matches DDL column")

    diff = render_diff(ctx)

    assert "--- a/src/dag.py" in diff
    assert "+++ b/src/dag.py" in diff
    assert "-record" in diff
    assert '+record = {"id": 1, "name": "x"}' in diff


def test_render_diff_is_empty_with_no_edits(ctx):
    assert render_diff(ctx) == ""


def test_render_diff_applies_two_non_overlapping_edits_to_the_same_file(ctx):
    # Regression: this legitimate case -- two edits to the same file that
    # don't touch each other's text -- must keep working. Only edits that
    # actually overlap should be rejected.
    propose_patch(ctx, "src/dag.py", "customer_name", "name", "matches DDL column")
    propose_patch(ctx, "src/dag.py", '"id": 1', '"id": 2', "bump the id")

    diff = render_diff(ctx)

    assert '+record = {"id": 2, "name": "x"}' in diff


def test_propose_patch_rejects_an_edit_that_overlaps_a_prior_staged_edit(ctx):
    # Regression for silent edit loss: propose_patch must validate against
    # the content as it would look *after* edits already staged for this
    # path, not the on-disk original. Here "aXb" -> "aYb" is staged first;
    # "Xb" no longer occurs in the accumulated content, so the second stage
    # attempt must be rejected up front rather than silently dropped later
    # by render_diff.
    (ctx.project_root / "src" / "overlap.py").write_text("aXb\n")
    propose_patch(ctx, "src/overlap.py", "aXb", "aYb", "first edit")

    with pytest.raises(ToolError, match="0 times"):
        propose_patch(ctx, "src/overlap.py", "Xb", "Zb", "conflicting second edit")


def test_render_diff_raises_if_a_staged_edit_cannot_be_applied(ctx):
    # An edit appended directly to ctx.edits bypasses propose_patch's
    # validation entirely -- exactly the door Task 8 uses when it assigns
    # tool_ctx.edits = list(result.edits). render_diff must fail loudly
    # rather than silently produce a diff missing this edit.
    ctx.edits.append(
        ProposedEdit(
            path="src/dag.py",
            old_str="nonexistent text",
            new_str="x",
            rationale="bogus",
        )
    )
    with pytest.raises(ToolError, match="src/dag.py"):
        render_diff(ctx)


def test_render_diff_rejects_a_staged_edit_for_a_missing_file(ctx):
    ctx.edits.append(
        ProposedEdit(path="src/gone.py", old_str="a", new_str="b", rationale="bogus")
    )
    with pytest.raises(ToolError, match="no such file"):
        render_diff(ctx)


def test_tool_specs_expose_exactly_three_tools():
    assert {spec["name"] for spec in TOOL_SPECS} == {
        "read_task_log",
        "read_repo_file",
        "propose_patch",
    }
