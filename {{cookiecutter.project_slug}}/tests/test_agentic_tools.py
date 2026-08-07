import pytest

from {{ cookiecutter.project_slug }}.agentic.contracts import ProposedEdit
from {{ cookiecutter.project_slug }}.agentic.tools import (
    TOOL_DEFINITIONS,
    ToolContext,
    ToolDefinition,
    ToolError,
    list_repo_files,
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
    with pytest.raises(ToolError, match=r"src/dag\.py"):
        render_diff(ctx)


def test_render_diff_rejects_a_staged_edit_for_a_missing_file(ctx):
    ctx.edits.append(
        ProposedEdit(path="src/gone.py", old_str="a", new_str="b", rationale="bogus")
    )
    with pytest.raises(ToolError, match="no such file"):
        render_diff(ctx)


def test_list_repo_files_returns_paths_read_repo_file_accepts(ctx):
    # The two tools are a pair: whatever this one prints, the other one has
    # to be able to open. Anything else and the agent gets a listing it
    # cannot act on -- which is the failure this tool exists to prevent.
    listing = list_repo_files(ctx).splitlines()

    assert "src/dag.py" in listing
    for path in listing:
        assert read_repo_file(ctx, path) is not None


def test_list_repo_files_finds_a_file_no_one_could_have_guessed(ctx):
    # The regression this tool was added for: the failure context names a
    # DAG *id*, never a filename, so an agent with only read_repo_file has
    # to guess the path. The live evals caught it doing exactly that --
    # seventeen misses, down to casing permutations, then no patch.
    deep = ctx.project_root / "src" / "pkg" / "batch" / "dags" / "example_dag.py"
    deep.parent.mkdir(parents=True)
    deep.write_text("output_setting = 'output_file'\n")

    assert "src/pkg/batch/dags/example_dag.py" in list_repo_files(ctx).splitlines()


def test_list_repo_files_scopes_to_a_subdirectory(ctx):
    (ctx.project_root / "evals").mkdir()
    (ctx.project_root / "evals" / "noise.py").write_text("x = 1\n")

    listing = list_repo_files(ctx, subdir="src").splitlines()

    assert listing == ["src/dag.py"]


@pytest.mark.parametrize("bad", ["../..", "/etc", "src/../../etc"])
def test_list_repo_files_rejects_paths_outside_the_root(ctx, bad):
    with pytest.raises(ToolError, match="outside the project root"):
        list_repo_files(ctx, subdir=bad)


def test_list_repo_files_rejects_a_file_as_a_subdir(ctx):
    with pytest.raises(ToolError, match="not a directory"):
        list_repo_files(ctx, subdir="src/dag.py")


def test_list_repo_files_skips_noise_directories(ctx):
    for noisy in (".git", "__pycache__", ".venv", "artifacts", "traces"):
        directory = ctx.project_root / noisy
        directory.mkdir()
        (directory / "junk.py").write_text("x = 1\n")

    listing = list_repo_files(ctx).splitlines()

    assert "src/dag.py" in listing
    assert [path for path in listing if "junk.py" in path] == []


def test_list_repo_files_says_so_when_it_truncates(ctx):
    # A silently truncated listing reads as "this is the whole repo", which
    # would send the agent back to guessing for anything below the cut.
    for index in range(60):
        (ctx.project_root / f"file_{index:03d}.py").write_text("x = 1\n")

    listing = list_repo_files(ctx, max_entries=25)

    assert len(listing.splitlines()) == 26
    assert listing.splitlines()[-1] == "... truncated at 25 entries"


def test_list_repo_files_reports_an_empty_directory_rather_than_nothing(ctx):
    (ctx.project_root / "empty").mkdir()

    assert list_repo_files(ctx, subdir="empty") == "no files found under 'empty'"


def test_tool_definitions_expose_exactly_four_tools():
    assert {definition.name for definition in TOOL_DEFINITIONS} == {
        "read_task_log",
        "list_repo_files",
        "read_repo_file",
        "propose_patch",
    }


def test_tool_definitions_carry_plain_json_schema():
    for definition in TOOL_DEFINITIONS:
        assert isinstance(definition, ToolDefinition)
        assert definition.description
        assert definition.parameters["type"] == "object"
        assert "properties" in definition.parameters


def test_tool_definitions_name_no_provider():
    # This module owns what the tools DO. How a tool is described to a
    # particular model is the runner's job, because providers disagree:
    # Anthropic nests the schema under `input_schema`, OpenAI under
    # `function.parameters`. A provider-shaped key appearing here means the
    # coupling has leaked back across the seam.
    fields = set(ToolDefinition.__dataclass_fields__)
    assert fields == {"name", "description", "parameters"}
    for provider_key in ("input_schema", "function", "parameters_schema"):
        assert provider_key not in fields or provider_key == "parameters"
