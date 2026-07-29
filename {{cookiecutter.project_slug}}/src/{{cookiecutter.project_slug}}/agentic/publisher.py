"""Turning an AgentResult into something a human can review.

LocalPatchPublisher is the default because a freshly generated scaffold
usually has no remote, and a first run that tries to push is a bad first
impression. The patch artifact is also exactly what the evals score, so
the diff is the assertable output either way.
"""

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from {{ cookiecutter.project_slug }}.agentic.contracts import AgentResult
from {{ cookiecutter.project_slug }}.agentic.tools import ToolContext, ToolError, render_diff

# Relative to the project root, never to the process CWD. An Airflow worker
# runs with CWD=$AIRFLOW_HOME, which is not a mounted volume -- a
# CWD-relative default writes the patch somewhere the host never sees and a
# container restart erases. GitHubPublisher already rooted its own local
# copy this way; this is the constant both of them now share.
ARTIFACTS_SUBDIR = Path("artifacts")


@dataclass
class PublishedPatch:
    metadata_path: str
    title: str
    body: str
    patch_path: str | None = None
    url: str | None = None


def _title(result: AgentResult) -> str:
    return f"fix: {result.diagnosis.patch_summary}"


def _body(trace_id: str, result: AgentResult, render_error: str | None = None) -> str:
    lines = [
        "Proposed automatically by the agentic healing DAG. **Review before merging.**",
        "",
        f"**Root cause:** {result.diagnosis.root_cause}",
        f"**Confidence:** {result.diagnosis.confidence}",
    ]
    if result.diagnosis.offending_field:
        lines.append(f"**Offending field:** `{result.diagnosis.offending_field}`")
    lines += ["", "**Edits:**"]
    for edit in result.edits:
        lines.append(f"- `{edit.path}` — {edit.rationale}")
    if render_error:
        lines += [
            "",
            "**Patch could not be rendered.** The agent staged edit(s) above, but "
            "turning them into a diff failed, so no `.patch` file was produced. "
            "This is different from the agent choosing not to edit anything -- "
            "something changed on disk between staging and publishing.",
            f"**Render error:** {render_error}",
        ]
    lines += ["", f"Trace id: `{trace_id}`"]
    return "\n".join(lines)


class PRPublisher(Protocol):
    def publish(
        self, trace_id: str, result: AgentResult, tool_ctx: ToolContext
    ) -> PublishedPatch: ...


class LocalPatchPublisher:
    """Writes a .patch plus metadata. Requires no credentials."""

    def __init__(self, artifacts_dir: Path) -> None:
        self.artifacts_dir = Path(artifacts_dir)

    def publish(
        self, trace_id: str, result: AgentResult, tool_ctx: ToolContext
    ) -> PublishedPatch:
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        title = _title(result)

        # render_diff raises ToolError if a staged edit can no longer be
        # applied (e.g. its file was deleted or moved after staging). That
        # must not cost us the audit record -- a render failure is exactly
        # the moment a human most needs to know the agent ran and what it
        # attempted, so we degrade to patch_path=None and carry the error
        # into the metadata/body rather than letting the exception erase
        # the whole artifact. This is deliberately distinguishable from the
        # "no edits" case: render_error is set here, and absent there.
        patch_path: str | None = None
        render_error: str | None = None
        try:
            diff = render_diff(tool_ctx)
        except ToolError as exc:
            render_error = str(exc)
        else:
            if diff:
                target = self.artifacts_dir / f"{trace_id}.patch"
                target.write_text(diff)
                patch_path = str(target)

        body = _body(trace_id, result, render_error)

        metadata = {
            "trace_id": trace_id,
            "title": title,
            "body": body,
            "confidence": result.diagnosis.confidence,
            "root_cause": result.diagnosis.root_cause,
            "offending_field": result.diagnosis.offending_field,
            "patch_path": patch_path,
            "render_error": render_error,
            "edits": [e.model_dump() for e in result.edits],
        }
        metadata_path = self.artifacts_dir / f"{trace_id}.json"
        metadata_path.write_text(json.dumps(metadata, indent=2))

        return PublishedPatch(
            metadata_path=str(metadata_path),
            title=title,
            body=body,
            patch_path=patch_path,
        )


class GitHubPublisher:
    """Opt-in: branches, applies, pushes, and opens a PR via the gh CLI.

    Enable by setting AGENTIC_GITHUB_REPO. Requires push credentials and an
    authenticated `gh` in the Airflow worker -- which is exactly why it is
    not the default.

    The heal branch is cut from `origin/<base_branch>` (after an explicit
    fetch), not from whatever the worker's checkout currently has at HEAD,
    so the PR's base/diff relationship is well-defined regardless of the
    worker's prior state.

    Recovery: if any step after the branch is created fails, the worker's
    checkout is restored to whatever branch it was on before `publish` ran
    and the newly created heal branch is deleted, so retrying the same
    `trace_id` is possible rather than permanently blocked on "branch
    already exists". This is *not* a full transactional rollback: if the
    branch was already pushed to `origin` before a later step failed (e.g.
    `push` succeeded but `gh pr create` did not), the remote branch -- and
    possibly an opened PR from an earlier partial run -- can outlive this
    call and may need manual cleanup.
    """

    def __init__(self, repo: str, base_branch: str = "main") -> None:
        self.repo = repo
        self.base_branch = base_branch

    def publish(
        self, trace_id: str, result: AgentResult, tool_ctx: ToolContext
    ) -> PublishedPatch:
        local = LocalPatchPublisher(tool_ctx.project_root / ARTIFACTS_SUBDIR).publish(
            trace_id, result, tool_ctx
        )
        if local.patch_path is None:
            return local

        root = tool_ctx.project_root
        branch = f"agentic/heal-{trace_id[:8]}"

        def run(*args: str) -> subprocess.CompletedProcess:
            return subprocess.run(args, cwd=root, check=True, capture_output=True)

        def run_text(*args: str) -> subprocess.CompletedProcess:
            return subprocess.run(args, cwd=root, check=True, capture_output=True, text=True)

        original_branch = run_text(
            "git", "rev-parse", "--abbrev-ref", "HEAD"
        ).stdout.strip()

        run("git", "fetch", "origin", self.base_branch)
        run("git", "checkout", "-b", branch, f"origin/{self.base_branch}")
        try:
            run("git", "apply", local.patch_path)
            run("git", "commit", "-am", local.title)
            run("git", "push", "-u", "origin", branch)
            created = run_text(
                "gh", "pr", "create",
                "--repo", self.repo,
                "--base", self.base_branch,
                "--head", branch,
                "--title", local.title,
                "--body", local.body,
            )
        except Exception:
            subprocess.run(
                ["git", "checkout", original_branch], cwd=root, capture_output=True
            )
            subprocess.run(["git", "branch", "-D", branch], cwd=root, capture_output=True)
            raise

        local.url = created.stdout.strip()
        return local


def default_publisher(project_root: Path) -> PRPublisher:
    repo = os.environ.get("AGENTIC_GITHUB_REPO")
    if repo:
        return GitHubPublisher(repo)
    return LocalPatchPublisher(Path(project_root) / ARTIFACTS_SUBDIR)
