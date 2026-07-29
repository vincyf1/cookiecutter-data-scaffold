"""Cases are data: adding a case means adding a YAML file, not writing a test.

That matters in a generated project, whose failure cases will differ from
the ones this template ships.
"""

import json
from dataclasses import dataclass
from pathlib import Path

import yaml

CASES_DIR = Path(__file__).parent / "cases"
CONFIDENCE_ORDER = ["low", "medium", "high"]


@dataclass
class EvalCase:
    id: str
    failure_mode: str
    expect: dict
    score: dict


def load_cases() -> list[EvalCase]:
    cases = []
    for path in sorted(CASES_DIR.glob("*.yaml")):
        raw = yaml.safe_load(path.read_text())
        cases.append(
            EvalCase(
                id=raw["id"],
                failure_mode=raw["failure_mode"],
                expect=raw["expect"],
                score=raw["score"],
            )
        )
    return cases


def _tools_called(trace, expected: list[str]) -> bool:
    actual = [e.name for e in trace.elements if e.kind == "tool_call"]
    return actual == expected


def _patch_targets(published, expected: str) -> bool:
    if published.patch_path is None:
        return False
    meta = json.loads(Path(published.metadata_path).read_text())
    return any(edit["path"] == expected for edit in meta["edits"])


def _diagnosis_mentions(published, expected: list[str]) -> bool:
    meta = json.loads(Path(published.metadata_path).read_text())
    haystack = (meta["root_cause"] + " " + meta["body"]).lower()
    return all(term.lower() in haystack for term in expected)


def _min_confidence(published, expected: str) -> bool:
    meta = json.loads(Path(published.metadata_path).read_text())
    return CONFIDENCE_ORDER.index(meta["confidence"]) >= CONFIDENCE_ORDER.index(expected)


def score_case(case: EvalCase, trace, published) -> dict:
    checks = {
        "tools_called": lambda: _tools_called(trace, case.expect["tools_called"]),
        "patch_targets": lambda: _patch_targets(published, case.expect["patch_targets"]),
        "diagnosis_mentions": lambda: _diagnosis_mentions(
            published, case.expect["diagnosis_mentions"]
        ),
        "min_confidence": lambda: _min_confidence(published, case.expect["min_confidence"]),
    }
    required = {name: checks[name]() for name in case.score["required"]}
    advisory = {name: checks[name]() for name in case.score["advisory"]}
    return {
        "case_id": case.id,
        "required": required,
        "advisory": advisory,
        "passed": all(required.values()),
    }


def patch_text(published) -> str:
    """The rendered diff, read back from the artifact the publisher wrote.

    Re-deriving it from a fresh ToolContext would yield an empty string --
    the staged edits live on the context the runner used, not a new one.
    """
    if published.patch_path is None:
        return ""
    return Path(published.patch_path).read_text()
