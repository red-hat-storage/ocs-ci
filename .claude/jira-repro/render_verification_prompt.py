#!/usr/bin/env python3
"""Render Claude prompts for AI-driven repro steps + verification scripts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def _read_json(path: Path) -> dict | None:
    if path.is_file():
        return json.loads(path.read_text())
    return None


def render_prompt(art: Path, key: str, *, odf_version: str, dry_run: bool) -> str:
    ctx = _read_json(art / "repro-context.json") or {}
    analysis = _read_json(art / "analysis.json") or {}
    cluster = _read_json(art / "cluster-fit.json") or {}

    lines = [
        f"# Verification generation: {key}",
        "",
        "You are the **script-generation** and **repro-extraction** agent for ocs-ci DFBUGS verification.",
        "**Do not** use placeholder tests (`assert True`, empty TODO).",
        "**Do not** rely on hardcoded issue-type templates in the framework.",
        "",
        "Read the issue context below, search the **ocs-ci** codebase for existing tests/helpers",
        "(e.g. `Grep`, `SemanticSearch`), and produce runnable verification for this specific bug.",
        "",
        f"**Issue:** {key}",
        f"**ODF version (workflow):** {odf_version or ctx.get('odf_version_target', '?')}",
        f"**Dry-run:** {dry_run}",
        "",
        "## Cluster snapshot",
        "```json",
        json.dumps(cluster, indent=2),
        "```",
        "",
        "## Issue context (from JIRA)",
        "```json",
        json.dumps(ctx, indent=2)[:50000],
        "```",
        "",
        "## Analysis",
        "```json",
        json.dumps(analysis, indent=2),
        "```",
        "",
        "## Your tasks (write all files under this directory)",
        "",
        f"Directory: `{art}`",
        "",
        "### 1. `repro-steps.yaml`",
        "- Detailed reproduction steps with concrete commands where possible.",
        "- Include `prerequisites`, `steps`, `verification_checks`, `pass_criteria`, `confidence`, `missing_info`.",
        "- Infer missing JIRA steps; document assumptions in `missing_info`.",
        "",
        "### 2. `reproduce.py`",
        "- Pytest module that **executes** the reproduction/verification on a live cluster.",
        "- Use `ocs_ci` helpers when appropriate (import from repo root on PYTHONPATH).",
        "- Include logging, retries for flaky ops, cleanup in fixtures/finally.",
        "- Assert real outcomes (logs, command exit codes, health) — **no** `assert True`.",
        "",
        "### 3. `verify.sh`",
        "- Wrapper: check KUBECONFIG, run pytest from ocs-ci repo root or artifact dir with correct PYTHONPATH.",
        "- Use `pytest -c /dev/null -o addopts=` if needed to avoid root pytest.ini conflicts.",
        "",
        "### 4. `test-environment.yaml`",
        "- Required env vars, ODF/OCP version constraints, namespaces, secrets, tools (`oc`, cluster access).",
        "",
        "### 5. `summary.md`",
        "- Short human summary of what the test does and pass/fail criteria.",
        "",
        "## Constraints",
        "- Read `.claude/configs/policies/safety.yaml` — no forbidden destructive commands.",
        "- Read `.claude/skills/ocs-ci-verify-script/SKILL.md` and `.claude/skills/update-logging/SKILL.md`.",
        "- If cluster ODF version does not match Target Release, document in repro-steps and add a check.",
        "- **Build version gate:** if JIRA states a product/ODF build (e.g. components line `4.20`), \
            cluster installed ODF must be >= that version; `execute_issue.sh` blocks \
                verification otherwise (see `cluster-fit.json` → `build_version_check`).",
        "",
        "## After writing files",
        "Log what you generated; tests will be executed via `verify.sh` by execute_issue.sh.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--issue", required=True)
    parser.add_argument("--art", type=Path, required=True)
    parser.add_argument("--odf-version", default="")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    text = render_prompt(
        args.art,
        args.issue.upper(),
        odf_version=args.odf_version,
        dry_run=args.dry_run,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(text)
    print(args.out)


if __name__ == "__main__":
    main()
