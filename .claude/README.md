# Autonomous DFBUGS Verification (Claude Code)

Claude Code–native QE orchestration for validating **DFBUGS** issues in **ON_QA** for a target ODF z-stream release.

## Architecture

| Layer | Location | Role |
|-------|----------|------|
| Orchestrator | `agents/orchestrator-coordinator.md` | Phase-ordered dispatch, aggregation |
| Specialists | `agents/*.md` | JIRA, repro, script, verify, infra, GitHub, report |
| Skills | `skills/*/SKILL.md` | Reusable MCP + ocs-ci patterns |
| Hooks | `hooks/` | Pre/post execution, safety validation |
| Memory | `memory/` | SQLite workflow state + issue history |
| Registry | `framework/registry/` | Agent + workflow catalog |
| Artifacts | `artifacts/DFBUGS-XXXX/` | Scripts, logs, evidence (gitignored) |

## Quick start

Replace `<odf-version>` with your target z-stream (e.g. `4.18`, `4.19`, `4.20`).

```bash
export JIRA_AGENT_WORKSPACE="${JIRA_AGENT_WORKSPACE:-$PWD/.claude/workspace}"

# Bootstrap workflow + prompt (ODF version is the last positional argument)
.claude/framework/orchestrator/run.sh --workflow zstream-issue-verification <odf-version>

# Dry-run (full workload, no JIRA/GitHub writes):
.claude/framework/orchestrator/run.sh --workflow zstream-issue-verification <odf-version> --dry-run

# Load ODF_VERSION for scripts / agents
eval "$(.claude/framework/lib/load_run_context.sh)"
echo "Verifying ODF $ODF_VERSION"

# Status / list workflows
.claude/framework/orchestrator/status.sh
.claude/framework/orchestrator/run.sh --list-workflows
```

In Claude Code: run **orchestrator-coordinator** with the prompt path printed by `run.sh`.
Run context skill: `.claude/skills/run-context/SKILL.md`

**Important:** `run.sh` only bootstraps the workspace and coordinator prompt.
The workflow runs when you start the **orchestrator-coordinator** agent in Claude Code
with that prompt. Check `active-run.json` to see which workflow id is active.

## Workflow (per issue)

1. **jira-analysis** — fetch issue, skip labels, verification plan
2. **cluster-compat** — OpenShift/ODF/cluster fit
3. **repro-extraction** — normalize reproduction steps
4. **script-generation** — pytest/bash under `artifacts/{KEY}/`
5. **Safety hook** — `hooks/safety/validate_script.sh`
6. **verification-execution** — run on cluster, collect evidence
7. **cluster-health-detection** — post-run health scan, unknown bug detection
8. **infra-diagnosis** — infra vs product on failure/degradation
9. **github-automation** — automation backlog (deduped)
10. **reporting** — per-issue + run summary

Policies: `.claude/configs/policies/safety.yaml`
Cluster profile: `.claude/configs/clusters/default.yaml`
Dry-run: `.claude/skills/dry-run/SKILL.md` — skips only JIRA/GitHub mutations

## Design principles

- Modular agents (not one giant prompt)
- MCP-driven JIRA/GitHub; `oc`/`pytest` via shell
- Shared workspace memory for resume/retry
- Human escalation when confidence &lt; 0.65 or destructive ops

See the workflow definition:
`framework/registry/workflows/zstream-issue-verification.yaml`
