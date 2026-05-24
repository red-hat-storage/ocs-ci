# Z-Stream Verification Agentic Workflow — Detailed Steps

## Overview

The z-stream verification workflow is a multi-agent, phase-based pipeline that autonomously verifies DFBUGS JIRA issues in `ON_QA` status for a target ODF z-stream release (e.g., `4.18`, `4.19`). It discovers issues from JIRA, generates reproduction scripts using AI, executes them on a live OpenShift cluster, scans for regressions, and updates JIRA with the results.

The entire system is declarative: the workflow YAML (`.claude/framework/registry/workflows/zstream-issue-verification.yaml`) defines phases, agent order, and conditional branches. Agents read configuration from YAML — no hardcoded values.

---

## Architecture Diagram

```
User
 │
 ▼
┌──────────────────────────────────────────────────────────┐
│  /zstream-verify --dry-run 4.19                          │
│  (Claude Code slash command)                             │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│  BOOTSTRAP (run.sh)                                      │
│  • Parse version + flags                                 │
│  • Init workspace + active-run.json                      │
│  • Preflight MCP check                                   │
│  • Render coordinator prompt                             │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│  ORCHESTRATOR-COORDINATOR (Sonnet)                       │
│  Reads workflow YAML, dispatches agents phase by phase   │
│                                                          │
│  Phase 1: PREFLIGHT                                      │
│    ├─ check_workspace.sh                                 │
│    ├─ setup_mcp.sh                                       │
│    └─ preflight_mcp.sh                                   │
│                                                          │
│  Phase 2: DISCOVERY                                      │
│    └─ jira-discovery agent (Haiku)                       │
│       └─ outputs: discovery/issues.json                  │
│                                                          │
│  Phase 3: PER-ISSUE PIPELINE (for each issue key)        │
│    ├─ 1. jira-analysis (Haiku)                           │
│    ├─ 2. cluster-compat (Haiku)                          │
│    ├─ 3. repro-extraction (Sonnet)                       │
│    ├─ 4. script-generation (Sonnet)                      │
│    ├─ 5. [Hook] safety/validate_script.sh                │
│    ├─ 6. verification-execution (Sonnet)                 │
│    ├─ 7. [Hook] cluster_health_collect.sh                │
│    ├─ 8. cluster-health-detection (Sonnet)               │
│    ├─ 9. infra-diagnosis (Sonnet) — conditional          │
│    ├─ 10. github-automation (Haiku) — conditional        │
│    ├─ 11. reporting (Haiku)                              │
│    └─ 12. [Hook] sync_memory.sh                          │
│                                                          │
│  Phase 4: FINALIZE                                       │
│    └─ reporting agent aggregates summary                 │
└──────────────────────────────────────────────────────────┘
```

---

## Step-by-Step Workflow

### Step 0: Entry Point — Slash Command

**Command:**
```bash
/zstream-verify [--dry-run] <odf-version>
```

**What happens:**
- Claude Code parses the arguments
- Calls `.claude/framework/orchestrator/run.sh` with the same arguments
- After bootstrap, the coordinator agent takes over

**File:** `.claude/commands/zstream-verify.md`

---

### Step 1: Bootstrap (run.sh)

**Script:** `.claude/framework/orchestrator/run.sh`

| Sub-step | Action | Output |
|----------|--------|--------|
| 1.1 | Parse `--dry-run` flag and `<odf-version>` positional arg | Variables set |
| 1.2 | Run `init_workspace.sh` — create workspace directory tree | `$JIRA_AGENT_WORKSPACE/` (default: `.claude/workspace`) |
| 1.3 | Run `set_run_config.py` — validate workflow exists, generate run metadata | `active-run.json`, `run-config.json` |
| 1.4 | Run `init_run_log.sh` — create log infrastructure | `logs/run.log` |
| 1.5 | Run `preflight_mcp.sh` — verify `redhat-jira` MCP server is reachable | Pass/fail |
| 1.6 | Run `render_prompt.py` — generate coordinator prompt from workflow YAML | `workflow-zstream-issue-verification-prompt.md` |
| 1.7 | Print bootstrap summary and instructions | Terminal output |

**`active-run.json` contents:**
```json
{
  "odf_version": "4.19",
  "workflow_id": "zstream-issue-verification",
  "run_id": "uuid-...",
  "dry_run": true,
  "coordinator_agent": "orchestrator-coordinator",
  "prompt_path": "workflow-zstream-issue-verification-prompt.md",
  "jira_status": "ON_QA",
  "jira_project": "DFBUGS"
}
```

If dry-run is enabled, a `.dry-run` marker file is created in the workspace.

---

### Step 2: Orchestrator Coordinator Starts

**Agent:** `orchestrator-coordinator` (Sonnet model)
**File:** `.claude/agents/orchestrator-coordinator.md`

The coordinator loads run context and drives all subsequent phases:

```bash
export JIRA_AGENT_WORKSPACE="${JIRA_AGENT_WORKSPACE:-$PWD/.claude/workspace}"
eval "$(.claude/framework/lib/load_run_context.sh)"
# Exports: ODF_VERSION, WORKFLOW_ID, RUN_ID, JIRA_STATUS, JIRA_PROJECT, DFBUGS_DRY_RUN
```

It then reads the workflow YAML to determine phase order and agent sequence.

**Key responsibilities:**
- Dispatch subagents in the order defined by the workflow YAML
- Track per-issue state via `run_state.py` (mark issues as processed)
- Enforce dry-run mode (no JIRA/GitHub writes)
- Enforce human escalation policy (confidence < 0.65 blocks transitions)
- Aggregate final reports

---

### Step 3: Preflight Phase

**Phase ID:** `preflight`

| Sub-step | Script/Hook | Purpose |
|----------|-------------|---------|
| 3.1 | `hooks/pre-execution/check_workspace.sh` | Verify workspace directory structure exists |
| 3.2 | `framework/orchestrator/setup_mcp.sh` | Ensure MCP servers are configured |
| 3.3 | `framework/orchestrator/preflight_mcp.sh` | Test MCP connectivity (fail fast if unavailable) |

If MCP is not available, the workflow stops immediately with an error. There is no REST API fallback.

---

### Step 4: Discovery Phase

**Phase ID:** `discovery`
**Agent:** `jira-discovery` (Haiku model)
**File:** `.claude/agents/jira-discovery.md`

| Sub-step | Action | Detail |
|----------|--------|--------|
| 4.1 | Load run context | Get `$ODF_VERSION`, `$JIRA_STATUS`, `$JIRA_PROJECT` |
| 4.2 | Build JQL query | `project = DFBUGS AND "Target Release" = odf-4.19.z AND status = "ON_QA" ORDER BY created DESC` |
| 4.3 | Execute search via MCP | Call `jira_search_jql` (redhat-jira MCP server) |
| 4.4 | Filter results | Keep only issues where Target Release exactly matches CLI version (drop mismatches) |
| 4.5 | Write output | `discovery/issues.json` |
| 4.6 | Log count | `jira-discovery: found N issue(s) via mcp` |

**Output — `discovery/issues.json`:**
```json
{
  "odf_version": "4.19",
  "status": "ON_QA",
  "issue_keys": ["DFBUGS-3742", "DFBUGS-3745", "DFBUGS-3801"],
  "discovery_method": "mcp",
  "target_release_filter": "odf-4.19.z",
  "excluded_mismatch_count": 2
}
```

**Custom field mapping** (from workflow YAML):
- Target Release: `customfield_10886`
- Prod Build Version: `customfield_10566`

This agent is **read-only** — it never modifies JIRA.

---

### Step 5: Per-Issue Pipeline

**Phase ID:** `per_issue`
**Loop:** `foreach: discovery/issues.json#issue_keys`

For each issue key (e.g., `DFBUGS-3742`), the coordinator runs the following agents in sequence. The coordinator checks `run-state.json` to skip already-processed issues (enables resume after interruption).

---

#### Step 5.1: JIRA Analysis

**Agent:** `jira-analysis` (Haiku model)
**File:** `.claude/agents/jira-analysis.md`

| Action | Detail |
|--------|--------|
| Fetch issue | Call `jira_issue_get` via MCP to get full issue data |
| Check skip label | If `skip-ocsci-agent` label is present → write `skipped_by_label` outcome, **stop pipeline for this issue** |
| Extract fields | Summary, description, components, linked PRs, attachments, comments |
| Assess feasibility | Determine if issue can be verified via automation |
| Output strategy | Write verification approach, confidence score, root cause analysis |

**Output — `artifacts/{KEY}/analysis.json`:**
```json
{
  "issue_key": "DFBUGS-3742",
  "summary": "...",
  "root_cause": "...",
  "verification_strategy": "...",
  "feasibility": "high",
  "confidence": 0.85,
  "components": ["Multi-Cloud Object Gateway"],
  "linked_prs": ["https://github.com/..."],
  "skip": false
}
```

**On skip:** If the issue has a blocked label, the pipeline writes the outcome and moves to the next issue (`on_skip: continue`).

---

#### Step 5.2: Cluster Compatibility Check

**Agent:** `cluster-compat` (Haiku model)

| Action | Detail |
|--------|--------|
| Load cluster config | Read `.claude/configs/clusters/default.yaml` |
| Check OCP/ODF versions | Run `oc` commands to verify cluster versions match the target |
| Build version gate | If JIRA specifies a Prod Build Version (`customfield_10566`), verify the cluster's ODF CSV >= that version |
| Capture baseline | Snapshot pre-run cluster state for drift detection |

**Output:**
- `artifacts/{KEY}/cluster-fit.json` — version compatibility result + baseline
- `artifacts/{KEY}/cluster-health/pre-snapshot.json` — pre-run health snapshot

**On incompatibility:** Writes `skipped_build_version_mismatch` outcome, pipeline continues to next issue (`on_skip: continue`).

---

#### Step 5.3: Reproduction Extraction

**Agent:** `repro-extraction` (Sonnet model)
**File:** `.claude/agents/repro-extraction.md`

| Action | Detail |
|--------|--------|
| Read JIRA issue | Description, comments, attachments |
| Read linked PRs | Fetch PR diffs, understand the fix |
| Search ocs-ci codebase | Find similar existing tests for context |
| AI-generate repro steps | Create structured reproduction plan |
| Assess confidence | Score based on completeness of information |

**Output — `artifacts/{KEY}/repro-steps.yaml`:**
```yaml
prerequisites:
  - OCP cluster with ODF 4.19 deployed
  - NooBaa component running
steps:
  - description: Access NooBaa pod
    command: oc rsh -n openshift-storage ...
  - description: Run md_blow script
    command: ...
verification_checks:
  - name: Script completes without NO_SUCH_KEY error
    assertion: exit_code == 0
pass_criteria: Script fills DB successfully with root keys loaded
confidence: 0.85
```

**Conditional branch:** If confidence < 0.5, the workflow branches to `jira_need_info`:
- **Live mode:** Labels the issue `Need Info`, transitions to `Assigned`
- **Dry-run mode:** Drafts the mutation to `planned-actions/jira.json`
- Pipeline stops for this issue (does not proceed to script generation)

---

#### Step 5.4: Script Generation

**Agent:** `script-generation` (Sonnet model)
**File:** `.claude/agents/script-generation.md`

Reads all prior artifacts (`analysis.json`, `cluster-fit.json`, `repro-steps.yaml`) and generates:

| Output File | Purpose |
|-------------|---------|
| `reproduce.py` | Pytest test file with real assertions (never `assert True`) |
| `verify.sh` | Bash wrapper that runs pytest on the cluster |
| `repro-steps.yaml` | Finalized QE plan (may refine from step 5.3) |
| `test-environment.yaml` | Required versions, namespaces, resources |
| `summary.md` | Human-readable summary of what the scripts do |

**Key constraints:**
- Scripts must include retries, cleanup, error handling
- No hardcoded cluster credentials or paths
- Must use structured logging per `.claude/skills/update-logging/SKILL.md`
- Must pass the safety validation hook (next step)

---

#### Step 5.5: Safety Validation Hook

**Hook:** `.claude/hooks/safety/validate_script.sh`
**Input:** `artifacts/{KEY}/verify.sh`

Scans the generated script against forbidden patterns defined in `.claude/configs/policies/safety.yaml`:

| Forbidden Pattern | Reason |
|-------------------|--------|
| `rm -rf /`, `rm -rf ~`, `rm -rf *` | Destructive deletion |
| `oc delete ns openshift-storage` | Destroys storage namespace |
| `ceph osd purge`, `ceph osd destroy` | Destroys Ceph storage |
| `mkfs.*`, `fdisk`, `parted` | Disk formatting |
| `dd if=` | Raw disk write |
| `chmod 777` | Insecure permissions |
| `curl \| sh`, `wget \| bash` | Pipe to shell |

**On violation:** Pipeline fails for this issue. The coordinator logs the error and moves on.

---

#### Step 5.6: Verification Execution

**Agent:** `verification-execution` (Sonnet model)
**File:** `.claude/agents/verification-execution.md`

| Action | Detail |
|--------|--------|
| Check prerequisites | Verify `KUBECONFIG` set, cluster reachable |
| Run test | `cd artifacts/{KEY} && pytest reproduce.py -v 2>&1 \| tee logs/pytest.log` |
| Collect evidence | Pod logs, `oc get events`, Ceph health, node status |
| Write result | `execution.json` |

**Output — `artifacts/{KEY}/execution.json`:**
```json
{
  "issue_key": "DFBUGS-3742",
  "passed": true,
  "duration_sec": 127,
  "failure_signature": "",
  "log_paths": ["logs/pytest.log"],
  "evidence_paths": ["evidence/ceph-health.txt", "evidence/pod-logs/"]
}
```

**Important:** This agent does NOT mark the issue as verified in JIRA. The cluster health scan must complete first.

---

#### Step 5.7: Cluster Health Collection Hook

**Hook:** `.claude/hooks/post-execution/cluster_health_collect.sh`
**Input:** `{issue_key}`

Collects post-execution cluster health data:
- Node status, pod status across storage namespaces
- Ceph health, OSD status, PG status
- Operator CSV status
- API server responsiveness

**Output:** `artifacts/{KEY}/cluster-health/post-snapshot.json`

---

#### Step 5.8: Cluster Health Detection

**Agent:** `cluster-health-detection` (Sonnet model)
**File:** `.claude/agents/cluster-health-detection.md`

**Always runs** — even if verification passed (catches silent regressions).

| Action | Detail |
|--------|--------|
| Compare snapshots | Diff pre-snapshot vs post-snapshot for drift |
| Scan for anomalies | CrashLoopBackOff, Pending, OOMKilled pods |
| Check storage health | Ceph, Rook, NooBaa, CSI driver status |
| Check operator health | ODF, NooBaa, Rook CSV status |
| Correlate known issues | Match against `.claude/configs/signatures/known-issues.yaml` |
| Detect new patterns | Flag potential bugs with confidence >= 0.7 |

**Output:**
- `artifacts/{KEY}/cluster-health-report.json` — machine-readable findings
- `artifacts/{KEY}/cluster-health/anomaly-report.md` — human-readable summary

**Critical finding:** If `cluster_health.status = CRITICAL` or `regression_detected: true`:
- The coordinator is blocked from transitioning the issue to `VERIFIED`
- Human review is required before any JIRA state change

---

#### Step 5.9: Infrastructure Diagnosis (Conditional)

**Agent:** `infra-diagnosis` (Sonnet model)
**Condition:** Runs only when `execution.failed OR cluster_health.degraded`

| Action | Detail |
|--------|--------|
| Classify failure root cause | `product_bug` \| `infra_instability` \| `cluster_misconfig` \| `environmental` |
| Provide evidence | Specific logs, events, metrics supporting classification |
| Set confidence | 0.0–1.0 for the classification |

**Output — `artifacts/{KEY}/diagnosis.json`:**
```json
{
  "classification": "infra_instability",
  "confidence": 0.82,
  "evidence": ["Node ip-10-0-1-5 NotReady during test window", "..."],
  "recommendation": "retry_later"
}
```

**If `infra_instability` with confidence >= 0.7:** Outcome is set to `blocked_by_infra` and the coordinator may retry the issue later.

---

#### Step 5.10: GitHub Automation (Conditional)

**Agent:** `github-automation` (Haiku model)
**File:** `.claude/agents/github-automation.md`
**Condition:** Runs when `automation_candidate` flag is set (issue is suitable for permanent test automation)

| Mode | Action |
|------|--------|
| **Live** | Search for duplicates in `red-hat-storage/ocs-ci`, create backlog issue if none found |
| **Dry-run** | Draft to `planned-actions/github-issue-draft.md`, no actual creation |

**Labels:** `automation backlog`, `QE`, `ODF`

---

#### Step 5.11: Reporting (Per-Issue)

**Agent:** `reporting` (Haiku model)
**File:** `.claude/agents/reporting.md`

Generates per-issue report appended to the appropriate file:

| Outcome | Report File |
|---------|-------------|
| Verified | `reports/verified.md` |
| Failed/Reproduced | `reports/failures.md` |
| Skipped | `reports/skipped.md` |

Each report includes:
- Issue summary and verification result
- Cluster health section (from `cluster-health-report.json`)
- Dry-run indicator (if applicable)
- Links to artifacts and evidence

---

#### Step 5.12: Memory Sync Hook

**Hook:** `.claude/hooks/post-execution/sync_memory.sh`
**Input:** `{issue_key}`

Persists findings (known issue signatures, cluster patterns) to local SQLite memory for future runs.

---

#### Step 5.13: State Tracking

After each issue completes, the coordinator marks it in `run-state.json`:

```python
python3 .claude/framework/lib/run_state.py mark \
  --workspace "$JIRA_AGENT_WORKSPACE" \
  --key "DFBUGS-3742" \
  --status "verified" \
  --confidence 0.85 \
  --processed
```

This enables:
- **Resume:** If the workflow is interrupted, already-processed issues are skipped
- **Retries:** Failed issues can be retried without re-running the entire pipeline

---

### Step 6: JIRA State Transition (Per-Issue)

After the per-issue pipeline completes and all checks pass, the coordinator transitions the JIRA issue:

| Verification Outcome | JIRA Action |
|----------------------|-------------|
| `verified` (confidence >= 0.65, no regressions) | Transition to `VERIFIED` |
| `reproduced` (bug still present) | Add label `FailedQA`, transition to `Assigned` |
| `insufficient_info` (confidence < 0.5) | Add label `Need Info`, transition to `Assigned` |
| `blocked_by_infra` | No transition — log for retry |
| `skipped_by_label` | No transition — already skipped |

**Guardrails:**
- **Dry-run:** All mutations are drafted to `planned-actions/jira.json`, nothing written to JIRA
- **Low confidence (< 0.65):** Human escalation — no JIRA transition
- **Critical health:** If `regression_detected: true`, VERIFIED transition is blocked
- **Destructive operation detected:** Human escalation triggered

---

### Step 7: Finalize Phase

**Phase ID:** `finalize`
**Agent:** `reporting` (Haiku model)

The coordinator aggregates all per-issue results into final reports:

| Output | Content |
|--------|---------|
| `reports/summary.md` | Executive summary: total issues, verified/failed/skipped counts, key findings |
| `reports/metrics.json` | Machine-readable aggregate metrics |
| `reports/report-odf-{VERSION}.json` | Version-specific JSON report |

---

## Workspace Directory Structure

```
$JIRA_AGENT_WORKSPACE/                     (default: .claude/workspace)
│
├── active-run.json                         # Run metadata (version, workflow, run ID)
├── run-config.json                         # Workflow configuration
├── run-state.json                          # Per-issue status tracking
├── .dry-run                                # Marker file (present in dry-run mode)
│
├── logs/
│   └── run.log                             # Central structured log
│
├── discovery/
│   └── issues.json                         # Discovered issue keys
│
├── artifacts/
│   └── {ISSUE_KEY}/                        # One directory per issue
│       ├── analysis.json                   # JIRA analysis output
│       ├── cluster-fit.json                # Cluster compatibility result
│       ├── repro-context.json              # Reproduction context sources
│       ├── repro-steps.yaml                # AI-generated QE plan
│       ├── reproduce.py                    # AI-generated pytest
│       ├── verify.sh                       # AI-generated execution wrapper
│       ├── test-environment.yaml           # Environment requirements
│       ├── verification-generation-prompt.md # Input to AI generation
│       ├── execution.json                  # Test execution result
│       ├── cluster-health-report.json      # Health analysis findings
│       ├── diagnosis.json                  # Failure classification (if failed)
│       ├── summary.md                      # Human-readable per-issue summary
│       │
│       ├── logs/
│       │   └── pytest.log                  # Test output
│       │
│       ├── evidence/                       # Collected cluster evidence
│       │   ├── ceph-health.txt
│       │   ├── pod-logs/
│       │   └── events.json
│       │
│       ├── cluster-health/
│       │   ├── pre-snapshot.json            # Before execution
│       │   ├── post-snapshot.json           # After execution
│       │   └── anomaly-report.md            # Human-readable findings
│       │
│       └── planned-actions/                # Dry-run only
│           ├── jira.json                   # Planned JIRA mutations
│           └── github-issue-draft.md       # Planned GitHub issue
│
├── outcomes/
│   └── {ISSUE_KEY}.json                    # Final outcome per issue
│
└── reports/
    ├── summary.md                          # Executive summary
    ├── verified.md                         # Verified issues list
    ├── failures.md                         # Failed issues list
    ├── skipped.md                          # Skipped issues list
    ├── metrics.json                        # Aggregate metrics
    └── report-odf-{VERSION}.json           # Version-specific report
```

---

## Dry-Run Mode

Activated via `--dry-run` flag or `DFBUGS_DRY_RUN=1` env var.

### What executes normally

- JIRA reads (discovery, issue fetch, search)
- GitHub searches (duplicate detection)
- Script generation and safety validation
- pytest execution on the cluster
- `oc` commands for health checks
- Cluster health scans and analysis
- All reporting and artifact generation
- SQLite memory persistence

### What is skipped (drafted instead)

| Blocked Action | Draft Location |
|----------------|----------------|
| `jira_comment_add` | `planned-actions/jira.json` |
| `jira_workflow_transition` | `planned-actions/jira.json` |
| `jira_label_add` | `planned-actions/jira.json` |
| `github_issue_create` | `planned-actions/github-issue-draft.md` |

All outcome JSONs include `"dry_run": true`.

---

## Safety & Escalation Policies

**Policy file:** `.claude/configs/policies/safety.yaml`

### Forbidden Patterns (in generated scripts)

| Pattern | Risk |
|---------|------|
| `rm -rf /` (or `~`, `$HOME`, `*`) | Destructive file deletion |
| `oc delete ns openshift-storage` | Destroys ODF namespace |
| `ceph osd purge/destroy` | Destroys Ceph storage |
| `mkfs.*`, `fdisk`, `parted` | Disk formatting |
| `dd if=` | Raw disk overwrite |
| `chmod 777` | Insecure permissions |
| `curl \| sh`, `wget \| bash` | Remote code execution |

### Human Escalation Triggers

The coordinator stops JIRA transitions when any of these conditions are met:

| Trigger | Threshold |
|---------|-----------|
| Low confidence | < 0.65 |
| Destructive operation in plan | Any |
| Ambiguous reproduction steps | Agent-assessed |
| Infrastructure failure unclear | Diagnosis indeterminate |
| JIRA transition blocked by API | Any |
| Critical cluster health | `regression_detected: true` |

---

## Agent Catalog

| Agent | Model | Role | Reusable |
|-------|-------|------|----------|
| `orchestrator-coordinator` | Sonnet | Main dispatcher, state tracking, report aggregation | All workflows |
| `jira-discovery` | Haiku | Find issues by version/status via MCP search | Z-stream specific |
| `jira-analysis` | Haiku | Fetch issue details, check labels, assess feasibility | Z-stream specific |
| `cluster-compat` | Haiku | Validate cluster versions, build version gate | All workflows |
| `repro-extraction` | Sonnet | AI-generate reproduction steps from JIRA + PRs | Z-stream specific |
| `script-generation` | Sonnet | Generate pytest, verify.sh, test environment | All workflows |
| `verification-execution` | Sonnet | Run scripts on cluster, collect evidence | All workflows |
| `cluster-health-detection` | Sonnet | Post-run cluster scan, regression detection | All workflows |
| `infra-diagnosis` | Sonnet | Classify failures: product vs infra | All workflows |
| `github-automation` | Haiku | Create automation backlog issues in ocs-ci | All workflows |
| `reporting` | Haiku | Per-issue and aggregate report generation | All workflows |
| `jira-verify-worker` | Sonnet | Single-issue shortcut (runs steps 1–9 in one agent) | Z-stream specific |

---

## MCP Server Requirements

| Server | Purpose | Tools Used |
|--------|---------|------------|
| `redhat-jira` | JIRA API access | `jira_search_jql`, `jira_issue_get`, `jira_comment_add`, `jira_workflow_transition`, `jira_workflow_get_transitions` |
| `GitHub` | GitHub API access | Issue search, issue creation |

Configured in Claude Code Settings → MCP. If unavailable, the workflow exits immediately during preflight.

---

## Running the Workflow

### First run (dry-run recommended)

```bash
# Option A: via slash command in Claude Code
/zstream-verify --dry-run 4.19

# Option B: bootstrap then run
.claude/framework/orchestrator/run.sh --dry-run 4.19
# then in Claude Code:
/zstream-verify --dry-run 4.19
```

### Live run (writes to JIRA and GitHub)

```bash
/zstream-verify 4.19
```

### Monitor progress

```bash
# Status dashboard
.claude/framework/orchestrator/status.sh

# Live log tail
.claude/framework/orchestrator/watch.sh

# Full detail (includes artifact logs)
.claude/framework/orchestrator/watch.sh --all

# Current run metadata
cat $JIRA_AGENT_WORKSPACE/active-run.json
```

### Resume after interruption

Re-run the same command. The coordinator reads `run-state.json` and skips already-processed issues.

---

## Conditional Branches & Edge Cases

| Condition | Behavior |
|-----------|----------|
| Issue has `skip-ocsci-agent` label | `jira-analysis` writes `skipped_by_label`, pipeline stops for this issue |
| Cluster ODF version mismatch | `cluster-compat` writes `skipped_build_version_mismatch`, pipeline stops |
| Repro confidence < 0.5 | Branch to `jira_need_info`: label `Need Info`, transition `Assigned`, pipeline stops |
| Safety hook violation | Pipeline fails for this issue, logged as error |
| Execution fails + infra diagnosed | Outcome `blocked_by_infra`, coordinator may retry |
| Cluster health CRITICAL | `VERIFIED` transition blocked, human review required |
| Confidence < 0.65 | Human escalation — no JIRA transition |
| 0 issues discovered | Coordinator logs warning, finalize phase still runs (empty report) |
