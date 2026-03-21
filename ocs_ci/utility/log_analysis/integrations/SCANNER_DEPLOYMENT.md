# Scanner Agent Deployment Guide

## Prerequisites
- RHEL/CentOS agent with Python 3.11+
- NFS mount at `/mnt/ocsci-jenkins/`
- Claude CLI installed at `/home/jenkins/.local/bin/claude`
- GCP service account credentials at `/opt/claude/auth/gcp-auth.json`

## 1. Clone ocs-ci

```bash
mkdir -p /home/jenkins/ocs-ci-analysis
cd /home/jenkins/ocs-ci-analysis
git clone --branch log_anal --single-branch https://github.com/shyRozen/ocs-ci.git
```

## 2. Create virtualenv (Python 3.11)

```bash
python3.11 -m venv /home/jenkins/ocs-ci-analysis/venv
/home/jenkins/ocs-ci-analysis/venv/bin/pip install --upgrade pip
```

## 3. Install ocs-ci (without full deps)

The full `pip install -e .` fails due to `ocp-network-split` requiring a specific version.
Install without deps, then add only what log_analysis needs:

```bash
cd /home/jenkins/ocs-ci-analysis/ocs-ci
/home/jenkins/ocs-ci-analysis/venv/bin/pip install --no-deps -e .
/home/jenkins/ocs-ci-analysis/venv/bin/pip install \
    beautifulsoup4 jinja2 requests pyyaml urllib3 \
    atlassian-python-api anthropic
```

## 4. Verify Claude CLI works

```bash
GOOGLE_APPLICATION_CREDENTIALS=/opt/claude/auth/gcp-auth.json \
CLAUDE_CODE_USE_VERTEX=1 \
ANTHROPIC_VERTEX_PROJECT_ID=itpc-gcp-core-pe-eng-claude \
CLOUD_ML_REGION=us-east5 \
claude -p 'say hello' --max-turns 1
```

## 5. Create shared directories

```bash
mkdir -p /mnt/ocsci-jenkins/log_analysis/session_manage
mkdir -p /mnt/ocsci-jenkins/log_analysis/history_dir
mkdir -p /mnt/ocsci-jenkins/log_analysis/cache_dir
mkdir -p /mnt/ocsci-jenkins/log_analysis/sessions_dir
```

## 6. Verify scanner works (dry-run)

```bash
cd /home/jenkins/ocs-ci-analysis/ocs-ci
CLAUDE_CODE_USE_VERTEX=1 \
ANTHROPIC_VERTEX_PROJECT_ID=itpc-gcp-core-pe-eng-claude \
CLOUD_ML_REGION=us-east5 \
GOOGLE_APPLICATION_CREDENTIALS=/opt/claude/auth/gcp-auth.json \
/home/jenkins/ocs-ci-analysis/venv/bin/python \
    -m ocs_ci.utility.log_analysis.integrations.scanner \
    --ocs-ci-path /home/jenkins/ocs-ci-analysis/ocs-ci \
    --no-git-pull --dry-run -v
```

## 7. Install crontab

```bash
cat <<'EOF' | crontab -
CLAUDE_CODE_USE_VERTEX=1
ANTHROPIC_VERTEX_PROJECT_ID=itpc-gcp-core-pe-eng-claude
CLOUD_ML_REGION=us-east5
GOOGLE_APPLICATION_CREDENTIALS=/opt/claude/auth/gcp-auth.json
PATH=/home/jenkins/.local/bin:/usr/local/bin:/usr/bin:/bin

*/5 * * * * cd /home/jenkins/ocs-ci-analysis/ocs-ci && /home/jenkins/ocs-ci-analysis/venv/bin/python -m ocs_ci.utility.log_analysis.integrations.scanner --ocs-ci-path /home/jenkins/ocs-ci-analysis/ocs-ci --no-git-pull >> /mnt/ocsci-jenkins/log_analysis/session_manage/scanner.log 2>&1
EOF
```

## Key paths

| What | Path |
|------|------|
| ocs-ci clone | `/home/jenkins/ocs-ci-analysis/ocs-ci` |
| virtualenv | `/home/jenkins/ocs-ci-analysis/venv` |
| Claude CLI | `/home/jenkins/.local/bin/claude` |
| GCP credentials | `/opt/claude/auth/gcp-auth.json` |
| Scanner log | `/mnt/ocsci-jenkins/log_analysis/session_manage/scanner.log` |
| State file | `/mnt/ocsci-jenkins/log_analysis/session_manage/scanner_state.json` |
| Lock file | `/mnt/ocsci-jenkins/log_analysis/session_manage/scanner.lock` |
| Cache dirs | `/mnt/ocsci-jenkins/log_analysis/cache_dir/{version}_cache_dir/` |
| History dirs | `/mnt/ocsci-jenkins/log_analysis/history_dir/{version}_history_dir/` |
| Sessions dirs | `/mnt/ocsci-jenkins/log_analysis/sessions_dir/{version}_sessions_dir/` |
| Scan target | `/mnt/ocsci-jenkins/openshift-clusters/j*/` |

## Crontab env vars (critical)

These must be in the crontab because cron has a minimal environment:

- `CLAUDE_CODE_USE_VERTEX=1` - tells Claude CLI to use Vertex AI
- `ANTHROPIC_VERTEX_PROJECT_ID=itpc-gcp-core-pe-eng-claude` - GCP project
- `CLOUD_ML_REGION=us-east5` - Vertex region
- `GOOGLE_APPLICATION_CREDENTIALS=/opt/claude/auth/gcp-auth.json` - GCP auth
- `PATH` must include `/home/jenkins/.local/bin` for the `claude` CLI

## Scanner defaults

- `--max-age-days 7` - only discover runs from last 7 days (once discovered, they stay in the pending queue regardless of age)
- `--max-runs-per-cycle 5` - process up to 5 runs per cron invocation
- `--parallel 3` - run up to 3 analyses concurrently
- `--max-budget 2.0` - $2.00 max per analysis run
- `--max-failures 70` - analyze up to 70 failures per run
- Lock file prevents concurrent scanner instances

## Per-XML tracking

The scanner tracks each JUnit XML file individually. This supports upgrade runs that produce
multiple `test_results_*.xml` files (e.g., tier1 on 4.16, then upgrade and tier1 on 4.17).

- Each XML with failures becomes a separate pending/processed entry
- State file keys are full XML paths (e.g., `.../logs/test_results_1774030457.xml`)
- Output files are suffixed with the XML identifier:
  - `ai_analysis_report_1774030457.html` (report)
  - `ai_analysis_1774030457.log` (analysis log)
- The CLI receives `--junit-xml <path>` to analyze a specific XML
- ODF version is detected per-XML from `rp_ocs_build`, so upgrade runs
  use the correct version-specific cache/history for each phase

**State migration**: old state files with `logs_dir` keys are automatically
migrated to `xml_path` keys on first load.

## Pending queue

Entries go through three states: **discovered → pending → processed**.

- **Discovery**: directory scan finds new XML files with failures (age filter applies only here)
- **Pending**: saved in the `"pending"` list in the state file — survives across scanner cycles, cannot age out
- **Processed**: moved to `"processed"` dict after analysis completes (status: `done` or `failed`)

Each cycle: discover new XMLs → merge into pending → sort oldest-first → process up to `--max-runs-per-cycle` from the front → move completed to processed.

This prevents runs from being missed when the backlog takes multiple cycles to drain. Pending entries whose XML files no longer exist on disk are automatically dropped.

Per-XML analysis logs are written to `<logs_dir>/ai_analysis_<suffix>.log` for live tailing.

## Troubleshooting

### "AI backend 'claude-code' is not available"
Missing `claude` in PATH or missing Vertex env vars. Check crontab.

### Fast analysis with "unknown" results
Cache poisoned with no-AI entries. Clean with:
```bash
python3 -c "
import json, os, glob
for f in glob.glob('/mnt/ocsci-jenkins/log_analysis/cache_dir/*_cache_dir/*.json'):
    with open(f) as fh:
        d = json.load(fh)
    if d.get('analysis', {}).get('category') == 'unknown' and not d.get('analysis', {}).get('root_cause_summary'):
        os.remove(f)
        print(f'Removed {f}')
"
```
Then clear state: `echo '{"processed": {}, "pending": []}' > /mnt/ocsci-jenkins/log_analysis/session_manage/scanner_state.json`

### "Could not load the default credentials"
`GOOGLE_APPLICATION_CREDENTIALS` not set or file missing. Verify `/opt/claude/auth/gcp-auth.json` exists.

### Re-analyze specific runs
Remove the XML path from `"processed"` in the state file. It will be rediscovered (if within `--max-age-days`) and added to the pending queue.
To re-analyze all: `echo '{"processed": {}, "pending": []}' > /mnt/ocsci-jenkins/log_analysis/session_manage/scanner_state.json`

### Check scanner status
```bash
# Follow scanner log
tail -f /mnt/ocsci-jenkins/log_analysis/session_manage/scanner.log

# Follow a specific run's analysis log (suffix matches the XML timestamp)
tail -f /mnt/ocsci-jenkins/openshift-clusters/j-xxx/j-xxx_TIMESTAMP/logs/ai_analysis_NNNNNNN.log

# Summary of state
python3 -c '
import json, sys
with open("/mnt/ocsci-jenkins/log_analysis/session_manage/scanner_state.json") as f:
    d = json.load(f)
p = d.get("processed", {})
done = sum(1 for v in p.values() if v.get("status") == "done")
failed = sum(1 for v in p.values() if v.get("status") == "failed")
pending = d.get("pending", [])
print(f"Processed: {len(p)} (done={done}, failed={failed})")
print(f"Pending: {len(pending)}")
if pending:
    print(f"  Oldest: {pending[0][\"logs_dir\"]}")
    print(f"  Newest: {pending[-1][\"logs_dir\"]}")
'
```
