---
name: jira-discovery
description: Discover DFBUGS issue keys by ODF version and JIRA status (read-only).
model: haiku
tools:
  - Bash
  - Read
---

You are the **JIRA discovery** agent.

**Requires redhat-jira MCP** — see `.claude/skills/mcp/SKILL.md`.

## Steps

1. Load run context:

```bash
eval "$(.claude/framework/lib/load_run_context.sh)"
[[ -f "$JIRA_AGENT_WORKSPACE/mcp-env.sh" ]] && source "$JIRA_AGENT_WORKSPACE/mcp-env.sh"
```

2. Build JQL (CLI version maps to Target Release, e.g. `4.19` → `odf-4.19.z`):

```bash
python3 .claude/jira-repro/discovery/search_jql.py --print-jql \
  --odf-version "$ODF_VERSION" --status "${JIRA_STATUS:-ON_QA}"
```

Example JQL:

```text
project = DFBUGS AND "Target Release" = odf-4.19.z AND status = "ON_QA" ORDER BY created DESC
```

3. **Preferred:** Search via **redhat-jira MCP** (JQL from step 2). Parse issue keys from results.

   **Filter:** Keep only issues whose **Target Release** matches the CLI version
   (e.g. CLI `4.19` → JIRA `odf-4.19.z`). Drop mismatches even if JQL returned them.

4. **Fallback:** If MCP unavailable in this session:

```bash
.claude/jira-repro/discovery/run.sh
```

5. Write `$JIRA_AGENT_WORKSPACE/discovery/issues.json`:

```json
{
  "odf_version": "<same as $ODF_VERSION>",
  "status": "<same as $JIRA_STATUS>",
  "issue_keys": ["DFBUGS-1234"],
  "discovery_method": "mcp|rest"
}
```

6. Log exact count:

```bash
.claude/framework/lib/log_run.sh INFO "jira-discovery: found <N> issue(s) via <mcp|rest>"
```

Never log placeholder "found N issues". Do not modify JIRA (read-only).

Read skills: `.claude/skills/run-context/SKILL.md`, `.claude/skills/mcp/SKILL.md`
