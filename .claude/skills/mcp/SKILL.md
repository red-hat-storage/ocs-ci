---
name: mcp-servers
description: Required MCP servers (redhat-jira, GitHub) before DFBUGS workflow execution
---

# MCP servers (required before workflow)

## 1. Install in Claude Code

Merge into your Claude Code MCP config (Settings → MCP, or `~/.claude.json`):

`.claude/configs/mcp/claude-code-mcp.example.json`

```json
"redhat-jira": {
  "command": "uvx",
  "args": ["atlassian-jira-mcp"],
  "env": {
    "JIRA_MCP_URL": "https://redhat.atlassian.net",
    "JIRA_MCP_TOKEN": "<your-atlassian-api-token>",
    "JIRA_MCP_EMAIL": "you@redhat.com"
  }
}
```

Also enable **GitHub** MCP for `github-automation` agent.

## 2. Export token for terminal / discover.sh

Claude MCP env does not automatically apply to your shell:

```bash
export JIRA_MCP_URL=https://redhat.atlassian.net
export JIRA_MCP_EMAIL=you@redhat.com
export JIRA_MCP_TOKEN="$TOKEN"    # or export TOKEN=...

.claude/framework/orchestrator/run.sh --workflow zstream-issue-verification 4.20 --dry-run
```

`run.sh` runs `setup_mcp.sh` → writes `$JIRA_AGENT_WORKSPACE/mcp-env.sh`.

## 3. Discovery paths

| Context | How to discover issues |
|---------|-------------------------|
| Claude Code coordinator | MCP `jira_search` / JQL on **redhat-jira** → write `discovery/issues.json` |
| Terminal `discover.sh` | REST via `search_jql.py` using synced `JIRA_URL` / token |

Build JQL:

```bash
source .claude/workspace/mcp-env.sh
python3 .claude/jira-repro/discovery/search_jql.py --print-jql --odf-version "$ODF_VERSION"
```

## 4. Preflight

```bash
.claude/framework/orchestrator/setup_mcp.sh
.claude/framework/orchestrator/preflight_mcp.sh
```

Fails fast if `JIRA_MCP_TOKEN` / `JIRA_MCP_EMAIL` are missing.

## 5. Agent tool names (redhat-jira)

Use MCP tools exposed by **redhat-jira** (prefix may appear as `mcp__redhat-jira__*`):

- Read: `jira_issue_get`, JQL search tool
- Write (live only): `jira_comment_add`, `jira_workflow_transition`

Catalog: `.claude/configs/mcp/required.yaml`
