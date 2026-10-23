---
name: ocs-ci-agents
description: Add or update an ODF agent in ocs-ci
authoritative_source: ocs_ci/agents/README.md
---

# ODF agents

Adds or updates an agent under `ocs_ci/agents`.

**Authoritative source**: `ocs_ci/agents/README.md` describes the layout, how to
copy `_template`, and where MCP tools live. Follow that document.

## What this skill does

1. Reads `ocs_ci/agents/README.md`.
2. Copies `ocs_ci/agents/_template/` to `ocs_ci/agents/<agent_name>/` when the
   user asks for a new agent.
3. Edits only that folder's `agent.yaml` and `prompt.md`.
4. Leaves `graph.py` calling `make_agent_graph`.
5. Adds a tool on an existing MCP server, or adds a server module, only when
   the agent needs a tool that is not already registered.
6. Runs `python -m ocs_ci.agents.runtime.discover` so `langgraph.json` matches
   the agent folders.
