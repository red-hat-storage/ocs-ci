# ODF agents

Agents in this package use LangGraph to run, LangChain to call the model, and
MCP servers for tools. Each agent is its own folder. A parent workflow routes
between agents only when one agent must call another.

```text
ocs_ci/agents/
├── langgraph.json          # generated registry of agent graphs
├── runtime/                # shared graph builder, model, discovery, Jenkins runner
├── mcp/
│   ├── registry.py         # local servers and remote Rovo
│   ├── client.py           # loads MCP servers as LangChain tools
│   └── servers/            # local tool implementations
│       ├── jira.py
│       ├── cluster.py
│       └── reportportal.py
├── _template/              # copy this folder to add an agent
├── jira_verification/      # first agent
├── workflows/              # parent graphs that call other agents
└── tests/
```

Tools stay in `mcp/servers/`. An agent lists the tool names it may call in
`agent.yaml`. Cursor uses the same servers through `.cursor/mcp.json`.

## Add an agent

1. Copy `_template/` to `<agent_name>/`.
2. Edit `agent.yaml` and `prompt.md`. Set `name` to the directory name.
3. Set `mcp_servers` and `tools.allow` to local servers under `mcp/servers/`,
   or to `rovo` and the Rovo tool names that agent may call.
4. Leave `graph.py` as it is. It calls the shared LangGraph builder.
5. Regenerate the registry:

```bash
python3 -m ocs_ci.agents.runtime.discover
```

## Run from Jenkins

A Jenkins job checks out this repo and runs one agent per build. The job passes
the agent name and the message as parameters. The process exit code is the
build result: 0 when the agent succeeds, 1 when the agent fails, and 2 when
the agent name is missing or unknown.

The last line of the console log is a JSON result. When WORKSPACE is set, the
same JSON is written to `agent-result.json` in the workspace so the job can
archive it.

Install the agent runtime in the job environment before the first run. The
packages are the `agents` dependency group in `pyproject.toml`:

```bash
uv pip install --python python3 \
  "langgraph>=1.0" "langchain>=1.0" \
  "langchain-openai>=1.0" "langchain-mcp-adapters>=0.1"
```

The chat model is OpenAI. Put the key in `data/auth.yaml`:

```yaml
agents_credentials:
  openai:
    api_key: <openai-api-key>
```

`OPENAI_API_KEY` is used when that file entry is empty. `OCS_AGENT_MODEL`
overrides the model name. The default is `gpt-4o`. Set
`OCS_AGENT_PROVIDER=claude` to use a logged-in Claude CLI instead. Jira
credentials come from `config.AUTH.jira`, from the `jira` section of
`data/auth.yaml` (`url`, `email`, `token`), or from `/etc/jira.cfg`.

```bash
python3 -m ocs_ci.agents.runtime.run \
  --agent "${OCS_AGENT_NAME}" \
  --message "${OCS_AGENT_MESSAGE}"
```

The same command with no flags reads `OCS_AGENT_NAME` and `OCS_AGENT_MESSAGE`
from the environment. `JOB_NAME`, `BUILD_NUMBER`, `BUILD_URL`, and `WORKSPACE`
are passed into the agent. Local MCP servers start with the same Python as
the job and inherit the job environment, including the ocs-ci config,
kubeconfig, and credentials the job already injects.

`.cursor/mcp.json` is for the editor. The Jenkins job does not read it.

## Atlassian Rovo

Rovo is Atlassian's remote MCP server. It is not a module under `mcp/servers/`.
An agent uses it by adding `rovo` to `mcp_servers` and naming the Rovo tools it
may call in `tools.allow`.

The Jenkins job reads the token from `data/auth.yaml`:

```yaml
agents_credentials:
  rovo_mcp:
    token: <atlassian-api-token>
```

That token is sent as a Bearer token. `ATLASSIAN_ROVO_MCP_AUTHORIZATION`
overrides the file when it holds a full Authorization header value. Cursor still
signs in with OAuth through `.cursor/mcp.json`.

## Add a tool

Add the tool function to a server module under `mcp/servers/`, and add its name
to that module's `TOOL_NAMES`. A new capability is a new server module plus a
line in `mcp/registry.py`. Agents then name that server in `mcp_servers`.

Server modules call the helpers that already exist in `ocs_ci.utility` and
`ocs_ci.ocs`. The cluster server stays read-only.

## Multi-agent workflows

`workflows/` holds a parent LangGraph that compiles other agents and routes
work between them. With no module there, each agent runs on its own.

## Tests

```bash
python -m pytest -c pytest_unittests.ini ocs_ci/agents/tests/test_agents_load.py
```

The test checks that every agent folder has `agent.yaml`, `prompt.md`, and
`graph.py`, that its tools belong to the servers it names, and that
`langgraph.json` matches the discovered agents.
