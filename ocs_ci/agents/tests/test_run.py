"""Jenkins entrypoint for ODF agents."""

import json
import os
import sys
from pathlib import Path

import pytest

from ocs_ci.agents.mcp import registry
from ocs_ci.agents.mcp.registry import servers_for
from ocs_ci.agents.runtime import llm, run
from ocs_ci.agents.runtime.tool_calls import (
    limit_tool_calls_for_model,
    split_tool_call_messages,
)


def test_build_request_reads_jenkins_environment():
    """Job parameters and build metadata become the agent request."""
    request = run.build_request(
        [],
        {
            "OCS_AGENT_NAME": "jira_verification",
            "OCS_AGENT_MESSAGE": "DFBUGS-1",
            "JOB_NAME": "odf-agent",
            "BUILD_NUMBER": "15",
            "BUILD_URL": "https://jenkins.example/job/odf-agent/15/",
            "WORKSPACE": "/var/lib/jenkins/workspace/odf-agent",
        },
    )
    assert request["agent"] == "jira_verification"
    assert request["message"] == "DFBUGS-1"
    assert (
        request["result_file"]
        == "/var/lib/jenkins/workspace/odf-agent/agent-result.json"
    )
    assert request["jenkins"]["job_name"] == "odf-agent"
    assert request["jenkins"]["build_number"] == "15"
    assert (
        request["jenkins"]["build_url"] == "https://jenkins.example/job/odf-agent/15/"
    )


def test_cli_overrides_jenkins_parameters():
    """Explicit flags win over the job environment."""
    request = run.build_request(
        [
            "--agent",
            "jira_verification",
            "--message",
            "from-cli",
            "--result-file",
            "out.json",
        ],
        {
            "OCS_AGENT_NAME": "other",
            "OCS_AGENT_MESSAGE": "from-env",
            "WORKSPACE": "/workspace",
        },
    )
    assert request["agent"] == "jira_verification"
    assert request["message"] == "from-cli"
    assert request["result_file"] == "out.json"


def test_issue_cluster_and_extra_args_reach_the_agent():
    """Issue keys, the kube context, and other KEY=VALUE pairs are named arguments."""
    request = run.build_request(
        [
            "--agent",
            "jira_verification",
            "--message",
            "odf-5.0",
            "--issue",
            "DFBUGS-487",
            "--issue",
            "DFBUGS-376",
            "--cluster",
            "amagrawa-c1",
            "--arg",
            "project=DFBUGS",
        ],
        {
            "OCS_AGENT_ISSUE": "DFBUGS-1",
            "OCS_AGENT_CLUSTER": "from-env",
            "OCS_AGENT_ARGS": "project=STOR note=keep",
        },
    )
    assert request["args"] == {
        "issues": ["DFBUGS-487", "DFBUGS-376"],
        "cluster": "amagrawa-c1",
        "project": "DFBUGS",
        "note": "keep",
    }


def test_release_selects_the_target_release():
    """--release names the Target Release, and it replaces OCS_AGENT_RELEASE."""
    request = run.build_request(
        ["--agent", "jira_verification", "--release", "odf-5.0"],
        {"OCS_AGENT_RELEASE": "odf-4.16", "OCS_AGENT_ARGS": "release=odf-4.15"},
    )
    assert request["args"]["release"] == "odf-5.0"
    state = run.agent_state(request)
    assert '"release": "odf-5.0"' in state["messages"][0]["content"]


def test_dry_run_reaches_the_agent():
    """--dry-run tells the agent not to update Jira or any other application."""
    request = run.build_request(
        ["--agent", "jira_verification", "--release", "odf-5.0", "--dry-run"],
        {},
    )
    assert request["args"]["dry_run"] is True
    state = run.agent_state(request)
    assert '"dry_run": true' in state["messages"][0]["content"]


def test_dry_run_comes_from_the_environment():
    """OCS_AGENT_DRY_RUN=1 is the same switch as --dry-run."""
    request = run.build_request(
        ["--agent", "jira_verification"],
        {"OCS_AGENT_DRY_RUN": "1"},
    )
    assert request["args"]["dry_run"] is True


def test_oversized_tool_call_message_is_split_for_openai():
    """An assistant message with more than 128 tool calls is split before the API call."""
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

    calls = [
        {
            "id": f"call_{index}",
            "name": "jira_get_issue",
            "args": {"issue_key": f"DFBUGS-{index}"},
            "type": "tool_call",
        }
        for index in range(166)
    ]
    history = [
        HumanMessage(content="odf-5.0"),
        AIMessage(content="Fetching issues.", tool_calls=calls),
        *[
            ToolMessage(content="{}", tool_call_id=f"call_{index}")
            for index in range(166)
        ],
    ]
    prepared = limit_tool_calls_for_model({"messages": history})["llm_input_messages"]
    assistant_messages = [
        message for message in prepared if isinstance(message, AIMessage)
    ]
    assert [len(message.tool_calls) for message in assistant_messages] == [128, 38]
    assert assistant_messages[0].content == "Fetching issues."
    assert assistant_messages[1].content == ""
    seen = [
        message.tool_call_id for message in prepared if isinstance(message, ToolMessage)
    ]
    assert seen == [f"call_{index}" for index in range(166)]
    assert split_tool_call_messages(history[:1]) == history[:1]


def test_dry_run_is_omitted_unless_requested():
    """A normal run does not claim to be a dry run."""
    request = run.build_request(["--agent", "jira_verification"], {})
    assert "dry_run" not in request["args"]


def test_invoke_exports_dry_run_and_restores_the_environment(monkeypatch):
    """Tool processes started during the run inherit OCS_AGENT_DRY_RUN."""
    monkeypatch.delenv("OCS_AGENT_DRY_RUN", raising=False)

    def load_make_graph(agent_dir):
        async def make_graph():
            raise RuntimeError(os.environ.get("OCS_AGENT_DRY_RUN") or "")

        return make_graph

    monkeypatch.setattr(run, "load_make_graph", load_make_graph)
    with pytest.raises(RuntimeError, match="^1$"):
        run.invoke_agent(
            Path("."),
            {"agent": "jira_verification", "args": {"dry_run": True}, "jenkins": {}},
        )
    assert "OCS_AGENT_DRY_RUN" not in os.environ


def test_environment_supplies_args_when_the_cli_omits_them():
    """Jenkins can pass the issue list and cluster without CLI flags."""
    request = run.build_request(
        ["--agent", "jira_verification", "--message", "odf-5.0"],
        {"OCS_AGENT_ISSUE": "DFBUGS-487, DFBUGS-378", "OCS_AGENT_CLUSTER": "hub"},
    )
    assert request["args"]["issues"] == ["DFBUGS-487", "DFBUGS-378"]
    assert request["args"]["cluster"] == "hub"


def test_malformed_arg_is_a_usage_error():
    """A pair without an equals sign exits 2."""
    code = run.main(
        ["--agent", "jira_verification", "--message", "odf-5.0", "--arg", "cluster"],
        {},
    )
    assert code == run.EXIT_USAGE


def test_agent_state_appends_args_for_the_model():
    """The graph message ends with the argument JSON, and state keeps the mapping."""
    state = run.agent_state(
        {
            "message": "odf-5.0",
            "args": {"cluster": "hub", "issues": ["DFBUGS-487"]},
            "jenkins": {},
        }
    )
    assert state["messages"][0]["content"] == (
        'odf-5.0\n{"cluster": "hub", "issues": ["DFBUGS-487"]}'
    )
    assert state["args"]["issues"] == ["DFBUGS-487"]


def test_missing_agent_name_is_a_usage_error():
    """A job that sets neither --agent nor OCS_AGENT_NAME exits 2."""
    assert run.main([], {}) == run.EXIT_USAGE


def test_unknown_agent_exits_usage():
    """An agent name that is not a folder exits 2 and does not run a graph."""
    called = False

    def invoke(agent_dir, request):
        nonlocal called
        called = True
        return {"ok": True}

    code = run.main(["--agent", "not_an_agent"], {}, invoke=invoke)
    assert code == run.EXIT_USAGE
    assert called is False


def test_successful_run_archives_result_in_the_workspace(tmp_path, capsys):
    """A successful agent writes agent-result.json where Jenkins can archive it."""

    def invoke(agent_dir, request):
        assert request["jenkins"]["job_name"] == "odf-agent"
        assert request["message"] == "DFBUGS-1"
        return {"ok": True, "agent": request["agent"]}

    code = run.main(
        ["--message", "DFBUGS-1"],
        {
            "OCS_AGENT_NAME": "jira_verification",
            "WORKSPACE": str(tmp_path),
            "JOB_NAME": "odf-agent",
            "BUILD_NUMBER": "7",
            "BUILD_URL": "https://jenkins.example/7",
        },
        invoke=invoke,
    )
    assert code == run.EXIT_OK
    archived = json.loads((tmp_path / "agent-result.json").read_text(encoding="utf-8"))
    assert archived["ok"] is True
    assert archived["agent"] == "jira_verification"
    console = json.loads(capsys.readouterr().out)
    assert console == archived


def test_invoke_agent_passes_the_jenkins_build(monkeypatch):
    """The compiled graph receives the user message and the Jenkins build fields."""

    class Graph:
        def __init__(self):
            self.state = None

        async def ainvoke(self, state, config=None):
            self.state = state
            self.config = config
            return {"verdict": "covered", "messages": []}

    graph = Graph()

    async def make_graph():
        return graph

    monkeypatch.setattr(run, "load_make_graph", lambda agent_dir: make_graph)
    request = {
        "agent": "jira_verification",
        "message": "DFBUGS-1",
        "jenkins": {"job_name": "odf-agent", "build_number": "7"},
    }
    result = run.invoke_agent(Path("jira_verification"), request)
    assert result["ok"] is True
    assert result["reply"] == ""
    assert graph.config["recursion_limit"] == 1000
    assert graph.state["messages"][0]["content"] == "DFBUGS-1"
    assert graph.state["jenkins"]["build_number"] == "7"


def test_rovo_is_the_atlassian_http_server(monkeypatch):
    """Jenkins authenticates to Rovo with a header and does not start a local process."""
    monkeypatch.setenv("ATLASSIAN_ROVO_MCP_AUTHORIZATION", "Bearer test-key")
    server = servers_for(["rovo"])["rovo"]
    assert server["url"] == "https://mcp.atlassian.com/v2/mcp"
    assert server["transport"] == "http"
    assert server["headers"]["Authorization"] == "Bearer test-key"
    assert "command" not in server


def test_rovo_omits_the_authorization_header_when_unset(monkeypatch):
    """OAuth clients connect when neither the environment nor auth.yaml has a token."""
    monkeypatch.delenv("ATLASSIAN_ROVO_MCP_AUTHORIZATION", raising=False)
    monkeypatch.setattr(registry, "_load_auth_config", lambda: {})
    server = servers_for(["rovo"])["rovo"]
    assert "headers" not in server


def test_rovo_token_comes_from_auth_yaml(monkeypatch):
    """agents_credentials.rovo_mcp.token in data/auth.yaml becomes a Bearer header."""
    monkeypatch.delenv("ATLASSIAN_ROVO_MCP_AUTHORIZATION", raising=False)
    monkeypatch.setattr(
        registry,
        "_load_auth_config",
        lambda: {"agents_credentials": {"rovo_mcp": {"token": "test-token"}}},
    )
    server = servers_for(["rovo"])["rovo"]
    assert server["headers"]["Authorization"] == "Bearer test-token"


def test_mcp_servers_use_the_job_interpreter():
    """MCP tools start with the same Python the Jenkins job used."""
    servers = servers_for(["jira", "cluster", "reportportal"])
    for server in servers.values():
        assert server["command"] == sys.executable
        assert server["transport"] == "stdio"
        assert server["args"][0] == "-m"


@pytest.mark.parametrize(
    "module",
    [
        "ocs_ci.agents.mcp.servers.jira",
        "ocs_ci.agents.mcp.servers.cluster",
        "ocs_ci.agents.mcp.servers.reportportal",
    ],
)
def test_mcp_server_modules_match_the_registry(module):
    """Each registered server is started as python -m that module."""
    name = module.rsplit(".", 1)[-1]
    assert servers_for([name])[name]["args"] == ["-m", module]


def test_openai_key_prefers_auth_yaml(monkeypatch):
    """agents_credentials.openai.api_key wins over a shell OPENAI_API_KEY."""
    monkeypatch.setattr(
        llm,
        "_load_auth_config",
        lambda: {"agents_credentials": {"openai": {"api_key": "from-file"}}},
    )
    monkeypatch.setenv("OPENAI_API_KEY", "from-env")
    assert llm._openai_api_key() == "from-file"


def test_openai_key_falls_back_to_the_environment(monkeypatch):
    """OPENAI_API_KEY is used when auth.yaml has no OpenAI key."""
    monkeypatch.setattr(llm, "_load_auth_config", lambda: {})
    monkeypatch.setenv("OPENAI_API_KEY", "from-env")
    assert llm._openai_api_key() == "from-env"


def test_openai_model_receives_the_auth_yaml_key(monkeypatch):
    """ChatOpenAI is constructed with the resolved key, not the process environment."""
    pytest.importorskip("langchain_openai")
    monkeypatch.setattr(llm, "_openai_api_key", lambda: "from-file")
    monkeypatch.delenv("OCS_AGENT_MODEL", raising=False)
    created = {}

    class FakeChat:
        def __init__(self, model, api_key, temperature):
            created["model"] = model
            created["api_key"] = api_key
            created["temperature"] = temperature

    import langchain_openai

    monkeypatch.setattr(langchain_openai, "ChatOpenAI", FakeChat)
    llm._openai_model()
    assert created == {"model": "gpt-4o", "api_key": "from-file", "temperature": 0}
