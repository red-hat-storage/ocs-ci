"""Jenkins entrypoint for ODF agents."""

import json
import sys
from pathlib import Path

import pytest

from ocs_ci.agents.mcp import registry
from ocs_ci.agents.mcp.registry import servers_for
from ocs_ci.agents.runtime import llm, run


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
