"""Connection settings for local stdio MCP servers and remote Rovo."""

import os
import sys

import yaml

from ocs_ci.agents.mcp.servers import SERVER_TOOLS
from ocs_ci.ocs.constants import AUTHYAML, DATA_DIR

ROVO_MCP_URL = "https://mcp.atlassian.com/v2/mcp"
ROVO_AUTHORIZATION_ENV = "ATLASSIAN_ROVO_MCP_AUTHORIZATION"


def _stdio_server(module):
    """
    Build a stdio MCP connection for a server module.

    Args:
        module (str): Import path started with python -m.

    Returns:
        dict: MultiServerMCPClient connection config. The command is the
            Python running the Jenkins job, so the tool server matches that
            interpreter.
    """
    return {
        "command": sys.executable,
        "args": ["-m", module],
        "transport": "stdio",
    }


LOCAL_SERVERS = {
    "jira": "ocs_ci.agents.mcp.servers.jira",
    "cluster": "ocs_ci.agents.mcp.servers.cluster",
    "reportportal": "ocs_ci.agents.mcp.servers.reportportal",
    "jenkins": "ocs_ci.agents.mcp.servers.jenkins",
}


def _load_auth_config():
    """
    Load data/auth.yaml.

    Returns:
        dict: Parsed auth config, or an empty dict when the file is missing.
    """
    auth_file = os.path.join(DATA_DIR, AUTHYAML)
    try:
        with open(auth_file, encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
    except (OSError, yaml.YAMLError):
        return {}
    if not isinstance(loaded, dict):
        return {}
    return loaded


def _rovo_authorization():
    """
    Return the Rovo Authorization header value.

    ATLASSIAN_ROVO_MCP_AUTHORIZATION overrides the file when a job injects the
    full header. Otherwise the token is agents_credentials.rovo_mcp.token in
    data/auth.yaml and is sent as a Bearer token.

    Returns:
        str: Header value, or None when no token is configured.
    """
    header = os.environ.get(ROVO_AUTHORIZATION_ENV)
    if header:
        return header
    auth = _load_auth_config()
    credentials = auth.get("agents_credentials") or {}
    rovo = credentials.get("rovo_mcp") or {}
    if not isinstance(rovo, dict):
        return None
    token = rovo.get("token")
    if not isinstance(token, str) or not token.strip():
        return None
    token = token.strip()
    if token.startswith("Bearer ") or token.startswith("Basic "):
        return token
    return f"Bearer {token}"


def _rovo_server():
    """
    Build the Atlassian Rovo HTTP connection.

    Returns:
        dict: MultiServerMCPClient connection config for https://mcp.atlassian.com/v2/mcp.
    """
    connection = {
        "url": os.environ.get("ATLASSIAN_ROVO_MCP_URL") or ROVO_MCP_URL,
        "transport": "http",
    }
    authorization = _rovo_authorization()
    if authorization:
        connection["headers"] = {"Authorization": authorization}
    return connection


def servers_for(names):
    """
    Return connection settings for the named MCP servers.

    Args:
        names (list): Server names from an agent's agent.yaml.

    Returns:
        dict: Mapping of server name to a stdio or HTTP connection config.

    Raises:
        KeyError: A name is not registered, or a local server has no tools.
    """
    unknown = [name for name in names if name not in LOCAL_SERVERS and name != "rovo"]
    if unknown:
        raise KeyError(", ".join(unknown))
    missing_tools = [
        name for name in names if name in LOCAL_SERVERS and name not in SERVER_TOOLS
    ]
    if missing_tools:
        raise KeyError(", ".join(missing_tools))
    connections = {}
    for name in names:
        if name == "rovo":
            connections[name] = _rovo_server()
        else:
            connections[name] = _stdio_server(LOCAL_SERVERS[name])
    return connections
