"""Jenkins MCP server for ODF agents.

Tools call the shared Jenkins client. Start this server with:

    python -m ocs_ci.agents.mcp.servers.jenkins

jenkins_trigger_build starts a job and is refused during --dry-run.
"""

import json

TOOL_NAMES = (
    "jenkins_find_cluster",
    "jenkins_get_build",
    "jenkins_trigger_build",
)


def jenkins_find_cluster(cluster_name: str) -> str:
    """
    Find the newest OCS deploy build for a cluster and whether its agent is online.

    Args:
        cluster_name (str): Jenkins CLUSTER_NAME, which is also the agent label.

    Returns:
        str: JSON with the job, build number, result, url, selected parameters,
            and agent_online. An empty build means no recent deploy matched.
    """
    from ocs_ci.agents.jenkins_api import JenkinsClient

    client = JenkinsClient()
    build = client.latest_deploy(cluster_name) or {}
    payload = {
        "cluster": cluster_name,
        "agent_online": client.agent_online(cluster_name),
        "job": build.get("job") or "",
        "build": build.get("number") or 0,
        "result": build.get("result") or "",
        "url": build.get("url") or "",
        "parameters": build.get("parameters") or {},
    }
    return json.dumps(payload)


def jenkins_get_build(job: str, number: int) -> str:
    """
    Read one Jenkins build.

    Password, token, and inline YAML parameters are omitted.

    Args:
        job (str): Jenkins job name, for example qe-deploy-ocs-cluster.
        number (int): Build number.

    Returns:
        str: JSON with number, url, result, building, and parameters.
    """
    from ocs_ci.agents.jenkins_api import JenkinsClient

    build = JenkinsClient().get_build(job, number)
    return json.dumps(build)


def jenkins_trigger_build(job: str, parameters_json: str) -> str:
    """
    Start a Jenkins job with buildWithParameters.

    parameters_json is a JSON object of the job's parameter names, the same
    fields a curl --data-urlencode list would send. For qe-deploy-ocs-cluster
    that includes CLUSTER_NAME, OCP_VERSION, OCS_VERSION, PLATFORM_CONF,
    CLUSTER_CONF, and the install flags. This is refused during --dry-run.

    Args:
        job (str): Jenkins job name, for example qe-deploy-ocs-cluster.
        parameters_json (str): JSON object of parameter names and values.

    Returns:
        str: JSON with job, queued, and the Jenkins queue URL.
    """
    from ocs_ci.agents.jenkins_api import JenkinsClient

    parameters = json.loads(parameters_json)
    if not isinstance(parameters, dict):
        raise ValueError("parameters_json must be a JSON object")
    queued = JenkinsClient().trigger_build(job, parameters)
    return json.dumps(queued)


def build_server():
    """
    Register the Jenkins tools on a FastMCP server.

    Returns:
        FastMCP: Server that exposes the Jenkins tools.

    Raises:
        ImportError: The mcp package is not installed.
    """
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("jenkins")
    server.tool(name="jenkins_find_cluster")(jenkins_find_cluster)
    server.tool(name="jenkins_get_build")(jenkins_get_build)
    server.tool(name="jenkins_trigger_build")(jenkins_trigger_build)
    return server


def main():
    """Start the Jenkins MCP server on stdio."""
    from ocs_ci.agents.runtime.logging import configure_agent_logging

    configure_agent_logging()
    try:
        server = build_server()
    except ImportError as error:
        raise NotImplementedError(
            "The Jenkins MCP server requires the mcp package in the agents extra"
        ) from error
    server.run()


if __name__ == "__main__":
    main()
