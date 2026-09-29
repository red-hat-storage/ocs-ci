"""MCP tool servers. Tool implementations live in these modules."""

from ocs_ci.agents.mcp.servers import cluster, jira, reportportal

SERVER_TOOLS = {
    "jira": jira.TOOL_NAMES,
    "cluster": cluster.TOOL_NAMES,
    "reportportal": reportportal.TOOL_NAMES,
}
