"""Jira MCP server.

Tools call ocs_ci.utility.jira.JiraHelper. Start this server with:

    python -m ocs_ci.agents.mcp.servers.jira
"""

TOOL_NAMES = (
    "jira_get_issue",
    "jira_search_issues",
)


def main():
    """Start the Jira MCP server on stdio."""
    raise NotImplementedError(
        "The Jira MCP server requires the mcp package in the agents extra"
    )


if __name__ == "__main__":
    main()
