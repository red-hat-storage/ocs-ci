"""ReportPortal MCP server.

Tools call ocs_ci.utility.reporting. Start this server with:

    python -m ocs_ci.agents.mcp.servers.reportportal
"""

TOOL_NAMES = (
    "reportportal_get_launch",
    "reportportal_get_test_log",
)


def main():
    """Start the ReportPortal MCP server on stdio."""
    raise NotImplementedError(
        "The ReportPortal MCP server requires the mcp package in the agents extra"
    )


if __name__ == "__main__":
    main()
