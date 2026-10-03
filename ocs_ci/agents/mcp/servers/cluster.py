"""Read-only cluster MCP server for ODF agents.

Start this server with:

    python -m ocs_ci.agents.mcp.servers.cluster
"""

TOOL_NAMES = (
    "cluster_get_pods",
    "cluster_get_ceph_status",
)


def main():
    """Start the cluster MCP server on stdio."""
    raise NotImplementedError(
        "The cluster MCP server requires the mcp package in the agents extra"
    )


if __name__ == "__main__":
    main()
