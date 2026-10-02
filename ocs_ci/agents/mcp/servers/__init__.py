"""MCP tool servers. Tool implementations live in these modules."""

# Names live here so `python -m ocs_ci.agents.mcp.servers.<name>` is not imported
# once by this package and again as __main__.
SERVER_TOOLS = {
    "jira": (
        "jira_get_issue",
        "jira_save_verification_report",
        "jira_search_issues",
    ),
    "cluster": (
        "cluster_get_pods",
        "cluster_get_ceph_status",
    ),
    "reportportal": (
        "reportportal_get_launch",
        "reportportal_get_test_log",
    ),
    "jenkins": (
        "jenkins_find_cluster",
        "jenkins_get_build",
        "jenkins_trigger_build",
    ),
}
