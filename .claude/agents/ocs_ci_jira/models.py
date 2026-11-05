"""Constants and models for the OCS-CI JIRA agent."""

DEFAULT_JIRA_FIELDS = [
    "summary",
    "description",
    "status",
    "priority",
    "components",
    "labels",
    "fixVersions",
    "versions",
    "issuetype",
    "assignee",
    "reporter",
    "created",
    "updated",
]

JIRA_PROJECT_DFBUGS = "Data Foundation Bugs"
JIRA_ISSUE_TYPE_BUG = "Bug"
JIRA_STATUS_ON_QA = "ON_QA"

# Write operations default to dry-run unless explicitly disabled
WRITE_DRY_RUN_DEFAULT = True
