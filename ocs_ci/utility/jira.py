import configparser
from atlassian import Jira
from logging import getLogger
from ocs_ci.framework import config
import os

log = getLogger(__name__)


class JiraHelper:
    """
    Simple Jira integration for OCS-CI.
    Requires a dict with keys: url, token.
    """

    def __init__(self):
        """
        Initialize JiraHelper.

        Provide credentials in the config.AUTH.jira section or in the config file /etc/jira.cfg
        """
        config_file = "/etc/jira.cfg"
        jira_auth = config.AUTH.get("jira")
        if not jira_auth:
            if os.path.exists("/etc/jira.cfg"):
                jira_auth = self._load_from_file(config_file)
            else:
                raise ValueError(
                    "Jira credentials not provided. Please provide them in the config file"
                    " /etc/jira.cfg or in the config.AUTH.jira section."
                )

        self.url = jira_auth["url"]
        self.token = jira_auth["token"]
        self.visibility = jira_auth.get(
            "visibility", {"type": "group", "value": "Red Hat Employee"}
        )

        log.debug(f"Initializing Jira: {self.url}")
        self.jira = Jira(url=self.url, token=self.token, cloud=False)

    @staticmethod
    def _load_from_file(path: str) -> dict:
        """
        Load an INI config file with a [DEFAULT] section
        containing url, token and optionally other values.

        Args:
            path (str): Path to the INI config file

        Returns:
            dict: A dictionary containing the URL and token for the Jira instance

        """
        config = configparser.ConfigParser()
        config.read(path)

        section = config["DEFAULT"]

        return {
            "url": section["url"],
            "token": section["token"],
        }

    def get_issue(self, issue_key: str) -> dict:
        """
        Return complete JSON of the Jira issue.

        Args:
            issue_key (str): The key of the Jira issue e.g. 'DFBUGS-2781'

        Returns:
            dict: A dictionary containing the complete JSON of the Jira issue

        """
        log.debug(f"Fetching Jira issue {issue_key}")
        return self.jira.issue(issue_key)

    def get_issue_summary(self, issue_key: str) -> dict:
        """
        Return a compact summary of a Jira issue.

        Args:
            issue_key (str): The key of the Jira issue e.g. 'DFBUGS-2781'

        Returns:
            dict: Compact issue summary with key, summary, status, assignee, priority

        """
        issue = self.get_issue(issue_key)
        fields = issue.get("fields", {})
        return {
            "key": issue.get("key", issue_key),
            "summary": fields.get("summary", ""),
            "status": (fields.get("status") or {}).get("name", "Unknown"),
            "assignee": (fields.get("assignee") or {}).get("displayName", "Unassigned"),
            "priority": (fields.get("priority") or {}).get("name", "None"),
            "url": f"{self.url}/browse/{issue_key}",
        }

    def search_issues(self, jql: str, max_results: int = 10) -> list:
        """
        Search Jira using JQL query.

        Args:
            jql (str): JQL query string
            max_results (int): Maximum number of results to return

        Returns:
            list: List of compact issue dicts (key, summary, status, assignee, priority, url)

        """
        log.debug(f"JQL search: {jql}")
        try:
            response = self.jira.jql(jql, limit=max_results)
        except Exception as e:
            log.warning(f"JQL search failed: {e}")
            return []

        results = []
        for issue in response.get("issues", []):
            fields = issue.get("fields", {})
            results.append(
                {
                    "key": issue.get("key", ""),
                    "summary": fields.get("summary", ""),
                    "status": (fields.get("status") or {}).get("name", "Unknown"),
                    "assignee": (fields.get("assignee") or {}).get(
                        "displayName", "Unassigned"
                    ),
                    "priority": (fields.get("priority") or {}).get("name", "None"),
                    "url": f"{self.url}/browse/{issue.get('key', '')}",
                }
            )
        log.debug(f"JQL search returned {len(results)} results")
        return results

    def add_comment(self, issue_key: str, text: str):
        """
        Add a comment to an issue.

        Args:
            issue_key (str): The key of the Jira issue e.g. 'DFBUGS-2781'
            text (str): The text of the comment

        Returns:
            dict: A dictionary containing the complete JSON of the Jira issue

        """
        log.info(f"Adding comment to {issue_key}: {text}")
        return self.jira.issue_add_comment(issue_key, text, visibility=self.visibility)
