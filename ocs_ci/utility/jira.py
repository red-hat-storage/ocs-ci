import configparser
from atlassian import Jira
from logging import getLogger
from ocs_ci.framework import config
import os

log = getLogger(__name__)

LEGACY_CREDENTIALS_ERROR = (
    "Jira credentials not provided. Please provide them in the config file"
    " /etc/jira.cfg or in the config.AUTH.jira section."
)
EXTENDED_CREDENTIALS_ERROR = (
    "Jira credentials not found. Configure AUTH.jira, /etc/jira.cfg, "
    "data/auth.yaml, --jira-config, or JIRA_URL/JIRA_USERNAME/JIRA_TOKEN env vars."
)


class JiraHelper:
    """
    Simple Jira integration for OCS-CI.
    Requires a dict with keys: url, username, password.
    """

    def __init__(self, config_path=None, *, allow_extended_sources=False):
        """
        Initialize JiraHelper.

        Legacy callers (``JiraHelper()``) use the original resolution order:
        1. config.AUTH.jira
        2. /etc/jira.cfg

        Extended sources (z-stream CLI) enable ``allow_extended_sources=True``:
        1. config.AUTH.jira
        2. config_path (--jira-config), when provided
        3. /etc/jira.cfg
        4. data/auth.yaml (jira or AUTH.jira)
        5. JIRA_URL / JIRA_USERNAME / JIRA_TOKEN environment variables

        Args:
            config_path (str): Optional path to a jira.cfg INI file
            allow_extended_sources (bool): Enable auth.yaml, env, and config_path
                fallbacks for agent/CLI use. Default False preserves legacy behavior.

        """
        jira_auth = self._get_jira_credentials(
            config_path=config_path,
            allow_extended_sources=allow_extended_sources,
        )

        self.url = jira_auth["url"]
        self.username = jira_auth["username"]
        self.password = jira_auth["password"]
        self.visibility = jira_auth.get(
            "visibility", {"type": "group", "value": "Red Hat Employee"}
        )

        log.debug(f"Initializing Jira: {self.url}")
        self.jira = Jira(
            url=self.url, username=self.username, password=self.password, cloud=True
        )

    @staticmethod
    def _normalize_jira_auth(jira_auth: dict) -> dict:
        auth = dict(jira_auth)
        if not auth.get("password") and auth.get("token"):
            auth["password"] = auth["token"]
        if not auth.get("username"):
            auth["username"] = auth.get("email", "") or os.environ.get(
                "JIRA_USERNAME", os.environ.get("JIRA_EMAIL", "")
            )
        return auth

    @classmethod
    def _validate_jira_auth(cls, jira_auth: dict) -> dict:
        auth = cls._normalize_jira_auth(jira_auth)
        missing = [key for key in ("url", "username", "password") if not auth.get(key)]
        if missing:
            raise ValueError(
                f"Jira credentials incomplete; missing: {', '.join(missing)}"
            )
        return auth

    @staticmethod
    def _load_from_auth_yaml() -> dict:
        from ocs_ci.utility.utils import load_auth_config

        auth_data = load_auth_config() or {}
        jira_auth = auth_data.get("AUTH", {}).get("jira") or auth_data.get("jira")
        if jira_auth:
            return JiraHelper._normalize_jira_auth(jira_auth)
        return {}

    @staticmethod
    def _load_from_env() -> dict:
        url = os.environ.get("JIRA_URL")
        username = os.environ.get("JIRA_USERNAME") or os.environ.get("JIRA_EMAIL")
        password = os.environ.get("JIRA_PASSWORD") or os.environ.get("JIRA_TOKEN")
        if url and username and password:
            return {"url": url, "username": username, "password": password}
        return {}

    @classmethod
    def _get_jira_credentials(
        cls, config_path=None, allow_extended_sources=False
    ) -> dict:
        jira_auth = config.AUTH.get("jira")
        if jira_auth:
            return cls._validate_jira_auth(jira_auth)

        if allow_extended_sources and config_path and os.path.exists(config_path):
            return cls._validate_jira_auth(cls._load_from_file(config_path))

        config_file = "/etc/jira.cfg"
        if os.path.exists(config_file):
            return cls._validate_jira_auth(cls._load_from_file(config_file))

        if not allow_extended_sources:
            raise ValueError(LEGACY_CREDENTIALS_ERROR)

        jira_auth = cls._load_from_auth_yaml()
        if jira_auth:
            return cls._validate_jira_auth(jira_auth)

        jira_auth = cls._load_from_env()
        if jira_auth:
            return jira_auth

        raise ValueError(EXTENDED_CREDENTIALS_ERROR)

    @staticmethod
    def _load_from_file(path: str) -> dict:
        """
        Load an INI config file with a [DEFAULT] section
        containing url, username, password and optionally other values.

        Args:
            path (str): Path to the INI config file

        Returns:
            dict: A dictionary containing the URL, username and password for the Jira instance

        """
        ini_config = configparser.ConfigParser()
        ini_config.read(path)

        section = ini_config["DEFAULT"]

        return {
            "url": section["url"],
            "username": section.get("username") or section.get("email"),
            "password": section.get("password") or section.get("token"),
        }

    def search_issues_by_jql(self, jql: str, fields=None) -> list:
        """
        Search Jira issues using JQL and return all matching issues.

        Args:
            jql (str): JQL query string
            fields (str | list | None): Fields to return; defaults to all fields

        Returns:
            list: List of issue dictionaries

        """
        log.debug(f"Searching Jira with JQL: {jql}")
        search_fields = fields if fields is not None else "*all"
        issues = self.jira.enhanced_jql_get_list_of_tickets(jql, fields=search_fields)
        log.info(f"Found {len(issues)} Jira issues matching JQL")
        return issues

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
