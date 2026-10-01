"""Keep a --dry-run agent from changing Jira or any other application."""

import os
from contextlib import contextmanager

DRY_RUN_ENV = "OCS_AGENT_DRY_RUN"
_TRUTHY = {"1", "true", "yes"}
# Methods the verification agent uses to read Jira. Every other Jira call is a
# write, so dry-run refuses it.
JIRA_READ_METHODS = frozenset(
    {
        "issue",
        "enhanced_jql_get_list_of_tickets",
        "get",
        "resource_url",
    }
)


def env_requests_dry_run(env):
    """
    Return whether the process environment asked for a dry run.

    Args:
        env (dict): Process environment.

    Returns:
        bool: True when OCS_AGENT_DRY_RUN is 1, true, or yes.
    """
    raw = str(env.get(DRY_RUN_ENV) or "").strip().lower()
    return raw in _TRUTHY


def dry_run_enabled(value=None, env=None):
    """
    Return whether this run must not update Jira or another application.

    Args:
        value: Argument value from the agent request. True and the strings
            1, true, and yes count as enabled.
        env (dict): Process environment. Defaults to os.environ when omitted
            and value is not already enabled.

    Returns:
        bool: True when updates must be refused.
    """
    if value is True:
        return True
    if isinstance(value, str) and value.strip().lower() in _TRUTHY:
        return True
    if env is None:
        env = os.environ
    return env_requests_dry_run(env)


@contextmanager
def activate_dry_run(request):
    """
    Export OCS_AGENT_DRY_RUN for this run so MCP tool processes inherit it.

    Args:
        request (dict): Value returned by build_request.

    Yields:
        bool: True when the run is a dry run.
    """
    enabled = dry_run_enabled((request.get("args") or {}).get("dry_run"))
    previous = os.environ.get(DRY_RUN_ENV)
    if enabled:
        os.environ[DRY_RUN_ENV] = "1"
    try:
        yield enabled
    finally:
        if not enabled:
            return
        if previous is None:
            os.environ.pop(DRY_RUN_ENV, None)
        else:
            os.environ[DRY_RUN_ENV] = previous


class ReadOnlyJira:
    """
    Jira client that allows reads and refuses every other call.

    A dry run may search and fetch issues. Comment, edit, transition, and
    any other write raises RuntimeError before the request is sent.
    """

    def __init__(self, inner):
        """
        Args:
            inner: Atlassian Jira client used for allowed reads.
        """
        self._inner = inner

    def __getattr__(self, name):
        """
        Return a read method, or refuse a write.

        Args:
            name (str): Jira client attribute.

        Returns:
            object: The underlying read method.

        Raises:
            RuntimeError: The attribute is not a read used by the agent.
        """
        if name not in JIRA_READ_METHODS:
            raise RuntimeError(
                f"dry-run: refused Jira.{name}. "
                "Jira and other applications stay unchanged."
            )
        return getattr(self._inner, name)
