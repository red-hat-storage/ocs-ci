"""Claude Code on Vertex, using a base64 service account from data/auth.yaml."""

import atexit
import base64
import binascii
import json
import logging
import os
import tempfile

from ocs_ci.agents.mcp.registry import _load_auth_config

logger = logging.getLogger(__name__)

_credential_path = None


def load_claude_code_account():
    """
    Decode the Claude Code service account from data/auth.yaml.

    agents_credentials.claude_code is the service account JSON, base64-encoded.

    Returns:
        dict: Service account fields, including project_id and private_key.

    Raises:
        ValueError: The entry is missing, not base64, or not a service account.
    """
    auth = _load_auth_config()
    credentials = auth.get("agents_credentials") or {}
    raw = credentials.get("claude_code") if isinstance(credentials, dict) else None
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(
            "Set agents_credentials.claude_code in data/auth.yaml to the "
            "base64-encoded service account JSON."
        )
    try:
        decoded = base64.b64decode(_padded(raw.strip()), validate=True)
        account = json.loads(decoded.decode("utf-8"))
    except (binascii.Error, UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise ValueError(
            "agents_credentials.claude_code is not a base64-encoded "
            "service account JSON."
        ) from error
    if not isinstance(account, dict):
        raise ValueError(
            "agents_credentials.claude_code is not a base64-encoded "
            "service account JSON."
        )
    if account.get("type") != "service_account" or not account.get("project_id"):
        raise ValueError(
            "agents_credentials.claude_code is not a service account JSON."
        )
    if not account.get("private_key"):
        raise ValueError(
            "agents_credentials.claude_code is missing the service account key."
        )
    return account


def claude_code_environment():
    """
    Return the process environment Claude Code needs for Vertex.

    The service account is written to a private temporary file. Claude Code
    reads it from GOOGLE_APPLICATION_CREDENTIALS. OCS_AGENT_CLOUD_ML_REGION
    overrides the Vertex region. The default region is us-east5.

    Returns:
        dict: Environment for the claude subprocess.
    """
    account = load_claude_code_account()
    path = _credential_file(account)
    region = (
        os.environ.get("OCS_AGENT_CLOUD_ML_REGION")
        or os.environ.get("CLOUD_ML_REGION")
        or "us-east5"
    )
    env = os.environ.copy()
    env["GOOGLE_APPLICATION_CREDENTIALS"] = path
    env["CLAUDE_CODE_USE_VERTEX"] = "1"
    env["ANTHROPIC_VERTEX_PROJECT_ID"] = account["project_id"]
    env["GOOGLE_CLOUD_PROJECT"] = account["project_id"]
    env["CLOUD_ML_REGION"] = region
    logger.info(
        f"Claude Code will use Vertex project {account['project_id']} in {region}"
    )
    return env


def _padded(value):
    """
    Add base64 padding when the stored value omitted it.

    Args:
        value (str): Base64 text.

    Returns:
        str: Padded base64 text.
    """
    return value + ("=" * ((-len(value)) % 4))


def _credential_file(account):
    """
    Write the service account JSON to a private file once per process.

    Args:
        account (dict): Decoded service account.

    Returns:
        str: Absolute path of the credentials file.
    """
    global _credential_path
    if _credential_path and os.path.exists(_credential_path):
        return _credential_path
    descriptor, path = tempfile.mkstemp(prefix="ocs-ci-claude-", suffix=".json")
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(account, handle)
    os.chmod(path, 0o600)
    _credential_path = path
    atexit.register(_remove_credential_file, path)
    return path


def _remove_credential_file(path):
    """
    Delete the temporary service account file.

    Args:
        path (str): File written for GOOGLE_APPLICATION_CREDENTIALS.
    """
    try:
        os.remove(path)
    except OSError:
        return
