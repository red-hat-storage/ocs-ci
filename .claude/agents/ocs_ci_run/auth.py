"""Load Jenkins credentials from data/auth.yaml or environment."""

import os
from pathlib import Path

import yaml

from config import AUTH_YAML_CANDIDATES


class JenkinsAuthError(RuntimeError):
    """Jenkins credentials could not be resolved."""


def _jenkins_username_from_email(email: str) -> str:
    if "@" in email:
        return email.split("@", 1)[0]
    return email


def load_jenkins_auth(
    *,
    auth_path: Path | None = None,
    username: str | None = None,
    token: str | None = None,
) -> tuple[str, str]:
    """
    Resolve Jenkins API username and token.

    Priority: explicit args → env JENKINS_USER/JENKINS_TOKEN → data/auth.yaml.

    Returns:
        tuple: (username, api_token)

    """
    user = username or os.environ.get("JENKINS_USER")
    api_token = token or os.environ.get("JENKINS_TOKEN")

    if user and api_token:
        return user, api_token

    path = auth_path
    if path is None:
        for candidate in AUTH_YAML_CANDIDATES:
            if candidate.is_file():
                path = candidate
                break

    if path is None or not path.is_file():
        raise JenkinsAuthError(
            "Jenkins credentials not found. Set JENKINS_USER/JENKINS_TOKEN or "
            f"configure jenkins: in {AUTH_YAML_CANDIDATES[0]}"
        )

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    jenkins = data.get("jenkins") or data.get("AUTH", {}).get("jenkins") or {}

    email = jenkins.get("email") or jenkins.get("username")
    api_token = api_token or jenkins.get("token") or jenkins.get("password")
    user = user or (email and _jenkins_username_from_email(str(email)))

    if not user or not api_token:
        raise JenkinsAuthError(f"jenkins.email and jenkins.token required in {path}")

    return str(user), str(api_token)
