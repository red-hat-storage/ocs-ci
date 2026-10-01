"""Write one verification report per issue for later agents."""

import re
from pathlib import Path

import yaml

REPORT_FIELDS = (
    "bug_description",
    "affected_version",
    "fix_version",
    "environment_reported",
    "environment_verify",
    "upgrade_scenario",
    "verification_steps",
    "additional_info",
    "git_prs",
)
OPTIONAL_REPORT_FIELDS = ("cluster", "dry_run")
REPORTS_ROOT = Path(__file__).resolve().parent / "reports"
_ISSUE_KEY = re.compile(r"^[A-Z][A-Z0-9]+-\d+$")


def write_verification_report(version, issue_key, report, root=None):
    """
    Store one issue report under reports/<version>/<issue>.yaml.

    Args:
        version (str): Version directory name, for example odf-5.0.
        issue_key (str): Jira issue key.
        report (dict): Report fields collected from that issue.
        root (Path): Reports directory. Defaults to the agent reports folder.

    Returns:
        Path: Written YAML file.

    Raises:
        ValueError: The version, issue key, or report fields are not usable.

    """
    version_name = _version_dirname(version)
    key = str(issue_key).strip()
    if not _ISSUE_KEY.fullmatch(key):
        raise ValueError(f"issue key is not usable as a file name: {issue_key}")
    if not isinstance(report, dict):
        raise ValueError("report must be a mapping")
    missing = [name for name in REPORT_FIELDS if name not in report]
    if missing:
        raise ValueError("report missing fields: " + ", ".join(missing))
    root = Path(root) if root else REPORTS_ROOT
    directory = root / version_name
    directory.mkdir(parents=True, exist_ok=True)
    document = {"key": key, "url": report.get("url") or _issue_url(key)}
    for name in REPORT_FIELDS:
        document[name] = report[name]
        if name == "fix_version":
            document["parent_issues"] = _parent_issues(report.get("parent_issues"))
    for name in OPTIONAL_REPORT_FIELDS:
        if report.get(name):
            document[name] = report[name]
    path = directory / f"{key}.yaml"
    path.write_text(
        yaml.safe_dump(document, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    _update_index(directory, version_name, path.name)
    return path


def _parent_issues(value):
    """
    Normalize parent issues stored on a verification report.

    Args:
        value (list): Parent issue mappings from the report.

    Returns:
        list: key, summary, status, and relation. Empty when the issue has
            no parent.

    """
    if not isinstance(value, list):
        return []
    cleaned = []
    for item in value:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip()
        if not _ISSUE_KEY.fullmatch(key):
            continue
        cleaned.append(
            {
                "key": key,
                "summary": item.get("summary") or "",
                "status": item.get("status") or "",
                "relation": item.get("relation") or "",
            }
        )
    return cleaned


def _version_dirname(version):
    """
    Turn a version name into a single path segment.

    Args:
        version (str): Version from the agent request.

    Returns:
        str: Directory name.

    Raises:
        ValueError: The version is empty.

    """
    cleaned = str(version).strip().replace("/", "-").replace("\\", "-")
    if not cleaned or cleaned in {".", ".."}:
        raise ValueError("version is required")
    return cleaned


def _issue_url(issue_key):
    """
    Return the browse URL for a Red Hat Jira issue.

    Args:
        issue_key (str): Jira issue key.

    Returns:
        str: Issue URL.
    """
    return f"https://redhat.atlassian.net/browse/{issue_key}"


def _update_index(directory, version, filename):
    """
    Add the report file to index.yaml in that version directory.

    Args:
        directory (Path): Version directory.
        version (str): Version name stored in the index.
        filename (str): Report file name.
    """
    index_path = directory / "index.yaml"
    reports = []
    if index_path.is_file():
        loaded = yaml.safe_load(index_path.read_text(encoding="utf-8")) or {}
        if isinstance(loaded, dict):
            reports = list(loaded.get("reports") or [])
    if filename not in reports:
        reports.append(filename)
    reports = sorted(name for name in reports if name != "index.yaml")
    index = {"version": version, "reports": reports}
    index_path.write_text(
        yaml.safe_dump(index, sort_keys=False),
        encoding="utf-8",
    )
