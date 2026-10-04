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


class _ReportDumper(yaml.SafeDumper):
    """YAML dumper that keeps multiline text as a block scalar."""


def _represent_str(dumper, data):
    """
    Represent a multiline string as a YAML block scalar.

    Args:
        dumper: YAML dumper.
        data (str): String value.

    Returns:
        A YAML scalar node.
    """
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return yaml.representer.SafeRepresenter.represent_str(dumper, data)


_ReportDumper.add_representer(str, _represent_str)


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
    document["summary"] = _summary_document(report.get("summary"))
    for name in REPORT_FIELDS:
        document[name] = report[name]
        if name == "fix_version":
            document["parent_issues"] = _parent_issues(report.get("parent_issues"))
        if name == "verification_steps":
            document["tests"] = _tests(report.get("tests"))
    for name in OPTIONAL_REPORT_FIELDS:
        if report.get(name):
            document[name] = report[name]
        if name == "cluster" and report.get("cluster_check"):
            document["cluster_check"] = _cluster_check(report.get("cluster_check"))
    path = directory / f"{key}.yaml"
    path.write_text(
        yaml.dump(
            document,
            Dumper=_ReportDumper,
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    _update_index(directory, version_name, path.name)
    return path


def write_execution_result(version, issue_key, result, root=None):
    """
    Store the cluster execution result beside the verification plan.

    The plan file is left unchanged. The result is
    reports/<version>/<issue>-result.yaml.

    Args:
        version (str): Version directory name, for example odf-4.22.6.
        issue_key (str): Jira issue key.
        result (dict): status, reasons, steps, and an optional GitHub issue URL.
        root (Path): Reports directory. Defaults to the agent reports folder.

    Returns:
        Path: Written YAML file.

    Raises:
        ValueError: The version or issue key is not usable.
    """
    version_name = _version_dirname(version)
    key = str(issue_key).strip()
    if not _ISSUE_KEY.fullmatch(key):
        raise ValueError(f"issue key is not usable as a file name: {issue_key}")
    root = Path(root) if root else REPORTS_ROOT
    directory = root / version_name
    directory.mkdir(parents=True, exist_ok=True)
    document = {
        "key": key,
        "cluster": str((result or {}).get("cluster") or ""),
        "status": str((result or {}).get("status") or ""),
        "reasons": [
            str(reason)
            for reason in ((result or {}).get("reasons") or [])
            if str(reason).strip()
        ],
        "steps": _execution_steps((result or {}).get("steps")),
    }
    issue_url = str((result or {}).get("github_issue") or "").strip()
    if issue_url:
        document["github_issue"] = issue_url
    report_path = str((result or {}).get("verification_report") or "").strip()
    if report_path:
        document["verification_report"] = report_path
    path = directory / f"{key}-result.yaml"
    path.write_text(
        yaml.dump(
            document,
            Dumper=_ReportDumper,
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    _update_index(directory, version_name, path.name)
    return path


def write_verification_markdown(version, issue_key, text, root=None):
    """
    Store the Markdown verification report beside the plan.

    Args:
        version (str): Version directory name.
        issue_key (str): Jira issue key.
        text (str): Markdown report.
        root (Path): Reports directory. Defaults to the agent reports folder.

    Returns:
        Path: Written Markdown file.

    Raises:
        ValueError: The version or issue key is not usable.
    """
    version_name = _version_dirname(version)
    key = str(issue_key).strip()
    if not _ISSUE_KEY.fullmatch(key):
        raise ValueError(f"issue key is not usable as a file name: {issue_key}")
    root = Path(root) if root else REPORTS_ROOT
    directory = root / version_name
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{key}-verification.md"
    path.write_text(str(text or "").rstrip() + "\n", encoding="utf-8")
    _update_index(directory, version_name, path.name)
    return path


def _execution_steps(value):
    """
    Normalize step results stored on an execution report.

    Args:
        value (list): Step mappings from the executor.

    Returns:
        list: step, command, exit_code, passed, and output_excerpt.
    """
    if not isinstance(value, list):
        return []
    cleaned = []
    for item in value:
        if not isinstance(item, dict):
            continue
        exit_code = item.get("exit_code")
        cleaned.append(
            {
                "step": item.get("step") or "",
                "command": str(item.get("command") or ""),
                "exit_code": "" if exit_code is None else exit_code,
                "passed": bool(item.get("passed")),
                "output_excerpt": str(item.get("output_excerpt") or ""),
            }
        )
    return cleaned


def _summary_document(value):
    """
    Normalize the summary stored on a verification report.

    Args:
        value (dict or str): Structured summary, or an older plain summary.

    Returns:
        dict: issue, reproduction_steps, and expected_results.

    """
    if isinstance(value, dict):
        issue = str(value.get("issue") or "").strip()
        reproduction = _string_list(value.get("reproduction_steps"))
        expected = _string_list(value.get("expected_results"))
    else:
        issue = str(value or "").strip()
        reproduction = []
        expected = []
    return {
        "issue": issue,
        "reproduction_steps": reproduction,
        "expected_results": expected,
    }


def _string_list(value):
    """
    Return a list of non-empty strings.

    Args:
        value (list): Text items.

    Returns:
        list: Trimmed strings.

    """
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


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


def _tests(value):
    """
    Normalize tests stored on a verification report.

    Args:
        value (list): Test mappings from the bug report.

    Returns:
        list: name and location. Empty when the bug names no test under tests/.

    """
    if not isinstance(value, list):
        return []
    cleaned = []
    seen = set()
    for item in value:
        if isinstance(item, str):
            name = item
            location = item
        elif isinstance(item, dict):
            name = str(item.get("name") or "").strip()
            location = str(item.get("location") or name).strip()
        else:
            continue
        if not name or not location or location in seen:
            continue
        if not location.startswith("tests/") and not name.startswith("test_"):
            continue
        seen.add(location)
        cleaned.append({"name": name, "location": location})
    return cleaned


def _cluster_check(value):
    """
    Normalize the Jenkins cluster fitness stored on a report.

    Args:
        value (dict): Cluster check from the Jenkins deploy build.

    Returns:
        dict: cluster, fits, reasons, jenkins, and details.
    """
    if not isinstance(value, dict):
        return {
            "cluster": "",
            "fits": False,
            "reasons": [],
            "jenkins": {},
            "details": {},
        }
    jenkins = value.get("jenkins") if isinstance(value.get("jenkins"), dict) else {}
    details = value.get("details") if isinstance(value.get("details"), dict) else {}
    reasons = [
        str(reason) for reason in (value.get("reasons") or []) if str(reason).strip()
    ]
    return {
        "cluster": str(value.get("cluster") or ""),
        "fits": bool(value.get("fits")),
        "reasons": reasons,
        "jenkins": {
            "job": str(jenkins.get("job") or ""),
            "build": jenkins.get("build") or "",
            "result": str(jenkins.get("result") or ""),
            "url": str(jenkins.get("url") or ""),
        },
        "details": details,
    }


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
