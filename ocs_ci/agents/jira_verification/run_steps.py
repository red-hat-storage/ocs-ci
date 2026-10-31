"""Run a saved verification plan once and keep the command output."""

import asyncio
import json
import logging
import os
import re

import requests

from ocs_ci.agents.jira_verification.command_policy import command_allowed
from ocs_ci.agents.jira_verification.summary import summary_text

logger = logging.getLogger(__name__)

_OPENSHIFT_VERSION = (
    "oc get clusterversion version -o jsonpath={.status.desired.version}"
)
_STORAGE_STATUS = "oc get storagecluster -A -o json"
_CEPH_STATUS = "oc get cephcluster -A -o json"


def verify_with_claude(report, kubeconfig, cluster):
    """
    Ask Claude Code to verify the report's reproduction steps on the cluster.

    KUBECONFIG is passed in the environment. Claude Code runs oc itself.

    Args:
        report (dict): Verification plan.
        kubeconfig (str): Kubeconfig file for the cluster.
        cluster (str): Cluster name.

    Returns:
        dict: status, reasons, commands Claude ran, and its verdict.

    Raises:
        RuntimeError: The Claude Agent SDK is not installed.
    """
    prompt = _verification_prompt(report)
    logger.info(
        f"Asking Claude Code to verify {report.get('key') or 'the issue'} on {cluster}"
    )
    final, commands = _ask_claude(prompt, kubeconfig)
    status = _verdict(final)
    logger.info(f"Claude Code verification status: {status}")
    return {
        "cluster": cluster,
        "status": status,
        "reasons": [] if status == "passed" else [_first_line(final)],
        "steps": commands,
        "openshift_version": _labeled(final, "OpenShift version"),
        "odf_version": _labeled(final, "ODF version"),
        "health": [],
        "checked": True,
        "claude_text": final,
    }


def _verification_prompt(report):
    """
    Build the Claude Code prompt from the report reproduction steps.

    Args:
        report (dict): Verification plan.

    Returns:
        str: Prompt telling Claude Code to verify the bug.
    """
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    lines = [
        "Verify this bug on the connected OpenShift cluster.",
        "KUBECONFIG is already set. Use oc. Do not tear the cluster down and do not delete an existing StorageClass.",
        "",
        f"Issue: {summary.get('issue') or report.get('bug_description') or ''}",
        "",
        "Reproduction steps:",
    ]
    steps = summary.get("reproduction_steps") or []
    if not steps:
        steps = [str(report.get("bug_description") or "").strip()]
    for index, step in enumerate(steps, start=1):
        lines.append(f"{index}. {step}")
    lines.extend(["", "Expected results:"])
    for index, step in enumerate(summary.get("expected_results") or [], start=1):
        lines.append(f"{index}. {step}")
    lines.extend(
        [
            "",
            "First print the OpenShift version and the ODF version.",
            "Confirm the StorageCluster phase is Ready and the CephCluster health is HEALTH_OK.",
            "Then carry out the reproduction steps and decide whether the expected results hold.",
            "End with these lines:",
            "Status: passed",
            "or",
            "Status: failed",
            "OpenShift version: <version>",
            "ODF version: <version>",
        ]
    )
    return "\n".join(lines)


def _ask_claude(prompt, kubeconfig):
    """
    Run Claude Code with Bash and the cluster kubeconfig.

    Args:
        prompt (str): Verification prompt.
        kubeconfig (str): Kubeconfig path.

    Returns:
        tuple: Final reply text, and the commands Claude ran.

    Raises:
        RuntimeError: The Claude Agent SDK is not installed.
    """
    try:
        from claude_agent_sdk import (
            AssistantMessage,
            ClaudeAgentOptions,
            ResultMessage,
            TextBlock,
            ToolResultBlock,
            ToolUseBlock,
            UserMessage,
            query,
        )
    except ImportError as error:
        raise RuntimeError(
            "Cluster verification requires claude-agent-sdk in the agent environment."
        ) from error

    env = dict(os.environ)
    env["KUBECONFIG"] = str(kubeconfig)
    options = ClaudeAgentOptions(
        tools=["Bash"],
        allowed_tools=["Bash"],
        permission_mode="dontAsk",
        max_turns=20,
        cwd="/tmp",
        env=env,
        system_prompt=(
            "You verify one ODF bug on the cluster. KUBECONFIG is set. "
            "Use oc to carry out the reproduction steps and compare them with the expected results."
        ),
    )
    commands = []
    commands_by_id = {}

    async def _query():
        final = ""
        texts = []
        async for message in query(prompt=prompt, options=options):
            if isinstance(message, AssistantMessage):
                for block in message.content or []:
                    if isinstance(block, TextBlock) and block.text:
                        texts.append(block.text)
                    elif isinstance(block, ToolUseBlock) and block.name == "Bash":
                        command = str((block.input or {}).get("command") or "")
                        logger.info(f"Claude Code running: {command}")
                        record = {
                            "id": block.id,
                            "step": len(commands) + 1,
                            "command": command,
                            "exit_code": "",
                            "passed": False,
                            "output": "",
                            "output_excerpt": "",
                        }
                        commands.append(record)
                        commands_by_id[block.id] = record
            elif isinstance(message, UserMessage) and isinstance(message.content, list):
                for block in message.content:
                    if not isinstance(block, ToolResultBlock):
                        continue
                    record = commands_by_id.get(block.tool_use_id)
                    if record is None:
                        record = next(
                            (item for item in commands if item["exit_code"] == ""),
                            None,
                        )
                    if record is None:
                        continue
                    output = _tool_output(block.content)
                    record["output"] = output
                    record["output_excerpt"] = " ".join(output.split())[:400]
                    record["passed"] = not block.is_error
                    record["exit_code"] = 0 if not block.is_error else 1
            elif isinstance(message, ResultMessage):
                final = message.result or ""
                if message.is_error:
                    logger.error(f"Claude Code finished with {message.subtype}")
        return final or "\n".join(texts)

    return asyncio.run(_query()), commands


def _tool_output(content):
    """
    Return the text of one Claude Code tool result.

    Args:
        content: Tool result content. A string, or a list of text blocks.

    Returns:
        str: Command output.
    """
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(str(item.get("text") or ""))
        return "\n".join(part for part in parts if part)
    return str(content)


def _verdict(text):
    """
    Read the pass or fail line from Claude Code's reply.

    Args:
        text (str): Final reply.

    Returns:
        str: passed or failed.
    """
    match = re.search(r"status:\s*(passed|failed)", text or "", re.IGNORECASE)
    if not match:
        return "failed"
    return match.group(1).lower()


def _labeled(text, label):
    """
    Return the value after a label line.

    Args:
        text (str): Final reply.
        label (str): Label such as OpenShift version.

    Returns:
        str: Value, or an empty string.
    """
    match = re.search(rf"{re.escape(label)}:\s*(.+)", text or "", re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def _first_line(text):
    """
    Return the first non-empty line of a reply.

    Args:
        text (str): Final reply.

    Returns:
        str: First line, clipped.
    """
    for line in (text or "").splitlines():
        if line.strip():
            return line.strip()[:400]
    return "Claude Code did not return a verification result."


def run_saved_steps(report, plan_steps, cluster):
    """
    Check cluster health, then run each saved verification command once.

    StorageCluster phase must be Ready and CephCluster health must be
    HEALTH_OK before the bug steps run. OpenShift and ODF versions are
    logged first.

    Args:
        report (dict): Verification plan.
        plan_steps (list): Verification steps.
        cluster (str): Cluster name.

    Returns:
        dict: status, reasons, steps, versions, and health output.
    """
    openshift = _exec(_OPENSHIFT_VERSION)
    storage = _exec(_STORAGE_STATUS)
    ceph = _exec(_CEPH_STATUS)
    openshift_version = (openshift.get("output") or "").strip()
    storage_lines, storage_ok, odf_version = _storage_status(storage)
    ceph_lines, ceph_ok = _ceph_status(ceph)
    logger.info(f"OpenShift version: {openshift_version or 'unknown'}")
    logger.info(f"ODF version: {odf_version or 'unknown'}")
    for line in storage_lines:
        logger.info(f"StorageCluster: {line}")
    for line in ceph_lines:
        logger.info(f"CephCluster: {line}")
    logger.info(f"StorageCluster status: {'OK' if storage_ok else 'not OK'}")
    logger.info(f"CephCluster status: {'OK' if ceph_ok else 'not OK'}")
    health = [
        _health_row("StorageCluster", storage, storage_ok, storage_lines),
        _health_row("CephCluster", ceph, ceph_ok, ceph_lines),
    ]
    result = {
        "cluster": cluster,
        "status": "blocked",
        "reasons": [],
        "steps": [],
        "openshift_version": openshift_version,
        "odf_version": odf_version,
        "health": health,
        "checked": True,
    }
    if not storage_ok or not ceph_ok:
        result["reasons"] = [
            "StorageCluster and CephCluster must be OK before verification steps run."
        ]
        result["steps"] = [
            _not_run(step, index) for index, step in enumerate(plan_steps, start=1)
        ]
        return result
    executed = [
        _run_step(step, index, plan_steps)
        for index, step in enumerate(plan_steps, start=1)
    ]
    failed = [step for step in executed if not step["passed"]]
    result["steps"] = executed
    result["status"] = "passed" if executed and not failed else "failed"
    if failed:
        result["reasons"] = [
            f"Step {step['step']} exited {step['exit_code']}" for step in failed
        ]
    return result


def render_verification_markdown(report, result):
    """
    Render the verification log as Markdown for the Jira ticket.

    Args:
        report (dict): Verification plan.
        result (dict): Output from run_saved_steps.

    Returns:
        str: Markdown report.
    """
    key = str(report.get("key") or "")
    lines = [
        f"# Verification report {key}",
        "",
        f"- Issue: {report.get('url') or key}",
        f"- Cluster: {result.get('cluster') or ''}",
        f"- OpenShift version: {result.get('openshift_version') or 'unknown'}",
        f"- ODF version: {result.get('odf_version') or 'unknown'}",
        f"- Status: {result.get('status') or ''}",
        "",
        "## Issue",
        "",
        summary_text(report.get("summary"))
        or str(report.get("bug_description") or "").strip(),
        "",
        "## Claude Code",
        "",
        result.get("claude_text") or "",
        "",
        "## Health",
        "",
    ]
    for check in result.get("health") or []:
        state = "OK" if check.get("passed") else "not OK"
        lines.extend(
            [
                f"### {check.get('name')} ({state})",
                "",
                "```",
                _fence(check.get("output") or check.get("output_excerpt") or ""),
                "```",
                "",
            ]
        )
    lines.extend(["## Verification steps", ""])
    for step in result.get("steps") or []:
        lines.extend(
            [
                f"### Step {step.get('step')}",
                "",
                f"Command: `{step.get('command') or ''}`",
                "",
                f"Exit code: {step.get('exit_code')}",
                "",
                "```",
                _fence(step.get("output") or step.get("output_excerpt") or ""),
                "```",
                "",
            ]
        )
    reasons = [
        str(reason) for reason in (result.get("reasons") or []) if str(reason).strip()
    ]
    if reasons:
        lines.extend(["## Reasons", ""])
        lines.extend(f"- {reason}" for reason in reasons)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


_QA_CONTACT_FIELD = "customfield_10470"
_COMMENT_LIMIT = 6000


def attach_verification_report(issue_key, path, status, result=None):
    """
    Comment on the Jira issue with the verification result and tag QA Contact.

    The Markdown file is attached only when it is too long for the comment.
    The comment then names that attachment.

    Args:
        issue_key (str): Jira issue key.
        path (Path): Markdown file.
        status (str): passed or failed.
        result (dict): Execution result with cluster, versions, and steps.

    Returns:
        str: Attachment filename, or an empty string when the file was not attached.
    """
    from ocs_ci.utility.jira import resolve_agent_jira_auth

    auth = resolve_agent_jira_auth()
    url = str(auth.get("url") or "").rstrip("/")
    report_text = path.read_text(encoding="utf-8")
    filename = ""
    if len(report_text) > _COMMENT_LIMIT:
        filename = _upload_report(url, auth, issue_key, path)
    contact = _qa_contact(url, auth, issue_key)
    comment = _evidence_comment(issue_key, status, result or {}, filename, report_text)
    _comment(url, auth, issue_key, comment, contact)
    return filename


def _upload_report(url, auth, issue_key, path):
    """
    Attach the Markdown report to the Jira issue.

    Args:
        url (str): Jira site URL.
        auth (dict): username and password.
        issue_key (str): Jira issue key.
        path (Path): Markdown file.

    Returns:
        str: Attachment filename, or an empty string when Jira rejects it.
    """
    filename = path.name
    try:
        with path.open("rb") as handle:
            response = requests.post(
                f"{url}/rest/api/3/issue/{issue_key}/attachments",
                auth=(auth["username"], auth["password"]),
                headers={"X-Atlassian-Token": "no-check", "Accept": "application/json"},
                files={"file": (filename, handle, "text/markdown")},
                timeout=60,
            )
    except requests.RequestException as error:
        logger.error(
            f"Could not attach the verification report to {issue_key}: {error}"
        )
        return ""
    if response.status_code not in {200, 201}:
        logger.error(
            f"Jira returned HTTP {response.status_code} while attaching {filename} to {issue_key}"
        )
        return ""
    logger.info(f"Attached {filename} to {issue_key}")
    return filename


def _qa_contact(url, auth, issue_key):
    """
    Return the QA Contact user on the issue.

    Args:
        url (str): Jira site URL.
        auth (dict): username and password.
        issue_key (str): Jira issue key.

    Returns:
        dict: account_id and name. Empty when the field is unset.
    """
    try:
        response = requests.get(
            f"{url}/rest/api/3/issue/{issue_key}",
            auth=(auth["username"], auth["password"]),
            params={"fields": _QA_CONTACT_FIELD},
            headers={"Accept": "application/json"},
            timeout=60,
        )
    except requests.RequestException as error:
        logger.error(f"Could not read the QA Contact for {issue_key}: {error}")
        return {}
    if response.status_code != 200:
        logger.error(
            f"Jira returned HTTP {response.status_code} while reading the QA Contact for {issue_key}"
        )
        return {}
    user = (response.json().get("fields") or {}).get(_QA_CONTACT_FIELD) or {}
    if not isinstance(user, dict) or not user.get("accountId"):
        logger.info(f"QA Contact is empty on {issue_key}")
        return {}
    logger.info(
        f"Tagging QA Contact {user.get('displayName') or 'user'} on {issue_key}"
    )
    return {
        "account_id": str(user.get("accountId") or ""),
        "name": str(user.get("displayName") or ""),
    }


def _evidence_comment(issue_key, status, result, filename, report_text):
    """
    Build the verification comment.

    Args:
        issue_key (str): Jira issue key.
        status (str): passed or failed.
        result (dict): Execution result.
        filename (str): Attached report name. Empty when the report fits in the comment.
        report_text (str): Markdown report.

    Returns:
        str: Comment text.
    """
    outcome = "succeeded" if status == "passed" else "did not succeed"
    lines = [f"Verification of {issue_key} {outcome}."]
    cluster = str(result.get("cluster") or "").strip()
    openshift = str(result.get("openshift_version") or "").strip()
    odf = str(result.get("odf_version") or "").strip()
    if cluster:
        lines.append(f"Cluster: {cluster}")
    if openshift:
        lines.append(f"OpenShift version: {openshift}")
    if odf:
        lines.append(f"ODF version: {odf}")
    evidence = _evidence_lines(result)
    if evidence:
        lines.extend(["", "Evidence:"])
        lines.extend(evidence)
    reasons = [
        str(reason) for reason in (result.get("reasons") or []) if str(reason).strip()
    ]
    if status != "passed" and reasons:
        lines.extend(["", "Reasons:"])
        lines.extend(f"- {reason}" for reason in reasons)
    if filename:
        lines.extend(["", f"The full verification report is attached: {filename}"])
    elif report_text.strip():
        lines.extend(["", report_text.strip()])
    return "\n".join(lines)


def _evidence_lines(result):
    """
    Return a short command result for the Jira comment.

    Args:
        result (dict): Execution result.

    Returns:
        list: One or two lines per command, capped so the comment stays short.
    """
    lines = []
    for step in (result.get("steps") or [])[:8]:
        command = " ".join(str(step.get("command") or "").split())
        if not command:
            continue
        if len(command) > 180:
            command = command[:180] + "..."
        exit_code = step.get("exit_code")
        lines.append(f"- exit {exit_code}: {command}")
        excerpt = " ".join(str(step.get("output_excerpt") or "").split())
        if excerpt:
            lines.append(f"  {excerpt[:180]}")
    return lines


def _comment(url, auth, issue_key, text, contact):
    """
    Add the verification comment and mention the QA Contact.

    Args:
        url (str): Jira site URL.
        auth (dict): username and password.
        issue_key (str): Jira issue key.
        text (str): Comment text.
        contact (dict): account_id and name for the mention.
    """
    content = []
    account_id = str((contact or {}).get("account_id") or "")
    name = str((contact or {}).get("name") or "QA Contact")
    if account_id:
        content.append(
            {
                "type": "paragraph",
                "content": [
                    {
                        "type": "mention",
                        "attrs": {"id": account_id, "text": f"@{name}"},
                    }
                ],
            }
        )
    for paragraph in text.split("\n\n"):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        content.append(
            {
                "type": "paragraph",
                "content": [{"type": "text", "text": paragraph[:30000]}],
            }
        )
    try:
        response = requests.post(
            f"{url}/rest/api/3/issue/{issue_key}/comment",
            auth=(auth["username"], auth["password"]),
            json={"body": {"type": "doc", "version": 1, "content": content}},
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=60,
        )
    except requests.RequestException as error:
        logger.error(f"Could not comment on {issue_key}: {error}")
        return
    if response.status_code not in {200, 201}:
        logger.error(
            f"Jira returned HTTP {response.status_code} while commenting on {issue_key}"
        )
        return
    logger.info(f"Commented on {issue_key} with the verification result")


def _run_step(step, index, plan_steps):
    """
    Run one saved verification command and keep its output.

    Args:
        step (dict): Verification step.
        index (int): Step number when the plan omits one.
        plan_steps (list): All steps, used to bind a manifest.

    Returns:
        dict: Step result including the full output.
    """
    number = step.get("step") or index
    template = " ".join(str(step.get("command") or "").split())
    allowed, reason = command_allowed(template, [template])
    if not allowed:
        logger.warning(f"Refused cluster command: {reason}")
        record = _not_run(step, index)
        record["output_excerpt"] = reason
        record["output"] = reason
        return record
    from ocs_ci.agents.jira_verification.execute import _bind_manifest

    resolved, manifest_path = _bind_manifest(template, plan_steps)
    try:
        record = _exec(resolved)
    finally:
        if manifest_path and os.path.isfile(manifest_path):
            os.remove(manifest_path)
    record["step"] = number
    record["command"] = template
    return record


def _exec(command):
    """
    Run one oc command and keep its output.

    Args:
        command (str): Command to run.

    Returns:
        dict: Command result from the verification executor.
    """
    from ocs_ci.agents.jira_verification.execute import _run_exec

    return _run_exec(command)


def _health_row(name, record, passed, lines):
    """
    Return one health check for the report.

    Args:
        name (str): StorageCluster or CephCluster.
        record (dict): Command result.
        passed (bool): Whether the status is OK.
        lines (list): Short status lines.

    Returns:
        dict: Name, command, output, and passed.
    """
    output = (
        "\n".join(lines)
        if lines
        else (record.get("output") or record.get("output_excerpt") or "")
    )
    return {
        "name": name,
        "command": record.get("command") or "",
        "exit_code": record.get("exit_code"),
        "passed": passed,
        "output": output,
        "output_excerpt": record.get("output_excerpt") or "",
    }


def _storage_status(record):
    """
    Read StorageCluster phase and ODF version.

    Args:
        record (dict): oc get storagecluster result.

    Returns:
        tuple: Status lines, whether every phase is Ready, and the ODF version.
    """
    lines = []
    version = ""
    ready = record.get("exit_code") == 0
    for item in _items(record):
        name = _resource_name(item)
        status = item.get("status") or {}
        phase = str(status.get("phase") or "")
        item_version = str(status.get("version") or "")
        if item_version and not version:
            version = item_version
        lines.append(f"{name} phase={phase} version={item_version}")
        if phase != "Ready":
            ready = False
    if not lines:
        ready = False
    return lines, ready, version


def _ceph_status(record):
    """
    Read CephCluster health.

    Args:
        record (dict): oc get cephcluster result.

    Returns:
        tuple: Status lines, and whether every cluster is HEALTH_OK.
    """
    lines = []
    ready = record.get("exit_code") == 0
    for item in _items(record):
        name = _resource_name(item)
        status = item.get("status") or {}
        phase = str(status.get("phase") or "")
        health = str((status.get("ceph") or {}).get("health") or "")
        lines.append(f"{name} phase={phase} health={health}")
        if health != "HEALTH_OK":
            ready = False
    if not lines:
        ready = False
    return lines, ready


def _items(record):
    """
    Return Kubernetes objects from an oc JSON result.

    Args:
        record (dict): Command result.

    Returns:
        list: Resource objects. Empty when the command failed or is not JSON.
    """
    if record.get("exit_code") != 0:
        return []
    try:
        payload = json.loads(record.get("output") or "")
    except json.JSONDecodeError:
        return []
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        return [item for item in payload["items"] if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def _resource_name(item):
    """
    Return namespace/name for one resource.

    Args:
        item (dict): Kubernetes object.

    Returns:
        str: namespace/name, or the name alone.
    """
    meta = item.get("metadata") or {}
    name = str(meta.get("name") or "")
    namespace = str(meta.get("namespace") or "")
    if namespace and name:
        return f"{namespace}/{name}"
    return name or namespace


def _not_run(step, index):
    """
    Return a step result for a command that was not run.

    Args:
        step (dict): Verification step.
        index (int): Step number when the plan omits one.

    Returns:
        dict: Step result.
    """
    return {
        "step": step.get("step") or index,
        "command": " ".join(str(step.get("command") or "").split()),
        "exit_code": "",
        "passed": False,
        "output_excerpt": "not run",
        "output": "not run",
    }


def _fence(text):
    """
    Keep command output from closing a Markdown fence.

    Args:
        text (str): Command output.

    Returns:
        str: Output safe to place in a fence.
    """
    return str(text or "").replace("```", "'''")
