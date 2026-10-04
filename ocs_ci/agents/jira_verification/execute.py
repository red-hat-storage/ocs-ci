"""Run a saved verification report on a cluster and record the result."""

import asyncio
import logging
import os
import tempfile
from pathlib import Path

import yaml

from ocs_ci.agents.jenkins_api import JenkinsClient
from ocs_ci.agents.jira_verification.command_policy import command_allowed
from ocs_ci.agents.jira_verification.summary import summary_text
from ocs_ci.agents.jira_verification.report_store import (
    REPORTS_ROOT,
    write_execution_result,
    write_verification_markdown,
)
from ocs_ci.agents.jira_verification.run_steps import (
    attach_verification_report,
    render_verification_markdown,
    verify_with_claude,
)

logger = logging.getLogger(__name__)

_CLUSTER_TOOL = "mcp__cluster__run_cluster_command"
_EXCERPT_LIMIT = 400


def execute_saved_reports(request):
    """
    Execute each saved verification report named by the request.

    Args:
        request (dict): Value returned by build_request. args.issues and
            args.release select the plan files. args.cluster is the Jenkins
            CLUSTER_NAME whose kubeconfig is copied from the deploy build.
            A kubeconfig response of OK means the cluster is online.
            kubeconfig overrides that copy. args.dry_run skips the cluster
            and GitHub.

    Returns:
        dict: JSON-ready result with one entry per issue.

    Raises:
        ValueError: The release, issue, cluster, or plan file is missing.
    """
    args = request.get("args") or {}
    version = str(args.get("release") or "").strip()
    issues = [
        str(key).strip() for key in (args.get("issues") or []) if str(key).strip()
    ]
    if not version:
        raise ValueError("--release is required with --execute")
    if not issues:
        raise ValueError("--issue is required with --execute")
    dry_run = bool(args.get("dry_run"))
    kubeconfig = str(request.get("kubeconfig") or "").strip()
    cluster_name = str(args.get("cluster") or "").strip()
    if not dry_run and not kubeconfig and not cluster_name:
        raise ValueError("--cluster is required with --execute")
    if kubeconfig and not Path(kubeconfig).is_file():
        raise ValueError(f"kubeconfig was not found: {kubeconfig}")
    results = []
    for key in issues:
        results.append(
            _execute_one(
                version,
                key,
                kubeconfig,
                dry_run,
                str(args.get("cluster") or "").strip(),
            )
        )
    return {
        "ok": True,
        "agent": request.get("agent"),
        "execute": True,
        "dry_run": dry_run,
        "results": results,
    }


def _execute_one(version, issue_key, kubeconfig, dry_run, cluster_arg):
    """
    Execute one saved plan and write its result file.

    Args:
        version (str): Report directory name.
        issue_key (str): Jira issue key.
        kubeconfig (str): Local kubeconfig path. Empty when Jenkins supplies it.
        dry_run (bool): Skip the cluster and GitHub.
        cluster_arg (str): Jenkins CLUSTER_NAME from --cluster.

    Returns:
        dict: key, status, saved path, and github_issue.

    Raises:
        ValueError: The plan file is missing or unreadable.
    """
    plan_path = REPORTS_ROOT / version / f"{issue_key}.yaml"
    if not plan_path.is_file():
        raise ValueError(f"verification report was not found: {plan_path}")
    loaded = yaml.safe_load(plan_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"verification report is empty: {plan_path}")
    report = loaded
    report["key"] = str(report.get("key") or issue_key)
    plan_steps = [
        step
        for step in (report.get("verification_steps") or [])
        if isinstance(step, dict)
    ]
    cluster = cluster_arg or str(report.get("cluster") or "")
    copied = ""
    if dry_run:
        result = _stopped(
            cluster,
            "skipped",
            ["Dry run: cluster commands were not run and no GitHub issue was opened."],
            plan_steps,
            "not run",
        )
    else:
        if not kubeconfig:
            copied_kubeconfig = JenkinsClient().copy_kubeconfig(cluster)
            kubeconfig = copied_kubeconfig["path"]
            copied = kubeconfig
            kubeconfig_reason = copied_kubeconfig["reason"]
        else:
            kubeconfig_reason = ""
        try:
            reasons = _block_reasons(plan_steps, kubeconfig_reason)
            if reasons:
                result = _stopped(cluster, "blocked", reasons, plan_steps, "not run")
            else:
                _prepare_cluster(kubeconfig)
                result = verify_with_claude(report, kubeconfig, cluster)
                markdown = render_verification_markdown(report, result)
                report_path = write_verification_markdown(version, issue_key, markdown)
                result["verification_report"] = str(report_path)
                logger.info(f"Wrote verification report {report_path}")
                result["jira_attachment"] = attach_verification_report(
                    issue_key, report_path, result["status"], result
                )
                if result["status"] == "passed" and not report.get("tests"):
                    result["github_issue"] = _open_issue(report, result)
        finally:
            if copied and os.path.isfile(copied):
                os.remove(copied)
    path = write_execution_result(version, issue_key, result)
    logger.info(
        f"Wrote execution result {path} status {result['status']} for {issue_key}"
    )
    return {
        "key": issue_key,
        "status": result["status"],
        "saved": str(path),
        "github_issue": result.get("github_issue") or "",
        "verification_report": result.get("verification_report") or "",
    }


def _block_reasons(plan_steps, kubeconfig_reason):
    """
    Return reasons that stop execution before any cluster command.

    A kubeconfig response of OK means the cluster is online. The saved
    cluster_check, including agent offline, teardown, and platform, does not
    stop execution.

    Args:
        plan_steps (list): Verification steps.
        kubeconfig_reason (str): Why the Jenkins kubeconfig could not be copied.

    Returns:
        list: Reasons. Empty when the SDK may run.
    """
    reasons = []
    if kubeconfig_reason:
        reasons.append(kubeconfig_reason)
    if not any(str(step.get("command") or "").strip() for step in plan_steps):
        reasons.append("Every verification step has an empty command.")
    return reasons


def _stopped(cluster, status, reasons, plan_steps, excerpt):
    """
    Build a result that did not call the cluster.

    Args:
        cluster (str): Cluster name.
        status (str): skipped or blocked.
        reasons (list): Why execution stopped.
        plan_steps (list): Steps from the plan.
        excerpt (str): Text stored on each step.

    Returns:
        dict: Execution result.
    """
    steps = []
    for index, step in enumerate(plan_steps, start=1):
        steps.append(
            _step_record(
                step.get("step") or index,
                str(step.get("command") or ""),
                None,
                False,
                excerpt,
            )
        )
    for reason in reasons:
        logger.info(reason)
    return {"cluster": cluster, "status": status, "reasons": reasons, "steps": steps}


def _finished(cluster, plan_steps, executed):
    """
    Build the result from commands the tool actually ran.

    Args:
        cluster (str): Cluster context name.
        plan_steps (list): Steps from the plan.
        executed (list): Records from run_cluster_command.

    Returns:
        dict: Execution result with passed or failed status.
    """
    steps = _merge_steps(plan_steps, executed)
    reasons = []
    if not executed:
        reasons.append("No verification command was run.")
    for step in steps:
        if not step["passed"] and step["output_excerpt"]:
            reasons.append(f"Step {step['step']}: {step['output_excerpt']}")
    status = "passed" if steps and all(step["passed"] for step in steps) else "failed"
    if status == "passed":
        reasons = []
    return {
        "cluster": cluster,
        "status": status,
        "reasons": reasons,
        "steps": steps,
    }


def _merge_steps(plan_steps, executed):
    """
    Pair plan steps with the commands that ran.

    Args:
        plan_steps (list): Steps from the plan.
        executed (list): Records from run_cluster_command.

    Returns:
        list: Step results in plan order, then extra probes.
    """
    used = set()
    rows = []
    for index, step in enumerate(plan_steps, start=1):
        command = " ".join(str(step.get("command") or "").split())
        match = None
        for executed_index, item in enumerate(executed):
            if executed_index in used:
                continue
            if command and item["command"] == command:
                match = executed_index
                break
        number = step.get("step") or index
        if match is None:
            excerpt = (
                "command was not run"
                if command
                else "The verification step has no command."
            )
            rows.append(_step_record(number, command, None, False, excerpt))
            continue
        used.add(match)
        item = dict(executed[match])
        item["step"] = number
        rows.append(item)
    for executed_index, item in enumerate(executed):
        if executed_index not in used:
            rows.append(item)
    return rows


def _run_commands(report, plan_steps):
    """
    Ask the Agent SDK to run the report commands through exec_cmd.

    Args:
        report (dict): Verification plan.
        plan_steps (list): Verification steps.

    Returns:
        list: One record per tool call.

    Raises:
        RuntimeError: The Agent SDK is not installed.
    """
    try:
        from claude_agent_sdk import (
            ClaudeAgentOptions,
            ResultMessage,
            create_sdk_mcp_server,
            query,
            tool,
        )
    except ImportError as error:
        raise RuntimeError(
            "Cluster execution requires claude-agent-sdk in the agent environment."
        ) from error

    commands = [str(step.get("command") or "") for step in plan_steps]
    executed = []

    @tool(
        "run_cluster_command",
        "Run one allowed oc command on the verification cluster.",
        {"command": str},
    )
    async def run_cluster_command(args):
        command = str((args or {}).get("command") or "")
        allowed, reason = command_allowed(command, commands)
        if not allowed:
            logger.warning(f"Refused cluster command: {reason}")
            executed.append(_step_record("", command, None, False, reason))
            return {
                "content": [{"type": "text", "text": reason}],
                "is_error": True,
            }
        resolved, manifest_path = _bind_manifest(command, plan_steps)
        try:
            record = _run_exec(resolved)
        finally:
            if manifest_path and os.path.isfile(manifest_path):
                os.remove(manifest_path)
        record["command"] = " ".join(command.split())
        executed.append(record)
        text = f"exit {record['exit_code']}\n{record['output_excerpt']}"
        payload = {"content": [{"type": "text", "text": text}]}
        if not record["passed"]:
            payload["is_error"] = True
        return payload

    server = create_sdk_mcp_server("cluster", tools=[run_cluster_command])
    options = ClaudeAgentOptions(
        tools=[],
        allowed_tools=[_CLUSTER_TOOL],
        permission_mode="dontAsk",
        max_turns=max(4, 2 + len(plan_steps) * 2),
        cwd="/tmp",
        strict_mcp_config=True,
        mcp_servers={"cluster": server},
        system_prompt=_system_prompt(report),
    )

    async def _query():
        async for message in query(
            prompt=_user_prompt(report, plan_steps), options=options
        ):
            if isinstance(message, ResultMessage) and message.is_error:
                logger.error(f"Claude Agent SDK finished with {message.subtype}")

    asyncio.run(_query())
    return executed


def _bind_manifest(command, plan_steps):
    """
    Replace {{manifest}} with a private temp file for one report step.

    Args:
        command (str): Command the model requested.
        plan_steps (list): Verification steps, including any manifest YAML.

    Returns:
        tuple: The command to run, and the temp path to delete. The temp path
            is empty when the step has no manifest.
    """
    text = " ".join(str(command or "").split())
    for step in plan_steps or []:
        if not isinstance(step, dict):
            continue
        template = " ".join(str(step.get("command") or "").split())
        manifest = str(step.get("manifest") or "").strip()
        if text != template or not manifest or "{{manifest}}" not in template:
            continue
        handle = tempfile.NamedTemporaryFile(
            prefix="ocs-ci-verify-",
            suffix=".yaml",
            delete=False,
        )
        try:
            handle.write(manifest.encode())
        finally:
            handle.close()
        os.chmod(handle.name, 0o600)
        return template.replace("{{manifest}}", handle.name), handle.name
    return text, ""


def _run_exec(command):
    """
    Run one allowed command with the ocs-ci command helper.

    Args:
        command (str): Command that already passed the policy.

    Returns:
        dict: Step record with the exit code and a short excerpt.
    """
    from ocs_ci.utility.utils import exec_cmd

    logger.info(f"Running cluster command: {command}")
    try:
        completed = exec_cmd(
            command,
            ignore_error=True,
            timeout=600,
            cmd_log_level=logging.INFO,
        )
    except Exception as error:
        logger.error(f"Cluster command failed to start: {error}")
        return _step_record("", command, None, False, str(error))
    code = completed.returncode
    stdout = _output_text(completed.stdout)
    stderr = _output_text(completed.stderr)
    excerpt = _excerpt(stdout, stderr)
    logger.info(f"Cluster command exited {code}")
    record = _step_record("", " ".join(command.split()), code, code == 0, excerpt)
    record["output"] = "\n".join(part for part in (stdout, stderr) if part).rstrip()
    return record


def _prepare_cluster(kubeconfig):
    """
    Point ocs-ci at the kubeconfig used for this execution.

    Args:
        kubeconfig (str): Absolute or relative kubeconfig path.
    """
    from ocs_ci.framework import config
    from ocs_ci.framework.logger_factory import set_log_record_factory

    set_log_record_factory()
    config.RUN["kubeconfig"] = str(Path(kubeconfig).resolve())


def _open_issue(report, result):
    """
    Open the automation issue. A GitHub failure stays on the result.

    Args:
        report (dict): Verification plan.
        result (dict): Passed execution result.

    Returns:
        str: Issue URL, or an empty string.
    """
    from ocs_ci.agents.github_api import create_automation_issue

    try:
        return create_automation_issue(report, result)
    except ValueError as error:
        logger.error(str(error))
        result.setdefault("reasons", []).append(str(error))
        return ""


def _system_prompt(report):
    """
    Tell the model which commands it may request.

    Args:
        report (dict): Verification plan.

    Returns:
        str: System prompt.
    """
    key = report.get("key") or "the issue"
    return (
        f"You verify {key} on the cluster by calling run_cluster_command. "
        "Request each verification step command in order. "
        "You may also request a read-only probe: oc whoami, oc get, oc describe, "
        "oc logs, or oc adm top. "
        "Do not invent a command when a step has none. "
        "When the steps are done, reply with one JSON object "
        '{"status":"passed"} or {"status":"failed"} and a one sentence summary.'
    )


def _user_prompt(report, plan_steps):
    """
    Describe the verification steps to the model.

    Args:
        report (dict): Verification plan.
        plan_steps (list): Verification steps.

    Returns:
        str: User prompt.
    """
    lines = [
        summary_text(report.get("summary"))
        or str(report.get("bug_description") or "").strip(),
        "",
        "Verification steps:",
    ]
    for index, step in enumerate(plan_steps, start=1):
        number = step.get("step") or index
        action = str(step.get("action") or "").strip()
        command = str(step.get("command") or "").strip() or "(no command)"
        lines.append(f"{number}. {action} command: {command}")
    return "\n".join(lines)


def _step_record(number, command, exit_code, passed, excerpt):
    """
    Return one step result.

    Args:
        number: Step number from the plan, or empty for an extra probe.
        command (str): Command text.
        exit_code: Process exit code, or None when the command did not run.
        passed (bool): True when the command exited 0.
        excerpt (str): Short output or the reason it did not pass.

    Returns:
        dict: Step result.
    """
    return {
        "step": number,
        "command": " ".join(str(command or "").split()),
        "exit_code": exit_code,
        "passed": bool(passed),
        "output_excerpt": _excerpt(excerpt),
    }


def _output_text(value):
    """
    Decode command output.

    Args:
        value: stdout or stderr from exec_cmd.

    Returns:
        str: Text.
    """
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return "" if value is None else str(value)


def _excerpt(first, second=""):
    """
    Collapse command output to one short excerpt.

    Args:
        first (str): Primary text.
        second (str): Extra text appended when the primary text is empty.

    Returns:
        str: Excerpt of at most 400 characters.
    """
    text = " ".join(f"{first or ''} {second or ''}".split())
    if len(text) <= _EXCERPT_LIMIT:
        return text
    return text[:_EXCERPT_LIMIT] + "..."
