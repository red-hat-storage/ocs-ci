"""
Post-hoc safety audit for commands executed during live debugging.

Defense-in-depth layer: even though the prompt instructs Claude to use
read-only commands, this module verifies that no destructive commands
were actually executed.
"""

import logging
import re

logger = logging.getLogger(__name__)

# oc subcommands that are read-only and safe
ALLOWED_OC_SUBCOMMANDS = frozenset({
    "get",
    "describe",
    "logs",
    "events",
    "version",
    "whoami",
    "api-resources",
    "api-versions",
    "status",
    "project",
    "projects",
    "auth",
    "config",
    "explain",
    "plugin",
})

# oc adm subcommands that are read-only
ALLOWED_OC_ADM_SUBCOMMANDS = frozenset({
    "top",
    "inspect",
})

# oc exec is allowed only for known diagnostic commands
ALLOWED_EXEC_COMMANDS = frozenset({
    "ceph",
    "rados",
    "rbd",
    "ceph-volume",
    "df",
    "cat",
    "ls",
    "ps",
    "env",
    "mount",
    "lsblk",
    "free",
    "uptime",
    "hostname",
    "nslookup",
    "dig",
    "curl",
    "ping",
    "ip",
    "ss",
    "netstat",
    "top",
})

# Commands that are always forbidden
FORBIDDEN_COMMANDS = frozenset({
    "rm",
    "rmdir",
    "mv",
    "dd",
    "mkfs",
    "fdisk",
    "parted",
    "shutdown",
    "reboot",
    "halt",
    "init",
    "systemctl",
    "kubectl",  # should use oc instead
})

# oc subcommands that mutate cluster state
DESTRUCTIVE_OC_SUBCOMMANDS = frozenset({
    "delete",
    "apply",
    "create",
    "patch",
    "edit",
    "scale",
    "drain",
    "cordon",
    "uncordon",
    "taint",
    "label",
    "annotate",
    "replace",
    "set",
    "rollout",
    "debug",
    "run",
    "expose",
    "new-app",
    "new-project",
    "start-build",
    "cancel-build",
    "import-image",
    "tag",
    "process",
})


def audit_commands(commands):
    """
    Audit a list of shell commands for safety violations.

    Args:
        commands: List of command strings that were executed.

    Returns:
        List of violation description strings. Empty list means all safe.
    """
    violations = []

    for cmd in commands:
        cmd_stripped = cmd.strip()
        if not cmd_stripped:
            continue

        # Split into tokens for analysis
        tokens = cmd_stripped.split()
        if not tokens:
            continue

        base_cmd = tokens[0]

        # Check for piped commands -- audit each segment
        if "|" in cmd_stripped:
            segments = cmd_stripped.split("|")
            for segment in segments:
                segment_violations = audit_commands([segment.strip()])
                violations.extend(segment_violations)
            continue

        # Check forbidden base commands
        if base_cmd in FORBIDDEN_COMMANDS:
            violations.append(f"Forbidden command: {cmd_stripped}")
            continue

        # Check oc commands
        if base_cmd == "oc" and len(tokens) > 1:
            subcmd = tokens[1]

            # oc adm <subcmd>
            if subcmd == "adm" and len(tokens) > 2:
                adm_subcmd = tokens[2]
                if adm_subcmd not in ALLOWED_OC_ADM_SUBCOMMANDS:
                    violations.append(
                        f"Forbidden oc adm subcommand: {cmd_stripped}"
                    )
                continue

            # oc exec -- check the command being executed
            if subcmd == "exec":
                violation = _audit_oc_exec(cmd_stripped, tokens)
                if violation:
                    violations.append(violation)
                continue

            # Check for destructive oc subcommands
            if subcmd in DESTRUCTIVE_OC_SUBCOMMANDS:
                violations.append(
                    f"Destructive oc command: {cmd_stripped}"
                )
                continue

            # Check that the subcommand is in the allow list
            if subcmd not in ALLOWED_OC_SUBCOMMANDS:
                # Not in allow list but also not destructive -- log a warning
                logger.warning(
                    f"Unknown oc subcommand (not in allow list): {subcmd}"
                )

    return violations


def _audit_oc_exec(full_cmd, tokens):
    """
    Audit an ``oc exec`` command to ensure it only runs diagnostic commands.

    Returns:
        A violation string if unsafe, or None if safe.
    """
    # Find the command after "--"
    try:
        dash_idx = tokens.index("--")
        exec_tokens = tokens[dash_idx + 1:]
    except ValueError:
        # No "--" separator; the command may be inline
        # This is unusual but not necessarily dangerous
        return None

    if not exec_tokens:
        return None

    exec_cmd = exec_tokens[0]
    if exec_cmd not in ALLOWED_EXEC_COMMANDS:
        return f"Forbidden exec command '{exec_cmd}' in: {full_cmd}"

    return None
