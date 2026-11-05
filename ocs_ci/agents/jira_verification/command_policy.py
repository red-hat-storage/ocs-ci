"""Decide which cluster commands the verification agent may run."""

import re

_SHELL = re.compile(r"[;&|`$<>\n]|\$\(")
_READ_ONLY = re.compile(r"^oc\s+(whoami|get|describe|logs|adm\s+top)\b")


def command_allowed(command, report_commands):
    """
    Return whether one command may be passed to exec_cmd.

    An exact verification-step command is allowed. A read-only oc probe is
    allowed. Shell syntax is refused, and oc delete, apply, create, and patch
    are refused unless the report step is that exact command.

    Args:
        command (str): Command the model requested.
        report_commands (list): Command strings copied from the verification
            report.

    Returns:
        tuple: True and an empty reason when the command may run. False and a
            short reason when it is refused.
    """
    text = _normalize(command)
    if not text:
        return False, "The command is empty."
    if _SHELL.search(text):
        return False, "Shell syntax is not allowed."
    allowed = {_normalize(item) for item in report_commands or []}
    allowed.discard("")
    if text in allowed:
        return True, ""
    if _READ_ONLY.match(text):
        return True, ""
    return False, "The command is not a verification step or a read-only oc probe."


def _normalize(command):
    """
    Collapse whitespace so an exact step command can be compared.

    Args:
        command (str): Raw command.

    Returns:
        str: Single-spaced command.
    """
    return " ".join(str(command or "").split())
