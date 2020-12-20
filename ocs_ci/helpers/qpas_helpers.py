"""
Helpers function for Performance and Scale test.
Functions here are tune to use as minimum as possible of memory.
functions can be used from pytest scrips or from regular python scripts.
"""
import logging
import subprocess

log = logging.getLogger(__name__)


def run_command(cmd, timeout=600, out_format="string", **kwargs):
    """
    Running command on the OS and return the STDOUT & STDERR outputs
    in case of argument is not string or list, return error message

    Args:
        cmd (str/list): the command to execute
        timeout (int): the command timeout in seconds, default is 10 Min.
        out_format (str): in which format to return the output -
            string - one long string separated with '\n' between lines
            list - list of lines
        kwargs (dict): dictionary of argument as subprocess get, with some
            specific arguments:

    Returns:
        list/str : all STDOUT / STDERR output as list of lines,
            or one string separated by '\n'

    """
    if isinstance(cmd, str):
        command = cmd.split()
    elif isinstance(cmd, list):
        command = cmd
    else:
        return "Error in command"

    for key in ["stdout", "stderr", "stdin"]:
        kwargs[key] = subprocess.PIPE

    if "out_format" in kwargs:
        out_format = kwargs["out_format"]
        del kwargs["out_format"]

    log.info(f"Going to format output as {out_format}")
    log.info(f"Going to run {cmd} with timeout of {timeout}")
    cp = subprocess.run(command, timeout=timeout, **kwargs)
    output = cp.stdout.decode()
    err = cp.stderr.decode()
    # exit code is not zero
    if cp.returncode:
        log.error(f"Command finished with non zero ({cp.returncode}): {err}")
        output += f"Error in command ({cp.returncode}): {err}"

    # TODO: adding more output_format types : json / yaml

    if out_format == "list":
        output = output.split("\n")  # convert output to list
        output.pop()  # remove last empty element from the list
        return output
