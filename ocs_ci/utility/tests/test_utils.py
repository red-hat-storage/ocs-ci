# -*- coding: utf8 -*-

import logging

import pytest

from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import utils


def test_mask_secret_null():
    """
    Checking that mask_secret function works with empty arguments.
    """
    assert utils.mask_secrets("", None) == ""


def test_mask_secret_nosecrets():
    """
    Checking that mask_secret function doesn't change plaintext when secrets
    are not specified.
    """
    assert utils.mask_secrets("ls -lh /tmp", None) == "ls -lh /tmp"


def test_mask_secret_nomatch():
    """
    Checking that mask_secret function works when there is no match.
    """
    secrets = [
        "8bca8d2e-1cd6-4ec0-8e55-9614aa01cf88",
        "683c08d7-bc07-4d72-b098-46ef00b74aec",
    ]
    assert utils.mask_secrets("ls -lh /tmp", secrets) == "ls -lh /tmp"


def test_mask_secret_simple_positive():
    """
    Checking that mask_secret function works in a simple positive case.
    """
    secrets = ["8bca8d2e-1cd6", "683c08d7-bc07"]
    cmd = "ls -lh /tmp/8bca8d2e /tmp/683c08d7-bc07 /1cd6-4ec0-8e55"
    cmd_masked_expected = "ls -lh /tmp/8bca8d2e /tmp/***** /1cd6-4ec0-8e55"
    assert utils.mask_secrets(cmd, secrets) == cmd_masked_expected


def test_run_cmd_simple_positive(caplog):
    """
    Check simple positive use case for run_cmd, including logging.
    """
    caplog.set_level(logging.DEBUG)
    cmd = "echo -n hello"
    assert utils.run_cmd(cmd) == "hello"
    # check that run_cmd logged the run as expected
    assert caplog.records[0].levelname == 'INFO'
    assert caplog.records[0].message == f'Executing command: {cmd}'
    assert caplog.records[1].levelname == 'DEBUG'
    assert caplog.records[1].message == 'Command stdout: hello'
    assert caplog.records[2].levelname == 'DEBUG'
    assert caplog.records[2].message == 'Command stderr is empty'
    assert caplog.records[3].levelname == 'DEBUG'
    assert caplog.records[3].message == 'Command return code: 0'


def test_run_cmd_simple_negative(caplog):
    """
    Check simple negative use case for run_cmd, including logging.
    """
    caplog.set_level(logging.DEBUG)
    cmd = "ls /tmp/this/file/isindeednotthereatall"
    with pytest.raises(CommandFailed) as excinfo:
        utils.run_cmd(cmd)
        assert "No such file or directory" in str(excinfo.value)
    # check that run_cmd logged the run as expected
    assert caplog.records[0].levelname == 'INFO'
    assert caplog.records[0].message == f'Executing command: {cmd}'
    assert caplog.records[1].levelname == 'DEBUG'
    assert caplog.records[1].message == 'Command stdout is empty'
    assert caplog.records[2].levelname == 'WARNING'
    assert caplog.records[2].message == (
        "Command stderr: "
        "ls: cannot access '/tmp/this/file/isindeednotthereatall': "
        "No such file or directory\n")
    assert caplog.records[3].levelname == 'DEBUG'
    assert caplog.records[3].message == 'Command return code: 2'


def test_run_cmd_simple_negative_ignoreerror(caplog):
    """
    Check simple negative use case for run_cmd with ignore error and logging.
    """
    caplog.set_level(logging.DEBUG)
    cmd = "ls /tmp/this/file/isindeednotthereatall"
    assert utils.run_cmd(cmd, ignore_error=True) == ""
    # check that run_cmd logged the run as expected
    assert caplog.records[0].levelname == 'INFO'
    assert caplog.records[0].message == f'Executing command: {cmd}'
    assert caplog.records[1].levelname == 'DEBUG'
    assert caplog.records[1].message == 'Command stdout is empty'
    assert caplog.records[2].levelname == 'WARNING'
    assert caplog.records[2].message == (
        "Command stderr: "
        "ls: cannot access '/tmp/this/file/isindeednotthereatall': "
        "No such file or directory\n")
    assert caplog.records[3].levelname == 'DEBUG'
    assert caplog.records[3].message == 'Command return code: 2'
