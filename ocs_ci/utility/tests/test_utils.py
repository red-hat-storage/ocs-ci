# -*- coding: utf8 -*-

import logging
from itertools import repeat
from sys import platform

import pytest

from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import utils, version


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
    assert caplog.records[0].levelname == "INFO"
    assert caplog.records[0].message == f"Executing command: {cmd}"
    assert caplog.records[1].levelname == "DEBUG"
    assert caplog.records[1].message == "Command stdout: hello"
    assert caplog.records[2].levelname == "DEBUG"
    assert caplog.records[2].message == "Command stderr is empty"
    assert caplog.records[3].levelname == "DEBUG"
    assert caplog.records[3].message == "Command return code: 0"


def test_run_cmd_simple_positive_with_secrets(caplog):
    """
    Check simple positive use case for run_cmd, including logging,
    when secrets are specified.
    """
    caplog.set_level(logging.DEBUG)
    secrets = ["8bca8d2e-1cd6", "683c08d7-bc07"]
    cmd = "echo -n hello 8bca8d2e-1cd6"
    assert utils.run_cmd(cmd, secrets=secrets) == "hello *****"
    # check that logs were satinized as well
    for secret in secrets:
        assert secret not in caplog.text


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
    assert caplog.records[0].levelname == "INFO"
    assert caplog.records[0].message == f"Executing command: {cmd}"
    assert caplog.records[1].levelname == "DEBUG"
    assert caplog.records[1].message == "Command stdout is empty"
    assert caplog.records[2].levelname == "WARNING"
    assert caplog.records[2].message.startswith("Command stderr: ls:")
    assert "No such file or directory" in caplog.records[2].message
    assert caplog.records[3].levelname == "DEBUG"
    return_code = 1 if platform == "darwin" else 2
    assert caplog.records[3].message == f"Command return code: {return_code}"


def test_run_cmd_simple_negative_with_secrets(caplog):
    """
    Check simple negative use case for run_cmd, including logging,
    when secrets are specified.
    """
    caplog.set_level(logging.DEBUG)
    secrets = ["8bca8d2e-1cd6", "683c08d7-bc07"]
    cmd = "ls /tmp/this/file/683c08d7-bc07/isnotthere"
    with pytest.raises(CommandFailed) as excinfo:
        utils.run_cmd(cmd, secrets=secrets)
        assert "No such file or directory" in str(excinfo.value)
        # check that exception was sanitized
        for secret in secrets:
            assert secret not in str(excinfo.value)
    # check that logs were satinized as well
    for secret in secrets:
        assert secret not in caplog.text


def test_run_cmd_simple_negative_ignoreerror(caplog):
    """
    Check simple negative use case for run_cmd with ignore error and logging.
    """
    caplog.set_level(logging.DEBUG)
    cmd = "ls /tmp/this/file/isindeednotthereatall"
    assert utils.run_cmd(cmd, ignore_error=True) == ""
    # check that run_cmd logged the run as expected
    assert caplog.records[0].levelname == "INFO"
    assert caplog.records[0].message == f"Executing command: {cmd}"
    assert caplog.records[1].levelname == "DEBUG"
    assert caplog.records[1].message == "Command stdout is empty"
    assert caplog.records[2].levelname == "WARNING"
    assert caplog.records[2].message.startswith("Command stderr: ls:")
    assert "No such file or directory" in caplog.records[2].message
    assert caplog.records[3].levelname == "DEBUG"
    return_code = 1 if platform == "darwin" else 2
    assert caplog.records[3].message == f"Command return code: {return_code}"


class A:
    def __init__(self, amount):
        self.num = amount
        if amount > 0:
            self.sub_attr = A(amount - 1)


@pytest.mark.parametrize("chain_length", [1, 2, 3, 4, 5])
def test_get_attr_chain(chain_length):
    attr_chain = ".".join(repeat("sub_attr", chain_length))
    sub_attr = utils.get_attr_chain(A(chain_length), attr_chain)
    assert sub_attr.num == 0


@pytest.mark.parametrize("chain_length", [1, 2, 3, 4, 5])
def test_get_nonexistent_attr_chain(chain_length):
    attr_chain = ".".join(repeat("sub_attr", chain_length + 1))
    sub_attr = utils.get_attr_chain(A(chain_length), attr_chain)
    assert sub_attr is None


def test_get_none_obj_attr():
    assert utils.get_attr_chain(None, "attribute") is None


def test_get_empty_attr():
    assert utils.get_attr_chain(A(1), "") is None


class B:
    def __init__(self):
        pass

    def __str__(self):
        return "B-object"


@pytest.mark.parametrize(
    "data_to_filter,expected_output",
    [
        ({"a": "A", "b": "B"}, {"a": "A", "b": "B"}),
        ({"v": version.VERSION_4_12}, {"v": "4.12"}),
        ({"o": B()}, {"o": "B-object"}),
        ({"t": [1, 2, B()]}, {"t": [1, 2, "B-object"]}),
        ({"t": tuple([1, 2, B()])}, {"t": [1, 2, "B-object"]}),
        ([1, 2, B()], [1, 2, "B-object"]),
        (tuple([1, 2, B()]), [1, 2, "B-object"]),
        ([{"a": "b"}, 1, 2, 3], [{"a": "b"}, 1, 2, 3]),
        ([{"a": "b"}, [B(), B()], 2, 3], [{"a": "b"}, ["B-object", "B-object"], 2, 3]),
        (
            [{"a": "b"}, [{"o": B()}, {"p": B()}], 2, 3],
            [{"a": "b"}, [{"o": "B-object"}, {"p": "B-object"}], 2, 3],
        ),
    ],
)
def test_filter_unrepresentable_values(data_to_filter, expected_output):
    assert utils.filter_unrepresentable_values(data_to_filter) == expected_output
