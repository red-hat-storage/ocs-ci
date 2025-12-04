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


def test_is_base64_block_valid_base64():
    """
    Check that _is_base64_block correctly identifies valid base64 strings.
    """
    # Valid base64 string (200 chars)
    valid_base64 = (
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
        * 2
    )
    assert utils._is_base64_block(valid_base64, min_length=100) is True


def test_is_base64_block_short_string():
    """
    Check that _is_base64_block rejects strings shorter than min_length.
    """
    short_base64 = "aGVsbG8="  # "hello" in base64, only 8 chars
    assert utils._is_base64_block(short_base64, min_length=100) is False


def test_is_base64_block_not_base64():
    """
    Check that _is_base64_block rejects non-base64 strings.
    """
    # Regular text with 200 chars
    regular_text = "This is just regular text without base64 encoding. " * 4
    assert utils._is_base64_block(regular_text, min_length=100) is False


def test_is_base64_block_empty_string():
    """
    Check that _is_base64_block handles empty strings correctly.
    """
    assert utils._is_base64_block("", min_length=100) is False
    assert utils._is_base64_block(None, min_length=100) is False


def test_truncate_large_base64_small_output():
    """
    Check that small outputs are returned unchanged (fast path).
    """
    small_output = "status: Running\nname: test-pod"
    result = utils._truncate_large_base64(small_output, max_base64_size=1024)
    assert result == small_output


def test_truncate_large_base64_single_large_block():
    """
    Check that large base64 block gets truncated.
    """
    # Create a large base64 block (2000 chars) - realistic multiline base64
    large_base64_line1 = "iVBORw0KGgoAAAANSUhEUgAAAA" * 40  # ~1200 chars
    large_base64_line2 = "AAAAASUVORK5CYII1234567890AB" * 40  # ~1200 chars
    output = f"image: myapp\n{large_base64_line1}\n{large_base64_line2}\nname: test"

    result = utils._truncate_large_base64(output, max_base64_size=1024)

    # Should contain truncation message
    assert "[BASE64_TRUNCATED:" in result
    # Should preserve non-base64 lines
    assert "image: myapp" in result
    assert "name: test" in result
    # Should NOT contain the large base64
    assert large_base64_line1 not in result
    assert large_base64_line2 not in result


def test_truncate_large_base64_small_block_preserved():
    """
    Check that small base64 blocks are preserved (might be secrets).
    """
    # Create a small base64 block (500 chars)
    small_base64 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" * 10  # ~360 chars
    output = f"token: {small_base64}\nstatus: active"

    result = utils._truncate_large_base64(output, max_base64_size=1024)

    # Small base64 should be preserved
    assert small_base64 in result
    assert "[BASE64_TRUNCATED:" not in result


def test_truncate_large_base64_multiline_block():
    """
    Check that multiline base64 blocks are detected and truncated.
    """
    # Simulate YAML with multiline base64 (like container images)
    line1 = "iVBORw0KGgoAAAANSUhEUgAAAA" * 30  # ~900 chars
    line2 = "AAAAASUVORK5CYII1234567890AB" * 30  # ~900 chars
    output = f"image: |\n  {line1}\n  {line2}\nname: test-pod"

    result = utils._truncate_large_base64(output, max_base64_size=1024)

    # Multiline block (~1800 chars) should be truncated
    assert "[BASE64_TRUNCATED:" in result
    # Non-base64 content preserved
    assert "name: test-pod" in result


def test_truncate_large_base64_no_base64():
    """
    Check that output without base64 is returned unchanged.
    """
    output = "status: Running\nready: 3/3\nage: 5d\nimage: nginx:latest"
    result = utils._truncate_large_base64(output, max_base64_size=1024)
    assert result == output


def test_truncate_large_base64_empty_output():
    """
    Check that empty output is handled correctly.
    """
    assert utils._truncate_large_base64("", max_base64_size=1024) == ""
    assert utils._truncate_large_base64(None, max_base64_size=1024) is None
