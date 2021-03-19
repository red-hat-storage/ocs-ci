# -*- coding: utf8 -*-


import logging

import pytest

from ocs_ci.framework.testlib import libtest
from ocs_ci.ocs import node
from ocs_ci.ocs.ocp import exec_oc_debug_cmd
from ocs_ci.ocs.exceptions import CommandFailed


logger = logging.getLogger(__name__)


@pytest.fixture
def node_names():
    """
    Returns list of all node names of the OCP cluster.
    """
    node_names = node.get_all_nodes()
    assert len(node_names) > 0, "tests case can't continue without nodes"
    return node_names


@libtest
def test_exec_oc_debug_cmd_simple(node_names):
    """
    Check that it's possible to run a command on given node.
    """
    stdout = exec_oc_debug_cmd(node_names[0], "uname")
    assert stdout == "Linux\n"


@libtest
def test_exec_oc_debug_cmd_cmdfail(node_names):
    """
    Exception is raised when command fails.
    """
    with pytest.raises(CommandFailed):
        exec_oc_debug_cmd(node_names[0], "ls /etc/foobar")


@libtest
def test_exec_oc_debug_cmd_invalidnode():
    """
    Exception is raised when node doesn't exist.
    """
    with pytest.raises(CommandFailed):
        exec_oc_debug_cmd("foo-47d3c46df8e98b4941e4878f49cc9ef7", "uname")
