# -*- coding: utf-8 -*-

import logging
import textwrap

import pytest


logger = logging.getLogger(__name__)


def test_pytest_works():
    logger.info("This is prove that pytest works")


def test_help_message(testdir):
    """
    Check that ``py.test --help`` output lists custom options of
    ocscilib pytest plugin.
    """
    result = testdir.runpytest('--help')
    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines([
        '*--ocsci-conf*',
        '*--cluster-conf*',
        '*--cluster-path*',
        '*--cluster-name*',
        ])


def test_simple(testdir):
    """
    Make sure that  pytest itself is not broken by running very simple test.
    """
    # create a temporary pytest test module
    testdir.makepyfile(textwrap.dedent("""\
        def test_foo():
            assert 1 == 1
        """))
    # run pytest with the following cmd args
    result = testdir.runpytest('-v')
    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(['*::test_foo PASSED*'])
    # make sure that that we get a '0' exit code for the testsuite
    assert result.ret == 0
