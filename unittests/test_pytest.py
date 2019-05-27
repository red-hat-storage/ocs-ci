# -*- coding: utf-8 -*-

import logging

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
