# -*- coding: utf-8 -*-

import logging
import textwrap

from run_ocsci import init_ocsci_conf

pytest_plugins = [
    'pytester',
]


logger = logging.getLogger(__name__)


def test_pytest_works():
    logger.info("This is prove that pytest works")


def test_help_message(testdir):
    """
    Check that ``py.test --help`` output lists custom options of
    ocscilib pytest plugin.
    """
    testdir.makeconftest(textwrap.dedent("""
        pytest_plugins = ['ocsci.pytest_customization.ocscilib']
    """))
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


def test_config_parametrize(testdir):
    """
    Parametrization via config values, use case described in
    https://github.com/red-hat-storage/ocs-ci/pull/61#issuecomment-494866745
    """
    testdir.makeconftest(textwrap.dedent("""
        pytest_plugins = ['ocsci.pytest_customization.ocscilib']
    """))
    # create a temporary pytest test module
    testdir.makepyfile(textwrap.dedent("""\
        import pytest

        from ocsci import config as ocsci_config

        @pytest.mark.parametrize("item", ocsci_config.DEMO)
        def test_demo_parametrized_config(item):
            assert item is not None
        """))
    # create config file
    conf_file = testdir.makefile(".yaml", textwrap.dedent("""\
        DEMO:
         - 1
         - 2
        """))
    pytest_arguments = ['-v', f'--ocsci-conf={conf_file}']
    # this is a bit hack which allow us init all the config which we do in
    # runner run_ocsci.py. Without this we won't be able to access config
    init_ocsci_conf(pytest_arguments)
    # run pytest with the following pytest_argumetns
    result = testdir.runpytest(*pytest_arguments)
    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines([
        'collecting*collected 2 items',
        '*test_demo_parametrized_config?1? PASSED*',
        '*test_demo_parametrized_config?2? PASSED*',
    ])
    # make sure that that we get a '0' exit code for the testsuite
    assert result.ret == 0
