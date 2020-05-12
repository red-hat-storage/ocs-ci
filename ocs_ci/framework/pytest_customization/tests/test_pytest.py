# -*- coding: utf-8 -*-

import logging
import textwrap
from pytest import fixture

from ocs_ci import framework
from ocs_ci.framework.main import init_ocsci_conf

pytest_plugins = [
    'pytester',
]


logger = logging.getLogger(__name__)


@fixture(autouse=True)
def reset_config():
    framework.config.reset()


def test_pytest_works():
    logger.info("This is prove that pytest works")


def test_help_message(testdir):
    """
    Check that ``py.test --help`` output lists custom options of
    ocscilib pytest plugin.
    """
    testdir.makeconftest(textwrap.dedent("""
        pytest_plugins = ['ocs_ci.framework.pytest_customization.ocscilib']
    """))
    result = testdir.runpytest('--help')
    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines([
        '*--ocsci-conf*',
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


def test_config_parametrize(testdir, tmpdir):
    """
    Parametrization via config values, use case described in
    https://github.com/red-hat-storage/ocs-ci/pull/61#issuecomment-494866745
    """
    testdir.makeconftest(textwrap.dedent("""
        pytest_plugins = ['ocs_ci.framework.pytest_customization.ocscilib']
    """))
    # create a temporary pytest test module
    testdir.makepyfile(textwrap.dedent("""\
        import pytest

        from ocs_ci.framework import config as ocsci_config

        @pytest.mark.parametrize("item", ocsci_config.RUN)
        def test_demo_parametrized_config(item):
            assert item is not None
        """))
    # create config file
    conf_file = testdir.makefile(".yaml", textwrap.dedent("""\
        RUN:
          things:
            - 1
            - 2
        """))
    pytest_arguments = [
        "-v",
        f"--ocsci-conf={conf_file}",
        f"--cluster-path={tmpdir}",
        "--cluster-name=fake-cluster",
    ]
    # this is a bit hack which allow us init all the config which we do in
    # runner run_ocsci.py. Without this we won't be able to access config
    init_ocsci_conf(pytest_arguments)
    # run pytest with the following pytest_argumetns
    result = testdir.runpytest(*pytest_arguments)
    # Build a list of lines we expect to see in the output
    run_defaults = framework.config.get_defaults()['RUN']
    expected_items = list(run_defaults.keys()) + ['things']
    expected_lines = [f'collecting*collected {len(expected_items)} items']
    expected_lines.extend([
        f'*test_demo_parametrized_config?{key}? PASSED*'
        for key in expected_items
    ])
    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(expected_lines)
    # make sure that that we get a '0' exit code for the testsuite
    assert result.ret == 0
