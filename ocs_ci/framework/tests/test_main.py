# -*- coding: utf-8 -*-
import os
import subprocess
from pytest import fixture
from unittest import mock

from ocs_ci.framework import config, main

pytest_plugins = [
    'pytester',
]


class TestEntrypoint(object):
    @fixture(autouse=True)
    def reset_config(self):
        config.reset()

    def test_help(self):
        result = subprocess.check_output(
            ['run-ci', '--help'],
            stderr=subprocess.STDOUT,
        ).decode()
        assert '--ocsci-conf' in result
        assert '--cluster-conf' in result
        assert '--cluster-path' in result
        assert '--cluster-name' in result

    @mock.patch('ocs_ci.framework.main.pytest.main')
    @mock.patch.object(config, 'update')
    def test_no_args(self, config_update, pytest_main, testdir):
        main.main([])
        assert config_update.call_count == 0

    @mock.patch('ocs_ci.framework.main.pytest.main')
    @mock.patch.object(config, 'update')
    def test_config_passing(self, config_update, pytest_main, testdir):
        tempdir = testdir.makefile(
            '.yaml',
            ocsci_conf='RUN: null',
            cluster_conf='DEPLOYMENT: null',
        ).dirname
        main.main([
            '--ocsci-conf', os.path.join(tempdir, 'ocsci_conf.yaml'),
            '--cluster-conf', os.path.join(tempdir, 'cluster_conf.yaml'),
        ])
        assert config_update.call_args_list == [
            mock.call(dict(RUN=None)),
            mock.call(dict(DEPLOYMENT=None)),
        ]
