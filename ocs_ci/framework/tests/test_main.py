# -*- coding: utf-8 -*-
import os
import subprocess
from pytest import fixture, raises
from unittest import mock

from ocs_ci.framework import config, main

pytest_plugins = [
    "pytester",
]


class TestEntrypoint(object):
    @fixture(autouse=True)
    def reset_config(self):
        config.reset()

    def test_help(self):
        result = subprocess.check_output(
            ["run-ci", "--help"],
            stderr=subprocess.STDOUT,
        ).decode()
        assert "--ocsci-conf" in result
        assert "--cluster-path" in result
        assert "--cluster-name" in result

    def test_multicluster_help(self):
        result = subprocess.check_output(
            ["run-ci", "multicluster", "--help"],
            stderr=subprocess.STDOUT,
        ).decode()
        assert "nclusters" in result
        assert "--cluster" in result

    @mock.patch("ocs_ci.framework.main.pytest.main")
    @mock.patch.object(config, "update", wraps=config.update)
    def test_no_args(self, config_update, pytest_main, testdir):
        main.main([])
        assert config_update.call_count == 2

    @mock.patch("ocs_ci.framework.main.pytest.main")
    @mock.patch.object(config, "update", wraps=config.update)
    def test_config_passing(self, config_update, pytest_main, testdir):
        tempdir = testdir.makefile(
            ".yaml",
            ocsci_conf="RUN: null",
        ).dirname
        main.main(
            [
                "--ocsci-conf",
                os.path.join(tempdir, "ocsci_conf.yaml"),
            ]
        )
        assert config_update.call_args_list[0] == mock.call(dict(RUN=None))

    @mock.patch("ocs_ci.framework.main.pytest.main")
    @mock.patch.object(config, "update", wraps=config.update)
    def test_multi_config_passing(self, config_update, pytest_main, testdir):
        tempdir = testdir.makefile(
            ".yaml",
            ocsci_conf1="RUN: null",
            ocsci_conf2="TEST_SECTION: null",
        ).dirname
        with raises(ValueError):
            main.main(
                [
                    "--ocsci-conf",
                    f"{os.path.join(tempdir, 'ocsci_conf1.yaml')}",
                    "--ocsci-conf",
                    f"{os.path.join(tempdir, 'ocsci_conf2.yaml')}",
                ]
            )
        assert config_update.call_args_list == [
            mock.call(dict(RUN=None)),
            mock.call(dict(TEST_SECTION=None)),
        ]
