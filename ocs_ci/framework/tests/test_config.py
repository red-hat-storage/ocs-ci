# -*- coding: utf-8 -*-
from pytest import fixture

from ocs_ci import framework


class TestConfig(object):
    @fixture(autouse=True)
    def reset_config(self):
        framework.config.reset()

    def test_defaults(self):
        config_sections = framework.config.to_dict().keys()
        for section_name in config_sections:
            section = getattr(framework.config, section_name)
            assert section == framework.config.get_defaults()[section_name]

    def test_defaults_specific(self):
        assert framework.config.ENV_DATA["cluster_namespace"] == "openshift-storage"
        assert framework.config.REPORTING["email"]["address"] == "ocs-ci@redhat.com"
        assert framework.config.RUN["bin_dir"] == "./bin"

    def test_custom_conf(self):
        user_dict = dict(
            REPORTING=dict(email="unit@test.com"),
            RUN=dict(log_dir="/dev/null"),
        )
        framework.config.update(user_dict)
        assert framework.config.REPORTING["email"] == "unit@test.com"
        assert framework.config.RUN["log_dir"] == "/dev/null"
        default_bin_dir = framework.config.get_defaults()["RUN"]["bin_dir"]
        assert framework.config.RUN["bin_dir"] == default_bin_dir

    def test_layered_conf(self):
        orig_client = framework.config.get_defaults()["RUN"]["client_version"]
        assert framework.config.RUN["client_version"] == orig_client
        first = dict(RUN=dict(client_version="1"))
        framework.config.update(first)
        assert framework.config.RUN["client_version"] == "1"
        second = dict(
            RUN=dict(client_version="2"),
            DEPLOYMENT=dict(installer_version="1"),
        )
        framework.config.update(second)
        assert framework.config.RUN["client_version"] == "2"
        assert framework.config.DEPLOYMENT["installer_version"] == "1"

    def test_custom_conf_multicluster(self):
        framework.config.nclusters = 2
        framework.config.init_cluster_configs()
        framework.config.switch_ctx(0)
        user_dict1 = dict(
            REPORTING=dict(email="cluster1@test.com"),
            RUN=dict(log_dir="/dev/null1"),
        )
        user_dict2 = dict(
            REPORTING=dict(email="cluster2@test.com"),
            RUN=dict(log_dir="/dev/null2"),
        )
        framework.config.update(user_dict1)
        assert framework.config.REPORTING["email"] == "cluster1@test.com"
        assert framework.config.RUN["log_dir"] == "/dev/null1"
        framework.config.switch_ctx(1)
        framework.config.update(user_dict2)
        assert framework.config.REPORTING["email"] == "cluster2@test.com"
        assert framework.config.RUN["log_dir"] == "/dev/null2"
        framework.config.reset_ctx()

    def test_multicluster_ctx_switch(self):
        framework.config.nclusters = 3
        framework.config.init_cluster_configs()
        user_dict = [
            dict(
                REPORTING=dict(email="USER1@CLUSTER1.com"),
                ENV_DATA=dict(cluster_name="cluster1"),
            ),
            dict(
                REPORTING=dict(email="USER1@CLUSTER2.com"),
                ENV_DATA=dict(cluster_name="cluster2"),
            ),
            dict(
                REPORTING=dict(email="USER1@CLUSTER3.com"),
                ENV_DATA=dict(cluster_name="cluster3"),
            ),
        ]

        for i in range(framework.config.nclusters):
            framework.config.switch_ctx(i)
            framework.config.update(user_dict[i])

        for i in range(framework.config.nclusters):
            framework.config.switch_ctx(i)
            assert (
                framework.config.REPORTING["email"]
                == user_dict[i]["REPORTING"]["email"]
            )
            assert (
                framework.config.ENV_DATA["cluster_name"]
                == user_dict[i]["ENV_DATA"]["cluster_name"]
            )
        framework.config.reset_ctx()


class TestMergeDict:
    def test_merge_dict(self):
        objA = dict(
            a_dict=dict(
                another_list=[],
                another_string="greetings",
            ),
            a_list=[0, 1, 2],
        )
        objB = dict(
            a_dict=dict(
                a_third_string="salutations",
            ),
            a_list=[1, 2, 3],
            a_string="string",
        )
        expected = dict(
            a_dict=dict(
                another_list=[],
                another_string="greetings",
                a_third_string="salutations",
            ),
            a_list=[1, 2, 3],
            a_string="string",
        )
        result = framework.merge_dict(objA, objB)
        assert objA is result
        assert result == expected
