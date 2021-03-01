# -*- coding: utf8 -*-


from unittest.mock import patch
import copy

from ocs_ci.framework import config
import ocs_ci.utility.azure_utils as azure


TEST_RG = "354e510d-df1a-4b74-a741-e894eb6a1b1d"


def test_azureutil_init_noresourcegroup():
    """
    Check that resource group of azure util object is None when this is not
    specified anywhere.
    """
    az = azure.AZURE()
    assert az.cluster_resource_group is None


def test_azureutil_init_resourcegroup():
    """
    Check that resource group of azure util object can be initialized.
    """
    az = azure.AZURE(cluster_resource_group=TEST_RG)
    assert az.cluster_resource_group == TEST_RG


def test_azureutil_init_resourcegroup_config():
    """
    Check that default resource group of azure util object could be redefined
    via ocsci config.
    """
    TEST_ENV_DATA = copy.deepcopy(config.ENV_DATA)
    TEST_ENV_DATA["azure_cluster_resource_group"] = TEST_RG
    with patch("ocs_ci.framework.config.ENV_DATA", TEST_ENV_DATA):
        az = azure.AZURE()
        assert az.cluster_resource_group == TEST_RG
        # but the config can specify only the default value
        another = azure.AZURE(cluster_resource_group="something_else")
        assert another.cluster_resource_group == "something_else"
