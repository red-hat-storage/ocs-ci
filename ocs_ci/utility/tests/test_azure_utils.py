# -*- coding: utf8 -*-


from unittest.mock import patch
import copy

import msrest.exceptions
import pytest

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


def test_azureutil_init_invalidcredentials():
    """
    Check that accessing credentials attribute raises azure auth exception when
    credentials passed via constructor are not valid.

    Note: This test case actually sends (invalid) requests to azure api.
    """
    az = azure.AZURE(
        subscription_id="8e011ec0-5d54-4401-a49e-89368783703e",
        tenant_id="68c4e710-fa36-4347-a8c7-41cee7c26205",
        client_id="743c4ede-ff6d-47e5-b065-0d52066f4dfd",
        client_secret="7a923b0f617623cb85c908021664dc44")
    with pytest.raises(msrest.exceptions.AuthenticationError):
        print(az.credentials)
