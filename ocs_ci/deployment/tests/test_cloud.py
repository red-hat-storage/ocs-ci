# -*- coding: utf8 -*-

from unittest.mock import patch
import copy

import pytest

from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import InvalidDeploymentPlatfrom


def test_clouddeploymentbase_init(clusterdir):
    """
    Test that creation of CloudDeploymentBase object without any constructor
    agruments works (object is created, no exceptions are raised) and it's
    cluster name is loaded from cluster dir properly.
    """
    cloud = CloudDeploymentBase()
    assert cloud.cluster_name == clusterdir['clusterName']


def test_clouddeploymentbase_init_withname(clusterdir):
    """
    Test that creation of CloudDeploymentBase object without any constructor
    agruments works when a cluster name is reconfigured via ocs-ci config.
    """
    TEST_ENV_DATA = copy.deepcopy(config.ENV_DATA)
    TEST_ENV_DATA["cluster_name"] = "another-cluster"
    with patch("ocs_ci.framework.config.ENV_DATA", TEST_ENV_DATA):
        cloud = CloudDeploymentBase()
        assert cloud.cluster_name == TEST_ENV_DATA["cluster_name"]


def test_clouddeploymentbase_has_no_default_storageclass(clusterdir):
    """
    Check assumption that cloud base class has the default storageclass
    attributed, but it's not defined.
    """
    cloud = CloudDeploymentBase()
    assert cloud.DEFAULT_STORAGECLASS is None
    with pytest.raises(InvalidDeploymentPlatfrom):
        cloud.patch_default_sc_to_non_default()
