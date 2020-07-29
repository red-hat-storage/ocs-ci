# -*- coding: utf8 -*-

from unittest.mock import patch
import copy

import pytest

from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.framework import config
from ocs_ci.ocs import exceptions


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
    with pytest.raises(exceptions.InvalidDeploymentPlatfrom):
        cloud.patch_default_sc_to_non_default()


def test_clouddeploymentbase_deploy_ocp_with_taken_cluster_name(clusterdir):
    """
    Check that ocp deploy fails when one tries to deploy a cluster with
    already taken cluster name.
    """
    cloud = CloudDeploymentBase()
    # monkey patch test implementation of check_cluster_existence() method,
    # which is not implemented in CloudDeploymentBase base class, so that
    # current cluster name is considered as already taken
    cloud.check_cluster_existence = lambda pr: pr in clusterdir['clusterName']
    # trying to deploy with already taken cluster name
    with pytest.raises(exceptions.SameNamePrefixClusterAlreadyExistsException):
        cloud.deploy_ocp()
