# -*- coding: utf8 -*-

from unittest.mock import patch
import copy

import pytest

from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.framework import config
from ocs_ci.ocs import exceptions


class TestCloudDeployment(CloudDeploymentBase):
    """
    For proper testing of CloudDeploymentBase class, we need to subclass it and
    define it's mandatory attributes. Instantiating sheer CloudDeploymentBase
    will fail (which is expected for such base class).
    """
    # avoid raising NotImplementedError so that testing base class is possible
    DEFAULT_STORAGECLASS = "cloudstorage"


def test_clouddeploymentbase_has_no_default_storageclass():
    """
    Check assumption that cloud base class doens't have the default
    storageclass defined, and so it can't be instantiated.
    """
    with pytest.raises(NotImplementedError):
        CloudDeploymentBase()


def test_clouddeploymentbase_init(clusterdir):
    """
    Test that creation of CloudDeploymentBase object without any constructor
    agruments works (object is created, no exceptions are raised) and it's
    cluster name is loaded from cluster dir properly.
    """
    cloud = TestCloudDeployment()
    assert cloud.cluster_name == clusterdir['clusterName']


def test_clouddeploymentbase_init_withname(clusterdir):
    """
    Test that creation of CloudDeploymentBase object without any constructor
    agruments works when a cluster name is reconfigured via ocs-ci config.
    """
    TEST_ENV_DATA = copy.deepcopy(config.ENV_DATA)
    TEST_ENV_DATA["cluster_name"] = "another-cluster"
    with patch("ocs_ci.framework.config.ENV_DATA", TEST_ENV_DATA):
        cloud = TestCloudDeployment()
        assert cloud.cluster_name == TEST_ENV_DATA["cluster_name"]


def test_clouddeploymentbase_deploy_ocp_with_taken_cluster_name(clusterdir):
    """
    Check that ocp deploy fails when one tries to deploy a cluster with
    already taken cluster name.
    """
    cloud = TestCloudDeployment()
    # monkey patch test implementation of check_cluster_existence() method,
    # which is not implemented in CloudDeploymentBase base class, so that
    # current cluster name is considered as already taken
    cloud.check_cluster_existence = lambda pr: pr in clusterdir['clusterName']
    # trying to deploy with already taken cluster name
    with pytest.raises(exceptions.SameNamePrefixClusterAlreadyExistsException):
        cloud.deploy_ocp()
