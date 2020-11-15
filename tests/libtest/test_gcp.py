# -*- coding: utf8 -*-


import logging

from ocs_ci.deployment.gcp import GCPIPI
from ocs_ci.framework import config
from ocs_ci.framework.testlib import libtest
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import gcp_platform_required


logger = logging.getLogger(__name__)


@libtest
@gcp_platform_required
def test_assumptions():
    """
    Check basic consistency in platform handling.
    """
    assert config.ENV_DATA["platform"] == constants.GCP_PLATFORM


@libtest
@gcp_platform_required
def test_gcp_service_account_key_loading():
    """
    Check that no exception is raised during loading of GCP service account
    key, and that the credentials are not None.
    """
    gcp_depl = GCPIPI()
    assert gcp_depl.util.service_account is not None


@libtest
@gcp_platform_required
def test_check_cluster_existence():
    """
    Simple test of GCP check_cluster_existence() method implementation.
    Invalid clustername should be evaluated as False, while current cluster
    name should result in True (obviously current cluster name exists).
    """
    gcp_depl = GCPIPI()
    assert not gcp_depl.check_cluster_existence("an_invalid_clustername000")
    assert gcp_depl.check_cluster_existence(gcp_depl.cluster_name)
    assert gcp_depl.check_cluster_existence(gcp_depl.cluster_name[:5])
