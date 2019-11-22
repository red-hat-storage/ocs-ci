# -*- coding: utf8 -*-
import logging

import pytest

# from ocs_ci.utility.prometheus import PrometheusAPI
# from ocs_ci.ocs.ocp import OCP
# from tests import helpers


logger = logging.getLogger(__name__)


@pytest.mark.libtest
def test_workload_rbd(workload_storageutilization_50p_rbd):
    logger.info(workload_storageutilization_50p_rbd)


@pytest.mark.libtest
def test_workload_rbd_in_some_other_way(workload_storageutilization_50p_rbd):
    """
    This test case is using the same workload fixture as the previous one.
    These workload fixtures are designed to execute only once, so that both
    this and the previous test are using the same workload.
    """
    logger.info(workload_storageutilization_50p_rbd)


@pytest.mark.libtest
def test_workload_cephfs(workload_storageutilization_50p_cephfs):
    logger.info(workload_storageutilization_50p_cephfs)
