# -*- coding: utf8 -*-
import logging

import pytest

from ocs_ci.utility.prometheus import PrometheusAPI
# from ocs_ci.ocs.ocp import OCP
# from tests import helpers


logger = logging.getLogger(__name__)


@pytest.mark.libtest
def test_workload_rbd(workload_storageutilization_50p_rbd):
    logger.info(workload_storageutilization_50p_rbd)
    prometheus = PrometheusAPI()


@pytest.mark.libtest
def test_workload_rbd_again(workload_storageutilization_50p_rbd):
    logger.info(workload_storageutilization_50p_rbd)


@pytest.mark.libtest
def test_workload_cephfs(workload_storageutilization_50p_cephfs):
    logger.info(workload_storageutilization_50p_cephfs)
