import logging
import pytest

from ocs_ci.ocs.ui.add_replace_device_ui import AddReplaceDeviceUI
from ocs_ci.framework import config
from ocs_ci.framework.testlib import ui, ignore_leftovers, skipif_lso
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification
from ocs_ci.framework.pytest_customization.marks import (
    skipif_bm,
    skipif_external_mode,
    skipif_bmpsi,
    skipif_ui_not_support,
    brown_squad,
)

logger = logging.getLogger(__name__)


class TestAddCapacityUI(object):
    """
    Test Add Capacity on via UI

    """

    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        def finalizer():
            logger.info("Perform Ceph health checks after 'add capacity'")
            ceph_health_check()

        request.addfinalizer(finalizer)

    @ui
    @skipif_lso
    @skipif_bm
    @skipif_bmpsi
    @ignore_leftovers
    @skipif_external_mode
    @skipif_ui_not_support("add_capacity")
    @brown_squad
    def test_add_capacity_internal(self, setup_ui_class):
        """
        Test Add Capacity on Internal cluster via UI

        """
        logger.info("Get osd pods before add capacity")
        osd_pods_before_add_capacity = get_osd_pods()
        osd_count = len(osd_pods_before_add_capacity)

        logger.info("Add capacity via UI")
        infra_ui_obj = AddReplaceDeviceUI()
        infra_ui_obj.add_capacity_ui()

        logger.info("Wait for osd pods to be in Running state")
        for osd_pods in TimeoutSampler(
            timeout=600,
            sleep=10,
            func=get_osd_pods,
        ):
            if len(osd_pods) == (osd_count + 3):
                break

        osd_pod_names = list()
        for osd_pod in osd_pods:
            wait_for_resource_state(
                resource=osd_pod, state=constants.STATUS_RUNNING, timeout=300
            )
            osd_pod_names.append(osd_pod.name)

        logger.info("Verify via ui, all osd pods in Running state")
        infra_ui_obj.verify_pod_status(pod_names=osd_pod_names)

        logger.info("Wait data re-balance to complete")
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=5400
        ), "Data re-balance failed to complete"

        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()
