"""
Test cases for multiple mds support
"""

import logging
import random

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    tier4c,
    skipif_external_mode,
    skipif_hci_client,
)
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs.cluster import (
    adjust_active_mds_count_storagecluster,
    get_active_mds_count_cephfilesystem,
    get_active_mds_pod_objs,
    get_mds_counts,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import node, constants
from ocs_ci.ocs.resources.pod import get_mds_pods
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler
from tests.functional.z_cluster.nodes.test_node_replacement_proactive import (
    delete_and_create_osd_node,
)


log = logging.getLogger(__name__)


def verify_active_and_standby_mds_count(target_count):
    """
    Get the active and standby mds pod count from ceph command and verify it matches the target count.

    Args:
        target_count (int): The desired count of active and standby mds pods.

    """
    TimeoutSampler(timeout=180, sleep=10, func=get_mds_counts).wait_for_func_value(
        (target_count, target_count)
    )
    log.info(f"Active and standby-replay MDS pod counts reached {target_count}.")


@brown_squad
@tier4c
@skipif_external_mode
@skipif_hci_client
class TestMultipleMds:
    """
    Tests for support multiple mds

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure mds pod count is set to original.

        """

        def finalizer():
            """
            Adjust the activeMetadataServers count for the Storage cluster to 1.
            """
            adjust_active_mds_count_storagecluster(
                1
            ), "Failed to set active mds count to 1"

            log.info("Validate mds pods are up and running")
            mds_pods = get_mds_pods()
            for mds_pod in mds_pods:
                helpers.wait_for_resource_state(
                    resource=mds_pod, state=constants.STATUS_RUNNING
                )

            log.info("Checking for Ceph Health OK")
            ceph_health_check()

        request.addfinalizer(finalizer)

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_node_replacement_multiple_mds(self):
        """
        1. Trigger the scale-up process to add new pods.
        2. Verify active and standby-replay mds count is same.
        3. Perform node replacement on a mds pod running node.
        4. Make sure all the active mds pods come to active state.

        """
        original_active_count_cephfilesystem = get_active_mds_count_cephfilesystem()

        log.info("Scale up active mds pods from 1 to 3 sequentially.")
        new_active_mds_count = original_active_count_cephfilesystem + 2
        adjust_active_mds_count_storagecluster(new_active_mds_count)

        log.info("Verify active and standby-replay mds counts")
        verify_active_and_standby_mds_count(new_active_mds_count)

        # Replace active mds node
        active_mds_pods = get_active_mds_pod_objs()
        active_mds_pod = random.choice(active_mds_pods)
        active_mds_node_name = active_mds_pod.data["spec"].get("nodeName")
        log.info(f"Replacing active mds node : {active_mds_node_name}")
        delete_and_create_osd_node(active_mds_node_name)

        log.info("Verify active and standby-replay mds counts after node replacement")
        verify_active_and_standby_mds_count(new_active_mds_count)

        log.info("Performing cluster and Ceph health checks")
        self.sanity_helpers.health_check(tries=120)

    def test_node_drain_and_fault_tolerance_for_multiple_mds(self, pod_factory):
        """
        1. Trigger the scale-up process to add new pods.
        2. Drain active mds pod running node.
        3. Verify active and standby-replay mds count is same.
        4. Fail one active mds pod [out of two] and standby pod changes to active.

        """
        original_active_count_cephfilesystem = get_active_mds_count_cephfilesystem()

        log.info("Scale up active mds pods from 1 to 2")
        new_active_mds_count = original_active_count_cephfilesystem + 1
        adjust_active_mds_count_storagecluster(new_active_mds_count)

        log.info("Get active mds node name")
        active_mds_pods = get_active_mds_pod_objs()
        active_mds_pod = random.choice(active_mds_pods)
        active_mds_pod_name = active_mds_pod.name
        selected_pod_obj = pod.get_pod_obj(
            name=active_mds_pod_name, namespace=config.ENV_DATA["cluster_namespace"]
        )
        active_mds_node_name = selected_pod_obj.data["spec"].get("nodeName")

        log.info("Drain active mds pod running node")
        node.drain_nodes([active_mds_node_name])
        # Make the node schedulable again
        node.schedule_nodes([active_mds_node_name])

        log.info("Performing cluster and Ceph health checks")
        self.sanity_helpers.health_check(tries=120)

        log.info("Verify active and standby-replay mds counts")
        verify_active_and_standby_mds_count(new_active_mds_count)

        log.info("Start IO Workload")
        pod_obj = pod_factory(interface=constants.CEPHBLOCKPOOL)
        pod_obj.run_io(direct=1, runtime=180, storage_type="fs", size="1G")

        # Fail one active mds pod [out of two]
        log.info("Fail one active mds pod")
        rand = random.randint(0, 1)
        ct_pod = pod.get_ceph_tools_pod()
        ct_pod.exec_ceph_cmd(f"ceph mds fail {rand}")

        # Verify active and standby-replay mds counts is still same.
        log.info("Verify active and standby-replay mds counts after pod failure")
        verify_active_and_standby_mds_count(new_active_mds_count)

        log.info("Wait for IO completion")
        fio_result = pod_obj.get_fio_results()
        log.info("IO completed on all pods")
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
        )
