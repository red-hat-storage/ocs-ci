"""
Test cases for multiple mds support
"""

import logging
import random
import time

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    tier4c,
    skipif_external_mode,
)
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import (
    adjust_active_mds_count_storagecluster,
    get_active_mds_count_cephfilesystem,
    get_active_mds_pods,
    get_active_and_standby_mds_count,
    get_active_mds_info,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import node, constants
from ocs_ci.utility.utils import ceph_health_check_base
from tests.functional.z_cluster.nodes.test_node_replacement_proactive import (
    delete_and_create_osd_node,
)


log = logging.getLogger(__name__)


@brown_squad
@tier4c
@skipif_external_mode
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
            active_mds_pod = get_active_mds_info()["active_pod"]
            log.info("Validate mds is up and running")
            pod.wait_for_pods_to_be_in_statuses(
                expected_statuses=[constants.STATUS_RUNNING],
                pod_names=[active_mds_pod],
            )
            log.info("Checking for Ceph Health OK")
            ceph_health_check_base()

        request.addfinalizer(finalizer)

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def verify_mds_count(self, new_active_mds_count):
        """
        Verify active and standby-replay mds counts.

        Args:
            new_active_mds_count (int): The desired count for active mds pods.

        """

        pod_counts = get_active_and_standby_mds_count()
        active_pod_count = pod_counts["active_pod_count"]
        standby_replay_count = pod_counts["standby_replay_count"]

        log.info(f"Number of active MDS daemons:{active_pod_count}")
        log.info(f"Number of standby MDS daemons:{standby_replay_count}")
        assert (
            active_pod_count == new_active_mds_count
        ), "Active mds counts did not increased"
        assert (
            standby_replay_count == new_active_mds_count
        ), "Standby replay mds counts did not increased"

    def test_node_replacement_multiple_mds(self):
        """
        1. Trigger the scale-up process to add new pods.
        2. Verify active and standby-replay mds count is same.
        3. Perform node replacement on a newly added mds pod running node.
        4. Make sure all the active mds pods come to active state.

        """
        original_active_count_cephfilesystem = get_active_mds_count_cephfilesystem()

        # Scale up active mds pods from 1 to 2 and then 2 to 3
        new_active_mds_count = original_active_count_cephfilesystem + 2
        adjust_active_mds_count_storagecluster(new_active_mds_count)

        # Verify active and standby-replay mds counts.
        self.verify_mds_count(new_active_mds_count)

        # Replace node
        active_mds_pods = get_active_mds_pods()
        active_mds_pod = random.choice(active_mds_pods)
        active_mds_node_name = active_mds_pod.data["spec"].get("nodeName")
        log.info(f"Replacing active mds node : {active_mds_node_name}")
        delete_and_create_osd_node(active_mds_node_name)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=120)

        # Verify active and standby-replay mds counts after node replacement
        self.verify_mds_count(new_active_mds_count)

    def test_node_drain_and_fault_tolerance_for_multiple_mds(self):
        """
        1. Trigger the scale-up process to add new pods.
        2. Drain active mds pod running node.
        3. Verify active and standby-replay mds count is same.
        4. Fail one active mds pod [out of two] and standby pod changes to active.

        """

        original_active_count_cephfilesystem = get_active_mds_count_cephfilesystem()

        # Scale up active mds pods from 1 to 2.
        new_active_mds_count = original_active_count_cephfilesystem + 1
        adjust_active_mds_count_storagecluster(new_active_mds_count)

        # Get active mds node name
        active_mds_pods = get_active_mds_pods()
        active_mds_pod = random.choice(active_mds_pods)
        active_mds_pod_name = active_mds_pod.name
        selected_pod_obj = pod.get_pod_obj(
            name=active_mds_pod_name, namespace=config.ENV_DATA["cluster_namespace"]
        )
        active_mds_node_name = selected_pod_obj.data["spec"].get("nodeName")

        # Drain active mds pod running node
        node.drain_nodes([active_mds_node_name])

        # Make the node schedulable again
        node.schedule_nodes([active_mds_node_name])

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=120)

        # Verify active and standby-replay mds counts.
        self.verify_mds_count(new_active_mds_count)

        # Fail one active mds pod [out of two]
        rand = random.randint(0, 1)
        ct_pod = pod.get_ceph_tools_pod()
        ct_pod.exec_ceph_cmd(f"ceph mds fail {rand}")
        time.sleep(60)

        # Verify active and standby-replay mds counts.
        self.verify_mds_count(new_active_mds_count)
