import logging

import pytest
import time

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    tier4a,
    E2ETest,
    ignore_leftovers,
    skipif_external_mode,
    skipif_bm,
)
from ocs_ci.ocs.node import get_worker_nodes, delete_and_create_osd_node
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.resources.pod import wait_for_storage_pods, run_io_in_bg
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


@tier4a
@ignore_leftovers
@skipif_external_mode
@skipif_bm
@pytest.mark.polarion_id("OCS-XXXX")
class TestRollingNodeReplacement(E2ETest):
    """
    Rolling fashion node replacement proactive

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_rolling_node_replacement_proactive(
        self,
        interface_iterate,
        pvc_factory,
        pod_factory,
        dc_pod_factory,
        bucket_factory,
        rgw_bucket_factory,
    ):

        """
        Testcase to replace a node one by one in rolling fashion

        """

        pod_obj = pod_factory(interface=interface_iterate)
        run_io_in_bg(pod_obj)

        iterations = 0

        while iterations < 5:

            log.info(f"This is the {iterations}th iteration")

            # Get worker nodes
            worker_node_list = get_worker_nodes()
            log.info(f"Current available worker nodes are {worker_node_list}")

            for node_to_replace in worker_node_list:

                # Replace node
                log.info(f"Replacing node {node_to_replace}")
                delete_and_create_osd_node(
                    osd_node_name=node_to_replace, validations=False
                )

                # Verify OSD is encrypted
                if config.ENV_DATA.get("encryption_at_rest"):
                    osd_encryption_verification()
                ceph_cluster_obj = CephCluster()
                assert ceph_cluster_obj.wait_for_rebalance(
                    timeout=1800
                ), "Data re-balance failed to complete"

                # Verify everything running fine
                log.info("Verifying All resources are Running and cluster is health OK")
                ceph_health_check(tries=120, delay=15)
                wait_for_storage_pods(timeout=1200)

                # New node list
                log.info(f"New node list are: {get_worker_nodes()}")

            # Next iteration
            log.info("Wait for 10 mins before starting next iteration")
            time.sleep(600)
            iterations += 1

        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=120)

        # Creating Resources
        log.info("Creating Resources using sanity helpers")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )

        # Deleting Resources
        self.sanity_helpers.delete_resources()
