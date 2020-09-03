import logging

import pytest
import random

from ocs_ci.framework import config
from tests.helpers import get_worker_nodes
from ocs_ci.framework.pytest_customization.marks import tier4a
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import (
    tier4, ManageTest, ignore_leftovers
)
from ocs_ci.ocs import constants, node

from tests.sanity_helpers import Sanity

log = logging.getLogger(__name__)


@tier4
@tier4a
@ignore_leftovers
class TestNodeReplacement(ManageTest):
    """
    Knip-894 Node replacement - AWS-IPI-Proactive

    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_nodereplacement_proactive(self, pvc_factory, pod_factory, dc_pod_factory):
        """
        Knip-894 Node Replacement proactive

        """

        # Get worker nodes
        worker_node_list = get_worker_nodes()
        log.info(f"Current available worker nodes are {worker_node_list}")

        osd_pods_obj = pod.get_osd_pods()
        osd_node_name = pod.get_pod_node(random.choice(osd_pods_obj)).name
        log.info(f"Selected OSD is {osd_node_name}")

        log.info("Creating dc pod backed with rbd pvc and running io in bg")
        for worker_node in worker_node_list:
            if worker_node != osd_node_name:
                rbd_dc_pod = dc_pod_factory(interface=constants.CEPHBLOCKPOOL, node_name=worker_node, size=20)
                pod.run_io_in_bg(rbd_dc_pod, expect_to_fail=False, fedora_dc=True)

        log.info("Creating dc pod backed with cephfs pvc and running io in bg")
        for worker_node in worker_node_list:
            if worker_node != osd_node_name:
                cephfs_dc_pod = dc_pod_factory(interface=constants.CEPHFILESYSTEM, node_name=worker_node, size=20)
                pod.run_io_in_bg(cephfs_dc_pod, expect_to_fail=False, fedora_dc=True)

        if config.ENV_DATA['platform'].lower() == constants.AWS_PLATFORM:
            if config.ENV_DATA['deployment_type'] == 'ipi':
                node.delete_and_create_osd_node_aws_ipi(osd_node_name)

            elif config.ENV_DATA['deployment_type'] == 'upi':
                node.delete_and_create_osd_node_aws_upi(osd_node_name)
            else:
                pytest.fail(
                    f"ocs-ci config 'deployment_type' value '{config.ENV_DATA['deployment_type']}' is not valid, "
                    f"results of this test run are all invalid.")

        elif config.ENV_DATA['platform'].lower() == constants.VSPHERE_PLATFORM:
            pytest.skip("Skipping add node in Vmware platform due to "
                        "https://bugzilla.redhat.com/show_bug.cgi?id=1844521"
                        )

        # Creating Resources
        log.info("Creating Resources using sanity helpers")
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        # Deleting Resources
        self.sanity_helpers.delete_resources()
        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=30)
