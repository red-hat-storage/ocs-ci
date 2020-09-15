import logging

import pytest
import random

from ocs_ci.framework import config
from tests.helpers import get_worker_nodes
from ocs_ci.framework.pytest_customization.marks import tier4a
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import (
    tier4, ManageTest, ignore_leftovers, aws_platform_required,
    ipi_deployment_required
)
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.cluster import CephCluster

from tests.sanity_helpers import Sanity

log = logging.getLogger(__name__)


def select_osd_node_name():
    """
    select randomly one of the osd nodes

    Returns:
        str: the selected osd node name

    """
    osd_pods_obj = pod.get_osd_pods()
    osd_node_name = pod.get_pod_node(random.choice(osd_pods_obj)).name
    log.info(f"Selected OSD is {osd_node_name}")
    return osd_node_name


def delete_and_create_osd_node(osd_node_name):
    """
    Delete an osd node, and create a new one to replace it

    Args:
        osd_node_name (str): The osd node name to delete

    """
    # error message for invalid deployment configuration
    msg_invalid = (
        "ocs-ci config 'deployment_type' value "
        f"'{config.ENV_DATA['deployment_type']}' is not valid, "
        f"results of this test run are all invalid."
    )
    # TODO: refactor this so that AWS is not a "special" platform
    if config.ENV_DATA['platform'].lower() == constants.AWS_PLATFORM:
        if config.ENV_DATA['deployment_type'] == 'ipi':
            node.delete_and_create_osd_node_ipi(osd_node_name)

        elif config.ENV_DATA['deployment_type'] == 'upi':
            node.delete_and_create_osd_node_aws_upi(osd_node_name)
        else:
            log.error(msg_invalid)
            pytest.fail(msg_invalid)
    elif config.ENV_DATA['platform'].lower() in constants.CLOUD_PLATFORMS:
        if config.ENV_DATA['deployment_type'] == 'ipi':
            node.delete_and_create_osd_node_ipi(osd_node_name)
        else:
            log.error(msg_invalid)
            pytest.fail(msg_invalid)
    elif config.ENV_DATA['platform'].lower() == constants.VSPHERE_PLATFORM:
        worker_nodes_not_in_ocs = node.get_worker_nodes_not_in_ocs()
        if not worker_nodes_not_in_ocs:
            pytest.skip(
                "Skipping the test because we don't have an "
                "extra worker node that not in ocs"
            )
        else:
            log.info(
                "Perform delete and create ocs node in Vmware using one "
                "of the existing extra worker nodes that not in ocs"
            )
            node.delete_and_create_osd_node_vsphere_upi(
                osd_node_name, use_existing_node=True
            )


@tier4
@tier4a
@ignore_leftovers
@aws_platform_required
@ipi_deployment_required
class TestNodeReplacementWithIO(ManageTest):
    """
    Knip-894 Node replacement proactive with IO

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_nodereplacement_proactive_with_io_running(
        self, pvc_factory, pod_factory, dc_pod_factory
    ):
        """
        Knip-894 Node Replacement proactive when IO running in the background

        """

        # Get worker nodes
        worker_node_list = get_worker_nodes()
        log.info(f"Current available worker nodes are {worker_node_list}")

        osd_node_name = select_osd_node_name()

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

        delete_and_create_osd_node(osd_node_name)

        # Creating Resources
        log.info("Creating Resources using sanity helpers")
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        # Deleting Resources
        self.sanity_helpers.delete_resources()

        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=120)


@tier4
@tier4a
@ignore_leftovers
class TestNodeReplacement(ManageTest):
    """
    Knip-894 Node replacement proactive

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_nodereplacement_proactive(self):
        """
        Knip-894 Node Replacement proactive(without IO running)

        """
        osd_node_name = select_osd_node_name()
        delete_and_create_osd_node(osd_node_name)

        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=90)
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(timeout=1800), (
            "Data re-balance failed to complete"
        )
