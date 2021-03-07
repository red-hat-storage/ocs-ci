import logging

import pytest
import random

from ocs_ci.framework import config
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import (
    tier4,
    tier4a,
    ManageTest,
    ignore_leftovers,
    aws_platform_required,
    ipi_deployment_required,
)
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.cluster import CephCluster, is_lso_cluster
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification
from ocs_ci.framework.pytest_customization.marks import (
    skipif_openshift_dedicated,
    skipif_bmpsi,
)

from ocs_ci.helpers.sanity_helpers import Sanity

log = logging.getLogger(__name__)


def select_osd_node_name():
    """
    select randomly one of the osd nodes

    Returns:
        str: the selected osd node name

    """
    osd_node_names = node.get_osd_running_nodes()
    osd_node_name = random.choice(osd_node_names)
    log.info(f"Selected OSD is {osd_node_name}")
    return osd_node_name


def check_node_replacement_verification_steps(
    old_node_name, new_node_name, old_osd_node_names, old_osd_id
):
    """
    Check if the node replacement verification steps finished successfully.

    Args:
        old_node_name (str): The name of the old node that has been deleted
        new_node_name (str): The name of the new node that has been created
        old_osd_node_names (list): The name of the new node that has been added to osd nodes
        old_osd_id (str): The old osd id

    Raises:
        AssertionError: If the node replacement verification steps failed.

    """
    new_osd_node_name = node.wait_for_new_osd_node(old_osd_node_names)
    assert new_osd_node_name, "New osd node not found"

    assert node.node_replacement_verification_steps_ceph_side(
        old_node_name, new_node_name, new_osd_node_name
    )
    assert node.node_replacement_verification_steps_user_side(
        old_node_name, new_node_name, new_osd_node_name, old_osd_id
    )


def delete_and_create_osd_node(osd_node_name):
    """
    Delete an osd node, and create a new one to replace it

    Args:
        osd_node_name (str): The osd node name to delete

    """
    new_node_name = None
    osd_pod = node.get_node_pods(osd_node_name, pods_to_search=pod.get_osd_pods())[0]
    old_osd_id = pod.get_osd_pod_id(osd_pod)

    old_osd_node_names = node.get_osd_running_nodes()

    # error message for invalid deployment configuration
    msg_invalid = (
        "ocs-ci config 'deployment_type' value "
        f"'{config.ENV_DATA['deployment_type']}' is not valid, "
        f"results of this test run are all invalid."
    )
    # TODO: refactor this so that AWS is not a "special" platform
    if config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
        if config.ENV_DATA["deployment_type"] == "ipi":
            new_node_name = node.delete_and_create_osd_node_ipi(osd_node_name)

        elif config.ENV_DATA["deployment_type"] == "upi":
            new_node_name = node.delete_and_create_osd_node_aws_upi(osd_node_name)
        else:
            log.error(msg_invalid)
            pytest.fail(msg_invalid)
    elif config.ENV_DATA["platform"].lower() in constants.CLOUD_PLATFORMS:
        if config.ENV_DATA["deployment_type"] == "ipi":
            new_node_name = node.delete_and_create_osd_node_ipi(osd_node_name)
        else:
            log.error(msg_invalid)
            pytest.fail(msg_invalid)
    elif config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
        wnodes_not_in_ocs = node.get_worker_nodes_not_in_ocs()
        # We use for vSphere an existing worker node, due to issue
        # https://github.com/red-hat-storage/ocs-ci/issues/3894
        # Issue for tracking is https://github.com/red-hat-storage/ocs-ci/issues/3945
        if wnodes_not_in_ocs:
            if is_lso_cluster():
                new_node_name = node.delete_and_create_osd_node_vsphere_upi_lso(
                    osd_node_name, use_existing_node=True
                )

            else:
                new_node_name = node.delete_and_create_osd_node_vsphere_upi(
                    osd_node_name, use_existing_node=True
                )
        else:
            pytest.skip(
                "We don't have an extra worker node not in OCS. "
                "Skip node replacement test with vSphere due to issue "
                "https://github.com/red-hat-storage/ocs-ci/issues/3945"
            )

    log.info("Start node replacement verification steps...")
    check_node_replacement_verification_steps(
        osd_node_name, new_node_name, old_osd_node_names, old_osd_id
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
        self,
        pvc_factory,
        pod_factory,
        dc_pod_factory,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        Knip-894 Node Replacement proactive when IO running in the background

        """

        # Get worker nodes
        worker_node_list = node.get_worker_nodes()
        log.info(f"Current available worker nodes are {worker_node_list}")

        osd_node_name = select_osd_node_name()

        log.info("Creating dc pod backed with rbd pvc and running io in bg")
        for worker_node in worker_node_list:
            if worker_node != osd_node_name:
                rbd_dc_pod = dc_pod_factory(
                    interface=constants.CEPHBLOCKPOOL, node_name=worker_node, size=20
                )
                pod.run_io_in_bg(rbd_dc_pod, expect_to_fail=False, fedora_dc=True)

        log.info("Creating dc pod backed with cephfs pvc and running io in bg")
        for worker_node in worker_node_list:
            if worker_node != osd_node_name:
                cephfs_dc_pod = dc_pod_factory(
                    interface=constants.CEPHFILESYSTEM, node_name=worker_node, size=20
                )
                pod.run_io_in_bg(cephfs_dc_pod, expect_to_fail=False, fedora_dc=True)

        delete_and_create_osd_node(osd_node_name)

        # Creating Resources
        log.info("Creating Resources using sanity helpers")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        # Deleting Resources
        self.sanity_helpers.delete_resources()

        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=120)

        # Verify OSD is encrypted
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()


@tier4
@tier4a
@ignore_leftovers
@skipif_openshift_dedicated
@skipif_bmpsi
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
        self.sanity_helpers.health_check(tries=120)

        # Verify OSD encrypted
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=1800
        ), "Data re-balance failed to complete"
