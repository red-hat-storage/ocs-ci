import logging

import pytest
import random

from ocs_ci.framework import config
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import (
    tier4a,
    ManageTest,
    ignore_leftovers,
    ipi_deployment_required,
)
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.cluster import CephCluster, is_lso_cluster, is_ms_provider_cluster
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification
from ocs_ci.framework.pytest_customization.marks import (
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_bmpsi,
    bugzilla,
    skipif_external_mode,
    skipif_ms_consumer,
    skipif_hci_client,
    brown_squad,
)
from ocs_ci.helpers.helpers import verify_storagecluster_nodetopology
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
    old_node_name, new_node_name, old_osd_node_names, old_osd_ids
):
    """
    Check if the node replacement verification steps finished successfully.

    Args:
        old_node_name (str): The name of the old node that has been deleted
        new_node_name (str): The name of the new node that has been created
        old_osd_node_names (list): The name of the new node that has been added to osd nodes
        old_osd_ids (list): List of the old osd ids

    Raises:
        AssertionError: If the node replacement verification steps failed.

    """
    min_osd_nodes = 3
    num_of_old_osd_nodes = len(old_osd_node_names)
    ocs_nodes = node.get_ocs_nodes()
    num_of_old_ocs_nodes = len(ocs_nodes)

    if num_of_old_osd_nodes <= min_osd_nodes:
        log.info(
            f"We have {num_of_old_osd_nodes} osd nodes in the cluster - which is the minimum number "
            f"of osd nodes. Wait for the new created worker node to appear in the osd nodes"
        )
        timeout = 1500
        # In vSphere UPI platform, we are creating new node with same name as deleted
        # node using terraform
        if (
            config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM
            and config.ENV_DATA["deployment_type"] == "upi"
        ):
            new_osd_node_name = old_node_name
        else:
            new_osd_node_name = node.wait_for_new_osd_node(old_osd_node_names, timeout)
        log.info(f"Newly created OSD name: {new_osd_node_name}")
        assert new_osd_node_name, (
            f"New osd node not found after the node replacement process "
            f"while waiting for {timeout} seconds"
        )
    elif num_of_old_osd_nodes < num_of_old_ocs_nodes:
        num_of_extra_old_ocs_nodes = num_of_old_ocs_nodes - num_of_old_osd_nodes
        log.info(
            f"We have {num_of_extra_old_ocs_nodes} existing extra OCS worker nodes in the cluster"
            f"Wait for one of the existing OCS nodes to appear in the osd nodes"
        )
        timeout = 600
        new_osd_node_name = node.wait_for_new_osd_node(old_osd_node_names, timeout)
        assert new_osd_node_name, (
            f"New osd node not found after the node replacement process "
            f"while waiting for {timeout} seconds"
        )
    else:
        log.info(
            f"We have more than {min_osd_nodes} osd nodes in the cluster, and also we don't have "
            f"an existing extra OCS worker nodes in the cluster. Don't wait for the new osd node"
        )
        new_osd_node_name = None

    assert node.node_replacement_verification_steps_ceph_side(
        old_node_name, new_node_name, new_osd_node_name
    )
    assert node.node_replacement_verification_steps_user_side(
        old_node_name, new_node_name, new_osd_node_name, old_osd_ids
    )

    # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
    if is_ms_provider_cluster() and config.is_consumer_exist():
        assert node.consumers_verification_steps_after_provider_node_replacement()


def delete_and_create_osd_node(osd_node_name):
    """
    Delete an osd node, and create a new one to replace it

    Args:
        osd_node_name (str): The osd node name to delete

    """
    new_node_name = None
    old_osd_ids = node.get_node_osd_ids(osd_node_name)

    old_osd_node_names = node.get_osd_running_nodes()

    # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
    if is_ms_provider_cluster() and config.is_consumer_exist():
        pytest.skip(
            "The test will not run with an MS provider and MS consumer clusters due to the BZ "
            "https://bugzilla.redhat.com/show_bug.cgi?id=2131581. issue for tracking: "
            "https://github.com/red-hat-storage/ocs-ci/issues/6540"
        )

    # error message for invalid deployment configuration
    msg_invalid = (
        "ocs-ci config 'deployment_type' value "
        f"'{config.ENV_DATA['deployment_type']}' is not valid, "
        f"results of this test run are all invalid."
    )

    if config.ENV_DATA["deployment_type"] in ["ipi", "managed"]:
        if is_lso_cluster():
            # TODO: Implement functionality for Internal-Attached devices mode
            # once ocs-ci issue #4545 is resolved
            # https://github.com/red-hat-storage/ocs-ci/issues/4545
            pytest.skip("Functionality not implemented for this deployment mode")
        else:
            new_node_name = node.delete_and_create_osd_node_ipi(osd_node_name)

    elif config.ENV_DATA["deployment_type"] == "upi":
        if config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            new_node_name = node.delete_and_create_osd_node_aws_upi(osd_node_name)
        elif config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
            if is_lso_cluster():
                new_node_name = node.delete_and_create_osd_node_vsphere_upi_lso(
                    osd_node_name, use_existing_node=False
                )
            else:
                new_node_name = node.delete_and_create_osd_node_vsphere_upi(
                    osd_node_name, use_existing_node=False
                )
    else:
        log.error(msg_invalid)
        pytest.fail(msg_invalid)

    log.info("Start node replacement verification steps...")
    check_node_replacement_verification_steps(
        osd_node_name, new_node_name, old_osd_node_names, old_osd_ids
    )


@brown_squad
@tier4a
@ignore_leftovers
@ipi_deployment_required
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_bmpsi
@skipif_external_mode
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

        assert (
            verify_storagecluster_nodetopology
        ), "Storagecluster node topology is having an entry of non ocs node(s) - Not expected"


@brown_squad
@tier4a
@ignore_leftovers
@skipif_bmpsi
@skipif_external_mode
@skipif_ms_consumer
@skipif_hci_client
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

        assert (
            verify_storagecluster_nodetopology
        ), "Storagecluster node topology is having an entry of non ocs node(s) - Not expected"


@tier4a
@brown_squad
@ignore_leftovers
@bugzilla("1840539")
@pytest.mark.polarion_id("OCS-2535")
@skipif_external_mode
@skipif_managed_service
@skipif_hci_provider_and_client
class TestNodeReplacementTwice(ManageTest):
    """
    Node replacement twice:
    node_x -> node_y
    node_z -> node_x

    After node_replacement, the deleted node (node_x) suppose to be removed from the ceph-osd-tree.
    The BZ deals with the SECOND node_replacement.
    The existence of the deleted node (node_x from previous replacement) in the crash-map ends with:
      1. node is labeled for rack correctly
      2. ceph side host still on the old rack
    """

    def test_nodereplacement_twice(self):
        for i in range(2):
            # Get random node name for replacement
            node_name_to_delete = select_osd_node_name()
            log.info(f"Selected node for replacement: {node_name_to_delete}")
            delete_and_create_osd_node(node_name_to_delete)
            ct_pod = pod.get_ceph_tools_pod()
            tree_output = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree")
            log.info("ceph osd tree output:")
            log.info(tree_output)

            assert not (
                node_name_to_delete in str(tree_output)
            ), f"Deleted host {node_name_to_delete} still exist in ceph osd tree after node replacement"

            assert (
                verify_storagecluster_nodetopology
            ), "Storagecluster node topology is having an entry of non ocs node(s) - Not expected"
