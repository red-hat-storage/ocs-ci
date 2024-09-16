"""
A test for multiple mds support
"""
import logging
from collections import defaultdict
import random

import pytest

from ocs_ci.helpers.helpers import verify_storagecluster_nodetopology
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.framework import config
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.storage_cluster import get_storage_cluster
from tests.functional.z_cluster.nodes.test_node_replacement_proactive import (
    delete_and_create_osd_node,
)

log = logging.getLogger(__name__)

MDS_NAME_TEMPLATE = "rook-ceph-mds-ocs-storagecluster-cephfilesystem-"


def get_mds_active_count():
    """
    Get the active mds count from the system
    Returns:
         int: active_mds_count

    """
    cephfs = ocp.OCP(
        kind=constants.CEPHFILESYSTEM,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    fs_data = cephfs.get(defaults.CEPHFILESYSTEM_NAME)
    mds_active_count = fs_data.get("spec").get("metadataServer").get("activeCount")
    return mds_active_count


# in  ocs_ci/ocs/resources/storage_cluster.py
def set_active_mds_count(count):
    """
    Set mds count for Storage cluster.

    Args:
        count (int): the count of active mds

    """
    sc = get_storage_cluster(namespace=config.ENV_DATA["cluster_namespace"])
    param = f'{{"spec": {{"managedResources": {{"cephFilesystems": {{"activeMetadataServers": {count}}}}}}}}}'
    sc.patch(
        resource_name=sc.get()["items"][0]["metadata"]["name"],
        params=param,
        format_type="merge",
    )


def adjust_active_mds_count(target_count):
    """
    Adjust the activeMetadataServers count for the Storage cluster to the target_count.
    The function will increase or decrease the count to match the target value.

    Args:
        target_count (int): The desired count for activeMetadataServers.
    """

    # Retrieve the current activeMetadataServers count
    current_count = get_mds_active_count()
    sc = get_storage_cluster(namespace=config.ENV_DATA["cluster_namespace"])
    resource_name = sc.get()["items"][0]["metadata"]["name"]

    if current_count < target_count:
        # Increment
        while current_count < target_count:
            current_count += 1
            param = (
                f'{{"spec": {{"managedResources": {{"cephFilesystems": '
                f'{{"activeMetadataServers": {current_count}}}}}}}}}'
            )
            sc.patch(resource_name=resource_name, params=param, format_type="merge")

            # Re-fetch the current count to handle race conditions
            current_params = sc.get(resource_name=resource_name)
            current_count = current_params["spec"]["managedResources"][
                "cephFilesystems"
            ]["activeMetadataServers"]

    elif current_count > target_count:
        # Decrement
        while current_count > target_count:
            current_count -= 1
            param = (
                f'{{"spec": {{"managedResources": {{"cephFilesystems": '
                f'{{"activeMetadataServers": {current_count}}}}}}}}}'
            )
            sc.patch(resource_name=resource_name, params=param, format_type="merge")

            # Re-fetch the current count to handle race conditions
            current_params = sc.get(resource_name=resource_name)
            current_count = current_params["spec"]["managedResources"][
                "cephFilesystems"
            ]["activeMetadataServers"]

    else:
        log.info(
            "The current count is already equal to the target count. No changes needed."
        )


def get_active_mds_pods():
    """
    Gets active mds pods.

    Returns:
        dict: mds pod names
    """
    ct_pod = pod.get_ceph_tools_pod()
    ceph_mdsmap = ct_pod.exec_ceph_cmd("ceph fs status")
    # Extract the mdsmap list from the data
    mdsmap = ceph_mdsmap["mdsmap"]

    # Filter and get the names of active MDS pods
    active_mds_names = [mds["name"] for mds in mdsmap if mds["state"] == "active"]
    return active_mds_names


# Place in node.py
def get_mds_per_node():
    """
    Gets the mds running pod names per node name

    Returns:
        dict: {"Node name":["mds running pod name running on the node",..,]}

    """
    dic_node_osd = defaultdict(list)
    mds_pods = get_active_mds_pods()
    for mds_pod in mds_pods:
        dic_node_osd[mds_pod.data["spec"]["nodeName"]].append(mds_pod)
    return dic_node_osd


def get_pod_memory_utilisation_in_percentage(pod_obj):
    """
    This function gets total and used memory of pods in Mebibytes and calculates the value in percentage.

    Returns:
         int: mds used memory in percentage

    """
    get_total_memory = pod_obj.get_memory(container_name="mds")
    total_memory_in_mebibytes = int(get_total_memory[:-2]) * 1024
    used_memory = pod.get_pod_used_memory_in_mebibytes(pod_obj.name)
    utilisation_in_percentage = (used_memory / total_memory_in_mebibytes) * 100
    return utilisation_in_percentage


class TestMultipleMds:
    """
    Tests for support multiple mds

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure mds pod count is set to original

        """

        def finalizer():
            assert adjust_active_mds_count(1), "Failed to set active mds count to 1"

        request.addfinalizer(finalizer)

    def test_multiple_mds(self, cluster):
        """
        1. Trigger the scale-up process to add new pods.
        2. Verify active and standby-replay mds counts.
        3. Perform node replacement on a newly added mds pod running node.
        4. Make sure all the active mds pods come to active state.
        """
        original_active_count = get_mds_active_count()
        # Scale up active mds pods from 1 to 2.
        new_active_count = original_active_count + 1

        adjust_active_mds_count(new_active_count)
        # Scale up active mds pods from 2 to 3.
        new_active_count = original_active_count + 1
        adjust_active_mds_count(new_active_count)
        # set_active_mds_count(new_active_count)
        ct_pod = pod.get_ceph_tools_pod()
        ceph_mdsmap = ct_pod.exec_ceph_cmd("ceph fs status")
        # Extract the mdsmap list from the data
        ceph_mdsmap = ceph_mdsmap["mdsmap"]
        # Counting active MDS daemons
        active_count = sum(1 for mds in ceph_mdsmap if mds["state"] == "active")

        standby_replay = sum(
            1 for mds in ceph_mdsmap if mds["state"] == "standby-replay"
        )

        log.info(f"Number of active MDS daemons:{active_count}")
        log.info(f"Number of standby MDS daemons:{standby_replay}")
        assert active_count == new_active_count, "Active mds counts did not increased"
        assert (
            standby_replay == new_active_count
        ), "Standby replay mds counts did not increased"

        # Get active mds node name for replacement
        active_mds_pod = get_active_mds_pods()
        selected_pod = random.choice(active_mds_pod)
        selected_pod = pod.get_pod_name_by_pattern(selected_pod[0])
        selected_pod_obj = pod.get_pod_obj(
            name=selected_pod, namespace=config.ENV_DATA["cluster_namespace"]
        )
        active_mds_node_name = selected_pod_obj.data["spec"].get("nodeName")
        log.info(f"Replacing active mds node : {active_mds_node_name}")

        # Replacing node
        delete_and_create_osd_node(active_mds_node_name)
        toolbox_pod = pod.get_ceph_tools_pod()
        tree_output = toolbox_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree")
        log.info(f"ceph osd tree output:{tree_output}")

        assert not (
            active_mds_node_name in str(tree_output)
        ), f"Deleted host {active_mds_node_name} still exist in ceph osd tree after node replacement"

        assert (
            verify_storagecluster_nodetopology
        ), "Storagecluster node topology is having an entry of non ocs node"

        assert (
            active_count == new_active_count
        ), "Active mds counts did not match after node replacement"
        assert (
            standby_replay == new_active_count
        ), "Standby replay mds counts did not match after node replacement"
