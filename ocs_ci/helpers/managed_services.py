"""
Managed Services related functionalities
"""
import logging
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    get_worker_nodes,
    get_node_objs,
    get_node_pods,
)
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_osd_pods
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.utility.utils import convert_device_size, run_cmd
import ocs_ci.ocs.cluster

log = logging.getLogger(__name__)


def verify_provider_topology():
    """
    Verify topology in a Managed Services provider cluster

    1. Verify replica count
    2. Verify total size
    3. Verify OSD size
    4. Verify OSD running worker nodes count
    5. Verify worker node instance type
    6. Verify OSD count
    7. Verify OSD cpu
    8. Verify machine pools
    9. Verify OSD running nodes are part of the correct machinepool
    10. Verify that other pods are not running on OSD nodes

    """
    # importing here to avoid circular import
    from ocs_ci.ocs.resources.storage_cluster import StorageCluster, get_osd_count

    size = f"{config.ENV_DATA.get('size', 4)}Ti"
    replica_count = 3
    osd_size = 4
    instance_type = "m5.xlarge"
    size_map = {
        "4Ti": {"total_size": 12, "osd_count": 3, "instance_count": 3},
        "8Ti": {"total_size": 24, "osd_count": 6, "instance_count": 3},
        "12Ti": {"total_size": 36, "osd_count": 9, "instance_count": 6},
        "16Ti": {"total_size": 48, "osd_count": 12, "instance_count": 6},
        "20Ti": {"total_size": 60, "osd_count": 15, "instance_count": 9},
        "48Ti": {"total_size": 144, "osd_count": 36, "instance_count": 18},
        "96Ti": {"total_size": 288, "osd_count": 72, "instance_count": 36},
    }
    cluster_namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
    storage_cluster = StorageCluster(
        resource_name="ocs-storagecluster",
        namespace=cluster_namespace,
    )

    # Verify replica count
    assert (
        int(storage_cluster.data["spec"]["storageDeviceSets"][0]["replica"])
        == replica_count
    ), (
        f"Replica count is not as expected. Actual:{storage_cluster.data['spec']['storageDeviceSets'][0]['replica']}. "
        f"Expected: {replica_count}"
    )
    log.info(f"Verified that the replica count is {replica_count}")

    # Verify total size
    ct_pod = get_ceph_tools_pod()
    ceph_osd_df = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd df")
    total_size = int(ceph_osd_df.get("summary").get("total_kb"))
    total_size = convert_device_size(
        unformatted_size=f"{total_size}Ki", units_to_covert_to="TB", convert_size=1024
    )
    assert (
        total_size == size_map[size]["total_size"]
    ), f"Total size {total_size}Ti is not matching the expected total size {size_map[size]['total_size']}Ti"
    log.info(f"Verified that the total size is {size_map[size]['total_size']}Ti")

    # Verify OSD size
    osd_pvc_objs = get_all_pvc_objs(
        namespace=cluster_namespace, selector=constants.OSD_PVC_GENERIC_LABEL
    )
    for pvc_obj in osd_pvc_objs:
        assert (
            pvc_obj.get()["status"]["capacity"]["storage"] == f"{osd_size}Ti"
        ), f"Size of OSD PVC {pvc_obj.name} is not {osd_size}Ti"
    log.info(f"Verified that the size of each OSD is {osd_size}Ti")

    # Verify OSD running worker nodes count
    osd_nodes = get_osd_running_nodes()
    assert len(osd_nodes) == size_map[size]["instance_count"], (
        f"Worker node instance count is not as expected. Actual instance count is {len(osd_nodes)}. "
        f"Expected {size_map[size]['instance_count']}. List of worker nodes : {osd_nodes}"
    )
    log.info("Verified the number of worker nodes where OSD is running.")

    # Verify worker node instance type
    worker_node_names = get_worker_nodes()
    worker_nodes = get_node_objs(worker_node_names)
    for node_obj in worker_nodes:
        assert (
            node_obj.get("metadata")
            .get("metadata")
            .get("labels")
            .get("beta.kubernetes.io/instance-type")
            == instance_type
        ), f"Instance type of the worker node {node_obj.name} is not {instance_type}"
    log.info(f"Verified that the instance type of wokeer nodes is {instance_type}")

    # Verify OSD count
    osd_count = get_osd_count()
    assert (
        osd_count == size_map[size]["osd_count"]
    ), f"OSD count is not as expected. Actual:{osd_count}. Expected:{size_map[size]['osd_count']}"
    log.info(f"Verified that the OSD count is {size_map[size]['osd_count']}")

    # Verify OSD cpu
    osd_cpu_limit = "1750m"
    osd_cpu_request = "1750m"
    osd_pods = get_osd_pods()
    log.info("Verifying OSD cpu")
    for osd_pod in osd_pods:
        for container in osd_pod.data["spec"]["containers"]:
            if container["name"] == "osd":
                assert container["resources"]["limits"]["cpu"] == osd_cpu_limit, (
                    f"OSD pod {osd_pod.name} container osd doesn't have cpu limit {osd_cpu_limit}. "
                    f"Limit is {container['resources']['limits']['cpu']}"
                )
                assert container["resources"]["requests"]["cpu"] == osd_cpu_request, (
                    f"OSD pod {osd_pod.name} container osd doesn't have cpu request {osd_cpu_request}. "
                    f"Request is {container['resources']['requests']['cpu']}"
                )
    log.info("Verified OSD CPU")

    # Verify machine pools
    cmd = f"rosa list machinepool --cluster={config.ENV_DATA['cluster_name']} -o yaml"
    out = run_cmd(cmd)
    machine_pool_info_list = yaml.safe_load(out)
    assert (
        len(machine_pool_info_list) == 2
    ), f"Number of machinepools is not 2. Machinepools details: {machine_pool_info_list}"

    machine_pool_ids = []
    ceph_osd_nodepool_info = None
    for machine_pool_info in machine_pool_info_list:
        machine_pool_ids.append(machine_pool_info["id"])
        assert machine_pool_info["instance_type"] == instance_type, (
            f"Instance type of machinepool {machine_pool_info['id']} is {machine_pool_info['instance_type']}. "
            f"Expected instance type is {instance_type}"
        )
        if "node.ocs.openshift.io/osd" in machine_pool_info.get("labels", {}):
            ceph_osd_nodepool_info = machine_pool_info
    log.info(f"Machinepool IDs: {machine_pool_ids}")

    assert ceph_osd_nodepool_info, "OSD node pool not found"
    log.info(f"OSD node pool machinepool id is {ceph_osd_nodepool_info['id']}")
    assert ceph_osd_nodepool_info["replicas"] == size_map[size]["instance_count"], (
        f"Replicas of OSD node pool machinepool is {ceph_osd_nodepool_info['replicas']}. "
        f"Expected {size_map[size]['instance_count']}."
    )
    assert ("key", "node.ocs.openshift.io/osd") in ceph_osd_nodepool_info["taints"][
        0
    ].items(), (
        f"Verification of taints failed for machinepool {ceph_osd_nodepool_info['id']}. "
        f"Machinepool info: {ceph_osd_nodepool_info}"
    )
    log.info(f"Verified taints on machinepool {ceph_osd_nodepool_info['id']}")

    # Verify OSD running nodes are part of the correct machinepool
    osd_node_objs = get_node_objs(osd_nodes)
    for node_obj in osd_node_objs:
        annotation = (
            node_obj.get()
            .get("metadata")
            .get("annotations")
            .get("machine.openshift.io/machine")
        )
        assert (
            ceph_osd_nodepool_info["id"] in annotation
        ), f"OSD running node {node_obj.name} is part of the machinepool {ceph_osd_nodepool_info['id']}"
    log.info(
        f"OSD running nodes are part of the machinepool {ceph_osd_nodepool_info['id']}"
    )

    # Verify that other pods are not running on OSD nodes
    for node_obj in osd_node_objs:
        pods_on_node = get_node_pods(node_name=node_obj.name)
        for pod_name in pods_on_node:
            assert pod_name.startswith(
                ("rook-ceph-osd", "rook-ceph-crashcollector")
            ), f"Pod {pod_name} is running on OSD running node {node_obj.name}"
    log.info("Verified that other pods are not running on OSD nodes")


def get_used_capacity(msg):
    """
    Verify OSD percent used capacity greate than ceph_full_ratio

    Args:
        msg (str): message to be logged

    Returns:
         float: The percentage of the used capacity in the cluster

    """
    log.info(f"{msg}")
    used_capacity = ocs_ci.ocs.cluster.get_percent_used_capacity()
    log.info(f"Used Capacity is {used_capacity}%")
    return used_capacity


def verify_osd_used_capacity_greater_than_expected(expected_used_capacity):
    """
    Verify OSD percent used capacity greater than ceph_full_ratio

    Args:
        expected_used_capacity (float): expected used capacity

    Returns:
         bool: True if used_capacity greater than expected_used_capacity, False otherwise

    """
    osds_utilization = ocs_ci.ocs.cluster.get_osd_utilization()
    log.info(f"osd utilization: {osds_utilization}")
    for osd_id, osd_utilization in osds_utilization.items():
        if osd_utilization > expected_used_capacity:
            log.info(
                f"OSD ID:{osd_id}:{osd_utilization} greater than {expected_used_capacity}%"
            )
            return True
    return False


def verify_osds_are_on_correct_machinepool():
    """
    Verify that the OSD pods are running on nodes which are part of the correct machinepool.
    Applicable for Managed Services.

    """
    osd_nodes = get_osd_running_nodes()
    osd_node_objs = get_node_objs(osd_nodes)
    for node_obj in osd_node_objs:
        annotation = (
            node_obj.get()
            .get("metadata")
            .get("annotations")
            .get("machine.openshift.io/machine")
        )
        assert (
            constants.OSD_NODE_POOL in annotation
        ), f"OSD running node {node_obj.name} is not part of the machinepool {constants.OSD_NODE_POOL}"
    log.info(f"OSD running nodes are part of the machinepool {constants.OSD_NODE_POOL}")
