"""
Managed Services related functionalities
"""
import logging

from ocs_ci.utility.version import get_semantic_version
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_worker_nodes, get_node_objs
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_osd_pods
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.utility.utils import convert_device_size
import ocs_ci.ocs.cluster

log = logging.getLogger(__name__)


def verify_provider_topology():
    """
    Verify topology in a Managed Services provider cluster

    1. Verify replica count
    2. Verify total size
    3. Verify OSD size
    4. Verify worker node instance type
    5. Verify worker node instance count
    6. Verify OSD count
    7. Verify OSD CPU and memory

    """
    # importing here to avoid circular import
    from ocs_ci.ocs.resources.storage_cluster import StorageCluster, get_osd_count

    size = f"{config.ENV_DATA.get('size', 4)}Ti"
    replica_count = 3
    osd_size = 4
    instance_type = "m5.2xlarge"
    size_map = {
        "4Ti": {"total_size": 12, "osd_count": 3, "instance_count": 3},
        "8Ti": {"total_size": 24, "osd_count": 6, "instance_count": 6},
        "12Ti": {"total_size": 36, "osd_count": 9, "instance_count": 6},
        "16Ti": {"total_size": 48, "osd_count": 12, "instance_count": 6},
        "20Ti": {"total_size": 60, "osd_count": 15, "instance_count": 6},
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
    log.info(f"Verified that the instance type of worker nodes is {instance_type}")

    # Verify worker node instance count
    assert len(worker_node_names) == size_map[size]["instance_count"], (
        f"Worker node instance count is not as expected. Actual instance count is {len(worker_node_names)}. "
        f"Expected {size_map[size]['instance_count']}. List of worker nodes : {worker_node_names}"
    )
    log.info("Verified the number of worker nodes.")

    # Verify OSD count
    osd_count = get_osd_count()
    assert (
        osd_count == size_map[size]["osd_count"]
    ), f"OSD count is not as expected. Actual:{osd_count}. Expected:{size_map[size]['osd_count']}"
    log.info(f"Verified that the OSD count is {size_map[size]['osd_count']}")

    # Verify OSD CPU and memory
    osd_cpu_limit = "1850m"
    osd_cpu_request = "1850m"
    osd_pods = get_osd_pods()
    osd_memory_size = config.ENV_DATA["ms_osd_pod_memory"]
    log.info("Verifying OSD CPU and memory")
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
                assert (
                    container["resources"]["limits"]["memory"] == osd_memory_size
                ), f"OSD pod {osd_pod.name} container osd doesn't have memory limit {osd_memory_size}"
                assert (
                    container["resources"]["requests"]["memory"] == osd_memory_size
                ), f"OSD pod {osd_pod.name} container osd doesn't have memory request {osd_memory_size}"
    log.info("Verified OSD CPU and memory")


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


def get_ocs_osd_deployer_version():
    """
    Get OCS OSD deployer version from CSV

    Returns:
         Version: OCS OSD deployer version

    """
    csv_kind = OCP(kind="ClusterServiceVersion", namespace="openshift-storage")
    deployer_csv = csv_kind.get(selector=constants.OCS_OSD_DEPLOYER_CSV_LABEL)
    assert (
        "ocs-osd-deployer" in deployer_csv["items"][0]["metadata"]["name"]
    ), "Couldn't find ocs-osd-deployer CSV"
    deployer_version = deployer_csv["items"][0]["spec"]["version"]
    return get_semantic_version(deployer_version)
