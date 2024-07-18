"""
Managed Services related functionalities
"""

import logging
import re

from ocs_ci.helpers.helpers import create_ocs_object_from_kind_and_name, create_resource
from ocs_ci.ocs.exceptions import ClusterNotFoundException
from ocs_ci.ocs.resources import csv
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.decorators import switch_to_orig_index_at_last
from ocs_ci.utility.managedservice import get_storage_provider_endpoint
from ocs_ci.utility.version import (
    get_semantic_version,
    get_semantic_ocs_version_from_config,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import (
    get_worker_nodes,
    get_node_objs,
    get_node_zone_dict,
    verify_worker_nodes_security_groups,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_osd_pods, get_pod_node
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.utility.utils import convert_device_size, TimeoutSampler
from ocs_ci.utility.aws import AWS
import ocs_ci.ocs.cluster
from ocs_ci.utility import version

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
    cluster_namespace = config.ENV_DATA["cluster_namespace"]
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
    osd_cpu_limit = config.ENV_DATA["ms_osd_pod_cpu"]
    osd_cpu_request = config.ENV_DATA["ms_osd_pod_cpu"]
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

    # Verify OSD distribution
    verify_osd_distribution_on_provider()


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
    ns_name = config.ENV_DATA["cluster_namespace"]
    csv_kind = OCP(kind="ClusterServiceVersion", namespace=ns_name)
    deployer_csv = csv_kind.get(selector=constants.OCS_OSD_DEPLOYER_CSV_LABEL)
    assert (
        "ocs-osd-deployer" in deployer_csv["items"][0]["metadata"]["name"]
    ), "Couldn't find ocs-osd-deployer CSV"
    deployer_version = deployer_csv["items"][0]["spec"]["version"]
    return get_semantic_version(deployer_version)


def verify_osd_distribution_on_provider():
    """
    Verify the OSD distribution on the provider cluster

    """
    size = config.ENV_DATA.get("size", 4)
    nodes_zone = get_node_zone_dict()
    osd_pods = get_osd_pods()
    zone_osd_count = {}

    # Get OSD zone and compare with it's node zone
    for osd_pod in osd_pods:
        osd_zone = osd_pod.get()["metadata"]["labels"]["topology-location-zone"]
        osd_node = get_pod_node(osd_pod).name
        assert osd_zone == nodes_zone[osd_node], (
            f"Zone in OSD label and node's zone are not matching. OSD name:{osd_node.name}, Zone: {osd_zone}. "
            f"Node name: {osd_node}, Zone: {nodes_zone[osd_node]}"
        )
        zone_osd_count[osd_zone] = zone_osd_count.get(osd_zone, 0) + 1

    # Verify the number of OSDs per zone
    for zone, osd_count in zone_osd_count.items():
        # 4Ti is the size of OSD
        assert (
            osd_count == int(size) / 4
        ), f"Zone {zone} does not have {size / 4} osd, but {osd_count}"


def verify_storageclient(
    storageclient_name=None, namespace=None, provider_name=None, verify_sc=True
):
    """
    Verify status, values and resources related to a storageclient

    Args:
        storageclient_name (str): Name of the storageclient to be verified. If the name is not given, it will be
            assumed that only one storageclient is present in the cluster.
        namespace (str): Namespace where the storageclient is present.
            Default value will be taken from ENV_DATA["cluster_namespace"]
        provider_name (str): Name of the provider cluster to which the storageclient is connected.
        verify_sc (bool): True to verify the storageclassclaims and storageclasses associated with the storageclient.

    """
    storageclient_obj = OCP(
        kind=constants.STORAGECLIENT,
        namespace=namespace or config.ENV_DATA["cluster_namespace"],
    )
    storageclient = (
        storageclient_obj.get(resource_name=storageclient_name)
        if storageclient_name
        else storageclient_obj.get()["items"][0]
    )
    storageclient_name = storageclient["metadata"]["name"]

    provider_name = provider_name or config.ENV_DATA.get("provider_name", "")
    endpoint_actual = get_storage_provider_endpoint(provider_name)
    assert storageclient["spec"]["storageProviderEndpoint"] == endpoint_actual, (
        f"The value of storageProviderEndpoint is not correct in storageclient {storageclient['metadata']['name']}."
        f" Value in storageclient is {storageclient['spec']['storageProviderEndpoint']}. "
        f"Value in the provider cluster {provider_name} is {endpoint_actual}"
    )
    log.info(
        f"Verified the storageProviderEndpoint value in the storageclient {storageclient_name}"
    )

    # Verify storageclient status
    assert storageclient["status"]["phase"] == "Connected"
    log.info(f"Storageclient {storageclient_name} is Connected.")

    if verify_sc:
        # Verify storageclassclaims and the presence of storageclasses
        verify_storageclient_storageclass_claims(storageclient_name)
        log.info(
            f"Verified the status of the storageclassclaims associated with the storageclient {storageclient_name}"
        )


def get_storageclassclaims_of_storageclient(storageclient_name):
    """
    Get all storageclassclaims associated with a storageclient

    Args:
        storageclient_name (str): Name of the storageclient

    Returns:
         List: OCS objects of kind Storageclassclaim

    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    sc_claims = get_all_storageclassclaims()
    for sc_claim in sc_claims:
        if ocs_version >= version.VERSION_4_16:
            sc_claim.data["spec"]["storageClient"] == storageclient_name
        else:
            sc_claim.data["spec"]["storageClient"]["name"] == storageclient_name
    return sc_claim


def get_all_storageclassclaims(namespace=None):
    """
    Get all storageclassclaims/storageclaims
    <storageclassclaim changed to storageclaim from ODF 4.16 >

    Returns:
         List: OCS objects of kind Storageclassclaim/storageclaim

    """
    if not namespace:
        namespace = config.ENV_DATA["cluster_namespace"]

    ocs_version = version.get_semantic_ocs_version_from_config()
    if ocs_version >= version.VERSION_4_16:
        sc_claim_obj = OCP(kind=constants.STORAGECLAIM, namespace=namespace)
    else:
        sc_claim_obj = OCP(kind=constants.STORAGECLASSCLAIM, namespace=namespace)
    sc_claims_data = sc_claim_obj.get(retry=6, wait=30)["items"]
    log.info(f"storage claims: {sc_claims_data}")
    return [OCS(**claim_data) for claim_data in sc_claims_data]


def verify_storageclient_storageclass_claims(storageclient):
    """
    Verify the status of storageclassclaims and the presence of the storageclass associated with the storageclient

    Args:
        storageclient_name (str): Name of the storageclient

    """
    sc_claim_objs = get_storageclassclaims_of_storageclient(storageclient)
    log.info(f"storageclaims: {sc_claim_objs}")

    # Wait for the storageclassclaims to be in Ready state
    for sc_claim in sc_claim_objs:
        for claim_info in TimeoutSampler(timeout=1200, sleep=30, func=sc_claim.get):
            if claim_info.get("status", {}).get("phase") == constants.STATUS_READY:
                log.info(
                    f"Storageclassclaim {sc_claim.name} associated with the storageclient {storageclient} is "
                    f"{constants.STATUS_READY}"
                )
                break

        # Create OCS object of kind Storageclass
        sc_obj = create_ocs_object_from_kind_and_name(
            kind=constants.STORAGECLASS,
            resource_name=sc_claim.name,
        )
        # Verify that the Storageclass is present
        sc_obj.get()
        log.info(f"Verified Storageclassclaim and Storageclass {sc_claim.name}")


def verify_pods_in_managed_fusion_namespace():
    """
    Verify the status of pods in the namespace managed-fusion

    """
    log.info(
        f"Verifying the status of the pods in the namespace {constants.MANAGED_FUSION_NAMESPACE}"
    )
    pods_dict = {
        constants.MANAGED_FUSION_ALERTMANAGER_LABEL: 1,
        constants.MANAGED_FUSION_AWS_DATA_GATHER: 1,
        constants.MANAGED_CONTROLLER_LABEL: 1,
        constants.MANAGED_FUSION_PROMETHEUS_LABEL: 1,
        constants.PROMETHEUS_OPERATOR_LABEL: 1,
    }
    pod = OCP(kind=constants.POD, namespace=constants.MANAGED_FUSION_NAMESPACE)
    for label, count in pods_dict.items():
        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=label,
            resource_count=count,
            timeout=600,
        )
    log.info(
        f"Verified the status of the pods in the namespace {constants.MANAGED_FUSION_NAMESPACE}"
    )


def verify_faas_resources():
    """
    Verify the presence and status of resources in FaaS clusters

    """
    # Verify pods in managed-fusion namespace
    verify_pods_in_managed_fusion_namespace()

    # Verify secrets
    verify_faas_cluster_secrets()

    # Verify attributes specific to cluster types
    if config.ENV_DATA["cluster_type"].lower() == "provider":
        sc_obj = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        sc_data = sc_obj.get()["items"][0]
        verify_faas_provider_storagecluster(sc_data)
        verify_faas_provider_resources()
        verify_provider_topology()
    else:
        verify_storageclient()
        verify_faas_consumer_resources()

    # Verify security
    if config.ENV_DATA["cluster_type"].lower() == "consumer":
        verify_client_operator_security()


def verify_faas_provider_resources():
    """
    Verify resources specific to FaaS provider cluster

    1. Verify CSV phase
    2. Verify ocs-provider-server pod is Running
    3. Verify ocs-metrics-exporter pod is Running
    4. Verify that Cephcluster is Ready and hostNetworking is True
    5. Verify that the security groups are set up correctly
    6. Check the presence of catalogsource and its state
    7. Check the presence of subscription and its health
    8. Check that mon PVCs have gp3-csi storageclass
    9. Check managedFusionOffering release, usableCapacityInTiB and onboardingValidationKey
    10. Verify the version of Prometheus
    11. Verify aws volumes
    12. Verify configmaps

    """
    # Verify CSV phase
    for csv_prefix in {
        constants.MANAGED_FUSION_AGENT,
        constants.OCS_CSV_PREFIX,
        constants.OSE_PROMETHEUS_OPERATOR,
    }:
        csvs = csv.get_csvs_start_with_prefix(
            csv_prefix, config.ENV_DATA["cluster_namespace"]
        )
        assert (
            len(csvs) == 1
        ), f"Unexpected number of CSVs with name prefix {csv_prefix}: {len(csvs)}"
        csv_name = csvs[0]["metadata"]["name"]
        csv_obj = csv.CSV(
            resource_name=csv_name, namespace=config.ENV_DATA["cluster_namespace"]
        )
        log.info(f"Verify that the CSV {csv_name} is in Succeeded phase.")
        csv_obj.wait_for_phase(phase="Succeeded", timeout=600)

    # Verify ocs-provider-server pod is Running
    pod_obj = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    pod_obj.wait_for_resource(
        condition="Running", selector=constants.PROVIDER_SERVER_LABEL, resource_count=1
    )
    # Verify ocs-metrics-exporter pod is Running
    pod_obj.wait_for_resource(
        condition="Running", selector=constants.OCS_METRICS_EXPORTER, resource_count=1
    )

    # Verify that Cephcluster is Ready and hostNetworking is True
    cephcluster = OCP(
        kind=constants.CEPH_CLUSTER,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.CEPH_CLUSTER_NAME,
    )
    cephcluster._has_phase = True
    log.info("Waiting for Cephcluster to be Ready")
    cephcluster.wait_for_phase(phase=constants.STATUS_READY, timeout=600)
    cephcluster_yaml = cephcluster.get()
    log.info("Verifying that Cephcluster's hostNetworking is True")
    assert cephcluster_yaml["spec"]["network"][
        "hostNetwork"
    ], f"hostNetwork is {cephcluster_yaml['spec']['network']['hostNetwork']} in Cephcluster"

    # Verify that the security groups are set up correctly
    assert verify_worker_nodes_security_groups()

    # Check the presence of catalogsource and its state
    catsrc = OCP(
        kind=constants.CATSRC,
        namespace=config.ENV_DATA["service_namespace"],
        resource_name="managed-fusion-catsrc",
    )
    catsrc_info = catsrc.get()
    log.info(f"Catalogsource: {catsrc_info}")
    assert catsrc_info["spec"]["displayName"].startswith("Managed Fusion Agent")
    assert catsrc_info["status"]["connectionState"]["lastObservedState"] == "READY"

    # Check the presence of subscription and its health
    subscr = OCP(
        kind="subscription",
        namespace=config.ENV_DATA["service_namespace"],
        selector="operators.coreos.com/managed-fusion-agent.managed-fusion",
    )
    subscr_info = subscr.get().get("items")[0]
    assert subscr_info["spec"]["name"] == "managed-fusion-agent"
    subscr_health = subscr_info["status"]["catalogHealth"]
    for sub_ref in subscr_health:
        log.info(
            f"Verifying Healthy state of subscription {sub_ref['catalogSourceRef']['name']}"
        )
        assert sub_ref["healthy"]

    # Check that mon PVCs have gp3-csi storageclass
    monpvcs = OCP(
        kind=constants.PVC,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.MON_APP_LABEL,
    )
    for pvc in monpvcs.get().get("items"):
        log.info(f"Verifying storageclass of mon PVC {pvc['metadata']['name']}")
        assert pvc["spec"]["storageClassName"] == constants.GP3_CSI, (
            f"Storage class of PVC {pvc['metadata']['name']} is "
            f"{pvc['spec']['storageClassName']}. It should be {constants.GP3_CSI}."
        )

    # Check that OSD PVCs have gp3-based storageclass
    osdpvcs = OCP(
        kind=constants.PVC,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.OSD_PVC_GENERIC_LABEL,
    )
    for pvc in osdpvcs.get().get("items"):
        log.info(f"Verifying storageclass of OSD PVC {pvc['metadata']['name']}")
        assert pvc["spec"]["storageClassName"] == constants.DEFAULT_OCS_STORAGECLASS, (
            f"Storage class of PVC {pvc['metadata']['name']} is "
            f"{pvc['spec']['storageClassName']}. "
            f"It should be {constants.DEFAULT_OCS_STORAGECLASS}"
        )
    defaultsc = OCP(
        kind=constants.STORAGECLASS,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.DEFAULT_OCS_STORAGECLASS,
    )
    defaultsc_info = defaultsc.get()
    assert defaultsc_info["parameters"]["type"] == constants.GP3, (
        f"Type of OSD PVC's storage class is {defaultsc_info['parameters']['type']}. "
        f"It should be {constants.GP3}"
    )

    # Check managedFusionOffering release, usableCapacityInTiB and onboardingValidationKey
    offering = OCP(
        kind="managedFusionOffering",
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    offering_info = offering.get().get("items")[0]
    # Check managedFusionOffering release
    log.info("Verifying managedFusionOffering version")
    odf_version = get_semantic_ocs_version_from_config()
    log.info(f"ODF version {odf_version}")
    log.info(f"Offering version {offering_info['spec']['release']}")
    assert offering_info["spec"]["release"] == str(odf_version)

    # Check managedFusionOffering usableCapacityInTiB
    log.info("Verifying managedFusionOffering usableCapacityInTiB")
    for line in offering_info["spec"]["config"].split("\n"):
        if "usableCapacityInTiB" in line:
            capacity = line.split()[-1]
        if "onboardingValidationKey" in line:
            onboarding_validation_key = line.split()[-1]
    assert (
        capacity == config.ENV_DATA["size"]
    ), f"usableCapacityInTiB expected value is {config.ENV_DATA['size']}. Actual value is {capacity}."

    # Check managedFusionOffering onboardingValidationKey
    log.info("Verifying managedFusionOffering onboardingValidationKey")
    assert len(onboarding_validation_key) > 700

    # Verify the version of Prometheus
    prometheus_csv = csv.get_csvs_start_with_prefix(
        constants.OSE_PROMETHEUS_OPERATOR, config.ENV_DATA["cluster_namespace"]
    )
    prometheus_version = prometheus_csv[0]["spec"]["version"]
    log.info("Verifying Prometheus version")
    assert prometheus_version == config.ENV_DATA["prometheus_version"], (
        f"Prometheus version is {prometheus_version} "
        f"but it should be {config.ENV_DATA['prometheus_version']}"
    )
    # Verify aws volumes
    verify_provider_aws_volumes()

    # Verify configmaps
    configmaps_obj = OCP(
        kind=constants.CONFIGMAP,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    configmap_names = [
        constants.ROOK_CEPH_MON_ENDPOINTS,
        constants.ROOK_CONFIG_OVERRIDE_CONFIGMAP,
        constants.ROOK_OPERATOR_CONFIGMAP,
        constants.ROOK_CEPH_CSI_CONFIG,
        constants.PDBSTATEMAP,
        constants.CSI_MAPPING_CONFIG,
        constants.OCS_OPERATOR_CONFIG,
        constants.METRICS_EXPORTER_CONF,
    ]
    for configmap_name in configmap_names:
        log.info(f"Verifying existence of {configmap_name} config map")
        assert configmaps_obj.is_exist(
            resource_name=configmap_name
        ), f"Configmap {configmap_name} does not exist in the cluster namespace"


def verify_provider_aws_volumes():
    """
    Verify provider AWS volumes:
    1. Volumes for OSD have size 4096
    2. Volumes for OSD have IOPS 12000
    3. Namespace should be fusion-storage
    """
    aws_obj = AWS()
    osd_pvc_objs = get_all_pvc_objs(
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.OSD_PVC_GENERIC_LABEL,
    )
    for osd_pvc_obj in osd_pvc_objs:
        log.info(f"Verifying AWS volume for {osd_pvc_obj.name} PVC")
        osd_volume_id = aws_obj.get_volumes_by_tag_pattern(
            constants.AWS_VOL_PVC_NAME_TAG, osd_pvc_obj.name
        )[0]["id"]
        log.info(f"AWS volume id: {osd_volume_id}")
        aws_obj.check_volume_attributes(
            volume_id=osd_volume_id,
            name_end=osd_pvc_obj.backed_pv,
            size=constants.AWS_VOL_OSD_SIZE,
            iops=constants.AWS_VOL_OSD_IOPS,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
    mon_pvc_objs = get_all_pvc_objs(
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.MON_APP_LABEL,
    )
    for mon_pvc_obj in mon_pvc_objs:
        log.info(
            f"Verifying AWS volume for {mon_pvc_obj.name} PVC "
            f", PV name {mon_pvc_obj.backed_pv}"
        )
        mon_volume_id = aws_obj.get_volumes_by_tag_pattern(
            constants.AWS_VOL_PV_NAME_TAG, mon_pvc_obj.backed_pv
        )[0]["id"]
        log.info(f"AWS volume id: {mon_volume_id}")
        aws_obj.check_volume_attributes(
            volume_id=mon_volume_id,
            name_end=mon_pvc_obj.backed_pv,
            size=constants.AWS_VOL_MON_SIZE,
            iops=constants.AWS_VOL_MON_IOPS,
            namespace=config.ENV_DATA["cluster_namespace"],
        )


def verify_faas_consumer_resources():
    """
    Verify resources specific to FaaS consumer

    1. Verify CSV phase
    2. Verify client endpoint
    3. Check that there's no storagecluster

    """

    # Verify CSV phase
    for csv_prefix in {
        constants.MANAGED_FUSION_AGENT,
        constants.OCS_CLIENT_OPERATOR,
        constants.ODF_CSI_ADDONS_OPERATOR,
        constants.OSE_PROMETHEUS_OPERATOR,
    }:
        csvs = csv.get_csvs_start_with_prefix(
            csv_prefix, config.ENV_DATA["cluster_namespace"]
        )
        assert (
            len(csvs) == 1
        ), f"Unexpected number of CSVs with name prefix {csv_prefix}: {len(csvs)}"
        csv_name = csvs[0]["metadata"]["name"]
        csv_obj = csv.CSV(
            resource_name=csv_name, namespace=config.ENV_DATA["cluster_namespace"]
        )
        log.info(f"Verify that the CSV {csv_name} is in Succeeded phase.")
        csv_obj.wait_for_phase(phase="Succeeded", timeout=600)

    # Verify client endpoint
    client_endpoint = OCP(
        kind=constants.ENDPOINTS,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector="operators.coreos.com/ocs-client-operator.fusion-storage",
    )
    client_ep_yaml = client_endpoint.get().get("items")[0]
    log.info("Verifying that The client endpoint has an IP address")
    ep_ip = client_ep_yaml["subsets"][0]["addresses"][0]["ip"]
    log.info(f"Client endpoint IP is {ep_ip}")
    assert re.match("\\d+(\\.\\d+){3}", ep_ip)

    # Check that there's no storagecluster on consumer
    sc_obj = OCP(
        kind=constants.STORAGECLUSTER,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    log.info("Verifying there's no storagecluster")
    assert not sc_obj.get(
        resource_name=constants.STORAGECLUSTER, dont_raise=True
    ), f"Storagecluster found on consumer: {sc_obj.get()['items']}"


def verify_faas_cluster_secrets():
    """
    Verify the secrets present in FaaS cluster

    """
    secret_cluster_namespace_obj = OCP(
        kind=constants.SECRET, namespace=config.ENV_DATA["cluster_namespace"]
    )
    secret_service_namespace_obj = OCP(
        kind=constants.SECRET, namespace=config.ENV_DATA["service_namespace"]
    )
    managed_fusion_secret_names = [
        "alertmanager-managed-fusion-alertmanager-generated",
        "managed-fusion-agent-config",
        "managed-fusion-alertmanager-secret",
        "prometheus-managed-fusion-prometheus",
    ]
    for secret_name in managed_fusion_secret_names:
        assert secret_service_namespace_obj.is_exist(
            resource_name=secret_name
        ), f"Secret {secret_name} does not exist in {config.ENV_DATA['service_namespace']} namespace"

    if config.ENV_DATA["cluster_type"].lower() == "provider":
        secret_names = [
            constants.MANAGED_ONBOARDING_SECRET,
            constants.MANAGED_PROVIDER_SERVER_SECRET,
            constants.MANAGED_MON_SECRET,
        ]
        for secret_name in secret_names:
            assert secret_cluster_namespace_obj.is_exist(
                resource_name=secret_name
            ), f"Secret {secret_name} does not exist in {config.ENV_DATA['cluster_namespace']} namespace"


def verify_faas_provider_storagecluster(sc_data):
    """
    Verify provider storagecluster

    1. allowRemoteStorageConsumers: true
    2. hostNetwork: true
    3. matchExpressions:
        key: node-role.kubernetes.io/worker
        operator: Exists
        key: node-role.kubernetes.io/infra
        operator: DoesNotExist
    4. storageProviderEndpoint
    5. annotations:
        uninstall.ocs.openshift.io/cleanup-policy: delete
        uninstall.ocs.openshift.io/mode: graceful
    6. Check the storagecluster resources limits and requests are valid
    7. Verify the Faas provider storagecluster storages

    Args:
        sc_data (dict): storagecluster data dictionary

    """
    log.info(
        f"allowRemoteStorageConsumers: {sc_data['spec']['allowRemoteStorageConsumers']}"
    )
    assert sc_data["spec"]["allowRemoteStorageConsumers"]
    log.info(f"hostNetwork: {sc_data['spec']['hostNetwork']}")
    assert sc_data["spec"]["hostNetwork"]
    expressions = sc_data["spec"]["labelSelector"]["matchExpressions"]
    for item in expressions:
        log.info(f"Verifying {item}")
        if item["key"] == "node-role.kubernetes.io/worker":
            assert item["operator"] == "Exists"
        else:
            assert item["operator"] == "DoesNotExist"
    log.info(f"storageProviderEndpoint: {sc_data['status']['storageProviderEndpoint']}")
    assert re.match(
        "(\\w+\\-\\w+\\.\\w+\\-\\w+\\-\\w+\\.elb.amazonaws.com):50051",
        sc_data["status"]["storageProviderEndpoint"],
    )
    annotations = sc_data["metadata"]["annotations"]
    log.info(f"Annotations: {annotations}")
    assert annotations["uninstall.ocs.openshift.io/cleanup-policy"] == "delete"
    assert annotations["uninstall.ocs.openshift.io/mode"] == "graceful"

    log.info("Check the storagecluster resources limits and requests are valid")
    resources_names = constants.FUSION_SC_RESOURCES_NAMES
    sc_data_resources = sc_data["spec"]["resources"]
    for resource_name in resources_names:
        resource_limits = sc_data_resources[resource_name]["limits"]
        log.info(f"Resource '{resource_name}' limits = {resource_limits}")
        # Simple verification to check that the limits values start with numbers
        assert re.match("(\\d+)", resource_limits["cpu"])
        assert re.match("(\\d+)", resource_limits["memory"])

        resource_requests = sc_data_resources[resource_name]["requests"]
        log.info(f"Resource '{resource_name}' requests = {resource_requests}")
        # Simple verification to check that the requests values start with numbers
        assert re.match("(\\d+)", resource_requests["cpu"])
        assert re.match("(\\d+)", resource_requests["memory"])

    log.info("Finish verifying the storagecluster resources limits and requests")
    verify_faas_provider_storagecluster_storages(sc_data)


def verify_client_operator_security():
    """
    Check ocs-client-operator-controller-manager permissions

    1. Verify `runAsUser` is not 0
    2. Verify `SecurityContext.allowPrivilegeEscalation` is set to false
    3. Verify `SecurityContext.capabilities.drop` contains ALL

    """
    pod_obj = OCP(
        kind=constants.POD,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.MANAGED_CONTROLLER_LABEL,
    )
    client_operator_yaml = pod_obj.get().get("items")[0]
    containers = client_operator_yaml["spec"]["containers"]
    for container in containers:
        log.info(f"Checking container {container['name']}")
        userid = container["securityContext"]["runAsUser"]
        log.info(f"runAsUser is {userid}. Verifying it is not 0")
        assert userid > 0
        escalation = container["securityContext"]["allowPrivilegeEscalation"]
        log.info("Verifying allowPrivilegeEscalation is False")
        assert not escalation
        dropped_capabilities = container["securityContext"]["capabilities"]["drop"]
        log.info(f"Dropped capabilities: {dropped_capabilities}")
        assert "ALL" in dropped_capabilities


def verify_faas_provider_storagecluster_storages(sc_data):
    """
    Verify the Faas provider storagecluster storages

    1. Check the storagecluster backingStorageClasses.
    2. Check that the default ocs storage class is exist in the backingStorageClasses.
    3. Check that the type of the default ocs storage class is "gp3".
    4. Check the defaultStorageProfile value
    5. Check that the default storage profile is found in the StorageProfiles.
    6. Check that the values in the storage profile are correct

    Args:
        sc_data (dict): The storagecluster data

    """
    # Check the backingStorageClasses
    log.info(f"sc backingStorageClasses = {sc_data['spec']['backingStorageClasses']}")
    backing_storage_classes = sc_data["spec"]["backingStorageClasses"]
    assert backing_storage_classes, "Didn't find any backingStorageClasses"
    # Search for the default ocs storage class
    default_ocs_sc_name = "default-ocs-storage-class"
    default_ocs_sc = None
    for storage_class in backing_storage_classes:
        if storage_class["metadata"].get("name") == default_ocs_sc_name:
            default_ocs_sc = storage_class
            break

    assert default_ocs_sc, f"The storage class {default_ocs_sc_name} does not exist"
    log.info(f"Found the storage class {default_ocs_sc_name}")
    # Check the type of the default ocs storage class
    default_ocs_sc_type = storage_class["parameters"]["type"]
    expected_type = "gp3"
    log.info(f"The type of '{default_ocs_sc_name}' is {default_ocs_sc_type}")
    assert (
        default_ocs_sc_type == expected_type
    ), f"The default ocs sc type is '{default_ocs_sc_type}' and not the expected type {expected_type}"

    # Check the defaultStorageProfile value
    default_sp_name = constants.FUSION_SC_DEFAULT_STORAGE_PROFILE["name"]
    log.info(
        f"The defaultStorageProfile value is {sc_data['spec']['defaultStorageProfile']}"
    )
    assert sc_data["spec"]["defaultStorageProfile"] == default_sp_name

    # Check the StorageProfiles
    log.info(f"sc storageProfiles = {sc_data['spec']['storageProfiles']}")
    storage_profiles = sc_data["spec"]["storageProfiles"]
    assert storage_profiles, "Didn't find any storageProfiles"
    # Search for the default storage profile
    default_sp = None
    for storage_profile in storage_profiles:
        if storage_profile["name"] == default_sp_name:
            default_sp = storage_profile
            break

    assert default_sp, f"The storage profile {default_sp_name} does not exist"
    log.info(f"Found the storage profile {default_sp_name}")

    log.info(f"Check the values in the storage profile {default_sp_name}")
    expected_default_sp = constants.FUSION_SC_DEFAULT_STORAGE_PROFILE
    for key, expected_key in zip(default_sp.keys(), expected_default_sp.keys()):
        assert key == expected_key
        assert default_sp[key] == expected_default_sp[expected_key]

    log.info(f"The values in the storage profile {default_sp_name} are correct")
    log.info("Finish Verifying all the storages in the provider faas storagecluster")


@switch_to_orig_index_at_last
def create_toolbox_on_faas_consumer():
    """
    Create toolbox on FaaS consumer cluster

    """
    current_cluster = config.cluster_ctx
    assert (
        current_cluster.ENV_DATA.get("cluster_type").lower()
        == constants.MS_CONSUMER_TYPE
    ), "This function is applicable for consumer cluster only"

    namespace = config.ENV_DATA["cluster_namespace"]

    # Switch to provider cluster and get the required secret, configmap and tools deployment
    try:
        config.switch_to_provider()
    except ClusterNotFoundException:
        log.warning(
            "Provider cluster is not available. Skipping toolbox creation on consumer cluster."
        )
        return

    secret_obj = OCP(
        kind=constants.SECRET, namespace=namespace, resource_name="rook-ceph-mon"
    )
    secret_data = secret_obj.get()

    configmap_obj = OCP(
        kind=constants.CONFIGMAP,
        namespace=namespace,
        resource_name=constants.ROOK_CEPH_MON_ENDPOINTS,
    )
    configmap_data = configmap_obj.get()

    deployment_obj = OCP(
        kind=constants.DEPLOYMENT,
        namespace=namespace,
        resource_name="rook-ceph-tools",
    )
    tools_deployment_data = deployment_obj.get()

    # Switch to current consumer cluster
    config.switch_ctx(current_cluster.MULTICLUSTER["multicluster_index"])

    # Remove the values that are not relevant
    for data in [secret_data, configmap_data, tools_deployment_data]:
        del data["metadata"]["ownerReferences"]
        del data["metadata"]["uid"]
    del tools_deployment_data["spec"]["template"]["spec"]["containers"][0][
        "securityContext"
    ]
    del tools_deployment_data["status"]

    # Create secret, configmap and tools deployment on consumer cluster
    create_resource(**secret_data)
    create_resource(**configmap_data)
    create_resource(**tools_deployment_data)

    # Wait for tools pod to reach Running state
    toolbox_pod = OCP(kind=constants.POD, namespace=namespace)
    toolbox_pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.TOOL_APP_LABEL,
        resource_count=1,
        timeout=180,
    )
