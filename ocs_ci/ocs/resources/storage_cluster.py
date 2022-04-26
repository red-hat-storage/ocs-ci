"""
StorageCluster related functionalities
"""
import copy
import logging
import re
import tempfile
import yaml

from jsonschema import validate

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults, ocp, managedservice
from ocs_ci.ocs.exceptions import (
    ResourceNotFoundError,
    UnsupportedFeatureError,
    PVNotSufficientException,
)
from ocs_ci.ocs.ocp import get_images, OCP
from ocs_ci.ocs.resources import csv, deployment
from ocs_ci.ocs.resources.ocs import get_ocs_csv
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    get_osd_pods,
    get_mon_pods,
    get_mds_pods,
    get_mgr_pods,
    get_rgw_pods,
    get_plugin_pods,
    get_cephfsplugin_provisioner_pods,
    get_rbdfsplugin_provisioner_pods,
)
from ocs_ci.ocs.resources.pv import check_pvs_present_for_ocs_expansion
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs
from ocs_ci.ocs.node import (
    get_osds_per_node,
    add_new_disk_for_vsphere,
    get_osd_running_nodes,
    get_encrypted_osd_devices,
    verify_worker_nodes_security_groups,
)
from ocs_ci.helpers.helpers import get_secret_names
from ocs_ci.utility import (
    localstorage,
    utils,
    templating,
    kms as KMS,
    version,
)
from ocs_ci.utility.rgwutils import get_rgw_count
from ocs_ci.utility.utils import run_cmd


log = logging.getLogger(__name__)


class StorageCluster(OCP):
    """
    This class represent StorageCluster and contains all related
    methods we need to do with StorageCluster.
    """

    _has_phase = True

    def __init__(self, resource_name="", *args, **kwargs):
        """
        Constructor method for StorageCluster class

        Args:
            resource_name (str): Name of StorageCluster

        """
        super(StorageCluster, self).__init__(
            resource_name=resource_name, kind="StorageCluster", *args, **kwargs
        )


def ocs_install_verification(
    timeout=600,
    skip_osd_distribution_check=False,
    ocs_registry_image=None,
    post_upgrade_verification=False,
    version_before_upgrade=None,
):
    """
    Perform steps necessary to verify a successful OCS installation

    Args:
        timeout (int): Number of seconds for timeout which will be used in the
            checks used in this function.
        skip_osd_distribution_check (bool): If true skip the check for osd
            distribution.
        ocs_registry_image (str): Specific image to check if it was installed
            properly.
        post_upgrade_verification (bool): Set to True if this function is
            called after upgrade.
        version_before_upgrade (float): Set to OCS version before upgrade

    """
    from ocs_ci.ocs.node import get_nodes
    from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs
    from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_all_pods
    from ocs_ci.ocs.cluster import validate_cluster_on_pvc
    from ocs_ci.ocs.resources.fips import check_fips_enabled

    number_of_worker_nodes = len(get_nodes())
    namespace = config.ENV_DATA["cluster_namespace"]
    log.info("Verifying OCS installation")
    if config.ENV_DATA.get("disable_components"):
        for component in config.ENV_DATA["disable_components"]:
            config.COMPONENTS[f"disable_{component}"] = True
    disable_noobaa = config.COMPONENTS["disable_noobaa"]
    disable_rgw = config.COMPONENTS["disable_rgw"]
    disable_blockpools = config.COMPONENTS["disable_blockpools"]
    disable_cephfs = config.COMPONENTS["disable_cephfs"]
    managed_service = (
        config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS
    )
    ocs_version = version.get_semantic_ocs_version_from_config()
    external = config.DEPLOYMENT["external_mode"] or (
        managed_service and config.ENV_DATA["cluster_type"].lower() == "consumer"
    )

    # Basic Verification for cluster
    basic_verification(ocs_registry_image)

    # Verify pods in running state and proper counts
    log.info("Verifying pod states and counts")
    storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
    storage_cluster = StorageCluster(
        resource_name=storage_cluster_name,
        namespace=namespace,
    )
    pod = OCP(kind=constants.POD, namespace=namespace)
    if not external:
        osd_count = int(
            storage_cluster.data["spec"]["storageDeviceSets"][0]["count"]
        ) * int(storage_cluster.data["spec"]["storageDeviceSets"][0]["replica"])
    rgw_count = None
    if config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS:
        if not disable_rgw:
            rgw_count = get_rgw_count(
                f"{ocs_version}", post_upgrade_verification, version_before_upgrade
            )

    min_eps = constants.MIN_NB_ENDPOINT_COUNT_POST_DEPLOYMENT

    if config.ENV_DATA.get("platform") == constants.IBM_POWER_PLATFORM:
        min_eps = 1

    nb_db_label = (
        constants.NOOBAA_DB_LABEL_46_AND_UNDER
        if ocs_version < version.VERSION_4_7
        else constants.NOOBAA_DB_LABEL_47_AND_ABOVE
    )
    resources_dict = {
        nb_db_label: 1,
        constants.OCS_OPERATOR_LABEL: 1,
        constants.OPERATOR_LABEL: 1,
        constants.NOOBAA_OPERATOR_POD_LABEL: 1,
        constants.NOOBAA_CORE_POD_LABEL: 1,
        constants.NOOBAA_ENDPOINT_POD_LABEL: min_eps,
    }

    if managed_service and config.ENV_DATA["cluster_type"].lower() == "provider":
        resources_dict.update(
            {
                constants.MON_APP_LABEL: 3,
                constants.OSD_APP_LABEL: osd_count,
                constants.MGR_APP_LABEL: 1,
                constants.MDS_APP_LABEL: 2,
            }
        )
    elif managed_service and config.ENV_DATA["cluster_type"].lower() == "consumer":
        resources_dict.update(
            {
                constants.CSI_CEPHFSPLUGIN_LABEL: number_of_worker_nodes,
                constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL: 2,
                constants.CSI_RBDPLUGIN_LABEL: number_of_worker_nodes,
                constants.CSI_RBDPLUGIN_PROVISIONER_LABEL: 2,
            }
        )
    elif not config.DEPLOYMENT["external_mode"]:
        resources_dict.update(
            {
                constants.MON_APP_LABEL: 3,
                constants.CSI_CEPHFSPLUGIN_LABEL: number_of_worker_nodes,
                constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL: 2,
                constants.CSI_RBDPLUGIN_LABEL: number_of_worker_nodes,
                constants.CSI_RBDPLUGIN_PROVISIONER_LABEL: 2,
                constants.OSD_APP_LABEL: osd_count,
                constants.MGR_APP_LABEL: 1,
                constants.MDS_APP_LABEL: 2,
                constants.RGW_APP_LABEL: rgw_count,
            }
        )

    if ocs_version >= version.VERSION_4_9:
        resources_dict.update(
            {
                constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL: 1,
            }
        )

    for label, count in resources_dict.items():
        if label == constants.RGW_APP_LABEL:
            if (
                not config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS
                or managed_service
                or disable_rgw
            ):
                continue
        if "noobaa" in label and (disable_noobaa or managed_service):
            continue
        if "mds" in label and disable_cephfs:
            continue

        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=label,
            resource_count=count,
            timeout=timeout,
        )

    # Verify StorageClasses (1 ceph-fs, 1 ceph-rbd)
    log.info("Verifying storage classes")
    storage_class = OCP(kind=constants.STORAGECLASS, namespace=namespace)
    storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
    required_storage_classes = {
        f"{storage_cluster_name}-cephfs",
        f"{storage_cluster_name}-ceph-rbd",
    }
    skip_storage_classes = set()
    if disable_cephfs:
        skip_storage_classes.update(
            {
                f"{storage_cluster_name}-cephfs",
            }
        )
    if disable_blockpools:
        skip_storage_classes.update(
            {
                f"{storage_cluster_name}-ceph-rbd",
            }
        )
    required_storage_classes = required_storage_classes.difference(skip_storage_classes)

    if config.DEPLOYMENT["external_mode"]:
        required_storage_classes.update(
            {
                f"{storage_cluster_name}-ceph-rgw",
                f'{config.ENV_DATA["cluster_namespace"]}.noobaa.io',
            }
        )
    storage_classes = storage_class.get()
    storage_class_names = {
        item["metadata"]["name"] for item in storage_classes["items"]
    }
    # required storage class names should be observed in the cluster under test
    missing_scs = required_storage_classes.difference(storage_class_names)
    if len(missing_scs) > 0:
        log.error("few storage classess are not present: %s", missing_scs)
    assert list(missing_scs) == []

    # Verify OSDs are distributed
    if not external:
        if not skip_osd_distribution_check:
            log.info("Verifying OSDs are distributed evenly across worker nodes")
            ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
            osds = ocp_pod_obj.get(selector=constants.OSD_APP_LABEL)["items"]
            deviceset_count = get_deviceset_count()
            node_names = [osd["spec"]["nodeName"] for osd in osds]
            for node in node_names:
                assert (
                    not node_names.count(node) > deviceset_count
                ), "OSD's are not distributed evenly across worker nodes"

    # Verify that CSI driver object contains provisioner names
    log.info("Verifying CSI driver object contains provisioner names.")
    csi_driver = OCP(kind="CSIDriver")
    csi_drivers = {item["metadata"]["name"] for item in csi_driver.get()["items"]}
    if not managed_service or config.ENV_DATA["cluster_type"].lower() != "provider":
        assert defaults.CSI_PROVISIONERS.issubset(csi_drivers)

    # Verify node and provisioner secret names in storage class
    log.info("Verifying node and provisioner secret names in storage class.")
    if config.DEPLOYMENT["external_mode"]:
        sc_rbd = storage_class.get(
            resource_name=constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
        )
        sc_cephfs = storage_class.get(
            resource_name=(constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS)
        )
    else:
        if not disable_blockpools:
            sc_rbd = storage_class.get(resource_name=constants.DEFAULT_STORAGECLASS_RBD)
        if not disable_cephfs:
            sc_cephfs = storage_class.get(
                resource_name=constants.DEFAULT_STORAGECLASS_CEPHFS
            )
    if not disable_blockpools:
        if managed_service and config.ENV_DATA["cluster_type"].lower() == "consumer":
            assert (
                "rook-ceph-client"
                in sc_rbd["parameters"]["csi.storage.k8s.io/node-stage-secret-name"]
            )
            assert (
                "rook-ceph-client"
                in sc_rbd["parameters"]["csi.storage.k8s.io/provisioner-secret-name"]
            )
        else:
            assert (
                sc_rbd["parameters"]["csi.storage.k8s.io/node-stage-secret-name"]
                == constants.RBD_NODE_SECRET
            )
            assert (
                sc_rbd["parameters"]["csi.storage.k8s.io/provisioner-secret-name"]
                == constants.RBD_PROVISIONER_SECRET
            )
    if not disable_cephfs:
        if managed_service and config.ENV_DATA["cluster_type"].lower() == "consumer":
            assert (
                "rook-ceph-client"
                in sc_cephfs["parameters"]["csi.storage.k8s.io/node-stage-secret-name"]
            )
            assert (
                "rook-ceph-client"
                in sc_cephfs["parameters"]["csi.storage.k8s.io/provisioner-secret-name"]
            )
        else:
            assert (
                sc_cephfs["parameters"]["csi.storage.k8s.io/node-stage-secret-name"]
                == constants.CEPHFS_NODE_SECRET
            )
            assert (
                sc_cephfs["parameters"]["csi.storage.k8s.io/provisioner-secret-name"]
                == constants.CEPHFS_PROVISIONER_SECRET
            )
    log.info("Verified node and provisioner secret names in storage class.")

    ct_pod = get_ceph_tools_pod()

    # https://github.com/red-hat-storage/ocs-ci/issues/3820
    # Verify ceph osd tree output
    if not (
        config.DEPLOYMENT.get("ui_deployment")
        or config.DEPLOYMENT["external_mode"]
        or managed_service
    ):
        log.info(
            "Verifying ceph osd tree output and checking for device set PVC names "
            "in the output."
        )
        if config.DEPLOYMENT.get("local_storage"):
            deviceset_pvcs = [osd.get_node() for osd in get_osd_pods()]
            # removes duplicate hostname
            deviceset_pvcs = list(set(deviceset_pvcs))
            if config.ENV_DATA.get("platform") == constants.BAREMETAL_PLATFORM or (
                config.ENV_DATA.get("flexy_deployment")
                and config.ENV_DATA.get("platform") == constants.AWS_PLATFORM
            ):
                deviceset_pvcs = [
                    deviceset.replace(".", "-") for deviceset in deviceset_pvcs
                ]
        else:
            deviceset_pvcs = [pvc.name for pvc in get_deviceset_pvcs()]

        osd_tree = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree", format="json")
        schemas = {
            "root": constants.OSD_TREE_ROOT,
            "rack": constants.OSD_TREE_RACK,
            "host": constants.OSD_TREE_HOST,
            "osd": constants.OSD_TREE_OSD,
            "region": constants.OSD_TREE_REGION,
            "zone": constants.OSD_TREE_ZONE,
        }
        schemas["host"]["properties"]["name"] = {"enum": deviceset_pvcs}
        for item in osd_tree["nodes"]:
            validate(instance=item, schema=schemas[item["type"]])
            if item["type"] == "host":
                deviceset_pvcs.remove(item["name"])
        assert not deviceset_pvcs, (
            f"These device set PVCs are not given in ceph osd tree output "
            f"- {deviceset_pvcs}"
        )
        log.info(
            "Verified ceph osd tree output. Device set PVC names are given in the "
            "output."
        )

    # TODO: Verify ceph osd tree output have osd listed as ssd
    # TODO: Verify ceph osd tree output have zone or rack based on AZ

    # verify caps for external cluster
    log.info("Verify CSI users and caps for external cluster")
    if config.DEPLOYMENT["external_mode"] and ocs_version >= version.VERSION_4_10:
        ceph_csi_users = copy.deepcopy(defaults.ceph_csi_users)
        ceph_auth_data = ct_pod.exec_cmd_on_pod("ceph auth ls -f json")
        for each in ceph_auth_data["auth_dump"]:
            if each["entity"] in defaults.ceph_csi_users:
                assert (
                    "osd blocklist" in each["caps"]["mon"]
                ), f"osd blocklist caps are not present for user {each['entity']}"
                ceph_csi_users.remove(each["entity"])
        assert (
            not ceph_csi_users
        ), f"CSI users {ceph_csi_users} not created in external cluster"
        log.debug("All CSI users exists and have expected caps")

    # Verify CSI snapshotter sidecar container is not present
    # if the OCS version is < 4.6
    if ocs_version < version.VERSION_4_6:
        log.info("Verifying CSI snapshotter is not present.")
        provisioner_pods = get_all_pods(
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            selector=[
                constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
                constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
            ],
        )
        for pod_obj in provisioner_pods:
            pod_info = pod_obj.get()
            for container, image in get_images(data=pod_info).items():
                assert ("snapshot" not in container) and ("snapshot" not in image), (
                    f"Snapshot container is present in {pod_obj.name} pod. "
                    f"Container {container}. Image {image}"
                )
        ocs_csv = get_ocs_csv()
        deployments = ocs_csv.get()["spec"]["install"]["spec"]["deployments"]
        rook_ceph_operator_deployment = [
            deployment_val
            for deployment_val in deployments
            if deployment_val["name"] == "rook-ceph-operator"
        ]
        assert {"name": "CSI_ENABLE_SNAPSHOTTER", "value": "false"} in (
            rook_ceph_operator_deployment[0]["spec"]["template"]["spec"]["containers"][
                0
            ]["env"]
        ), "CSI_ENABLE_SNAPSHOTTER value is not set to 'false'."
        log.info("Verified: CSI snapshotter is not present.")

    # Verify pool crush rule is with "type": "zone"
    if utils.get_az_count() == 3:
        log.info("Verifying pool crush rule is with type: zone")
        crush_dump = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd crush dump", format="")
        pool_names = [
            constants.METADATA_POOL,
            constants.DEFAULT_BLOCKPOOL,
            constants.DATA_POOL,
        ]
        crush_rules = [
            rule for rule in crush_dump["rules"] if rule["rule_name"] in pool_names
        ]
        for crush_rule in crush_rules:
            assert [
                item for item in crush_rule["steps"] if item.get("type") == "zone"
            ], f"{crush_rule['rule_name']} is not with type as zone"
        log.info("Verified - pool crush rule is with type: zone")
    # TODO: update pvc validation for managed services
    if not managed_service:
        log.info("Validate cluster on PVC")
        validate_cluster_on_pvc()

    # Verify ceph health
    log.info("Verifying ceph health")
    health_check_tries = 20
    health_check_delay = 30
    if post_upgrade_verification:
        # In case of upgrade with FIO we have to wait longer time to see
        # health OK. See discussion in BZ:
        # https://bugzilla.redhat.com/show_bug.cgi?id=1817727
        health_check_tries = 180
    assert utils.ceph_health_check(namespace, health_check_tries, health_check_delay)
    if config.ENV_DATA.get("fips"):
        # In case that fips is enabled when deploying,
        # a verification of the installation of it will run
        # on all running state pods
        check_fips_enabled()
    if config.ENV_DATA.get("encryption_at_rest"):
        osd_encryption_verification()
        if config.DEPLOYMENT.get("kms_deployment"):
            kms = KMS.get_kms_deployment()
            kms.post_deploy_verification()
            if config.ENV_DATA.get("VAULT_CA_ONLY", None):
                verify_kms_ca_only()

    storage_cluster_obj = get_storage_cluster()
    is_flexible_scaling = (
        storage_cluster_obj.get()["items"][0].get("spec").get("flexibleScaling", False)
    )
    if is_flexible_scaling is True:
        failure_domain = storage_cluster_obj.data["items"][0]["status"]["failureDomain"]
        assert failure_domain == "host", (
            f"The expected failure domain on cluster with flexible scaling is 'host',"
            f" the actaul failure domain is {failure_domain}"
        )

    if config.ENV_DATA.get("is_multus_enabled"):
        verify_multus_network()
    if managed_service:
        verify_managed_service_resources()


def mcg_only_install_verification(ocs_registry_image=None):
    """
    Verification for successful MCG only deployment

    Args:
        ocs_registry_image (str): Specific image to check if it was installed
            properly.

    """
    log.info("Verifying MCG Only installation")
    basic_verification(ocs_registry_image)


def basic_verification(ocs_registry_image=None):
    """
    Basic verification which is needed for Full deployment and MCG only deployment

    Args:
        ocs_registry_image (str): Specific image to check if it was installed
            properly.

    """
    verify_ocs_csv(ocs_registry_image)
    verify_storage_system()
    verify_storage_cluster()
    verify_noobaa_endpoint_count()
    verify_storage_cluster_images()


def verify_ocs_csv(ocs_registry_image=None):
    """
    OCS CSV verification ( succeeded state )

    Args:
        ocs_registry_image (str): Specific image to check if it was installed
            properly.

    """
    managed_service = (
        config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS
    )
    log.info("verifying ocs csv")
    # Verify if OCS CSV has proper version.
    ocs_csv = get_ocs_csv()
    csv_version = ocs_csv.data["spec"]["version"]
    ocs_version = version.get_semantic_ocs_version_from_config()
    if not managed_service:
        log.info(f"Check if OCS version: {ocs_version} matches with CSV: {csv_version}")
        assert (
            f"{ocs_version}" in csv_version
        ), f"OCS version: {ocs_version} mismatch with CSV version {csv_version}"
    # Verify if OCS CSV has the same version in provided CI build.
    ocs_registry_image = ocs_registry_image or config.DEPLOYMENT.get(
        "ocs_registry_image"
    )
    if ocs_registry_image and ocs_registry_image.endswith(".ci"):
        ocs_registry_image = ocs_registry_image.rsplit(":", 1)[1].split("-")[0]
        log.info(
            f"Check if OCS registry image: {ocs_registry_image} matches with "
            f"CSV: {csv_version}"
        )
        ignore_csv_mismatch = config.DEPLOYMENT.get("ignore_csv_mismatch")
        if ignore_csv_mismatch:
            log.info(
                "The possible mismatch will be ignored as you deployed "
                "the different version than the default version from the CSV"
            )
        else:
            assert ocs_registry_image in csv_version, (
                f"OCS registry image version: {ocs_registry_image} mismatch "
                f"with CSV version {csv_version}"
            )


def verify_storage_system():
    """
    Verify storage system status
    """
    managed_service = (
        config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS
    )
    ocs_version = version.get_semantic_ocs_version_from_config()
    if ocs_version >= version.VERSION_4_9 and not managed_service:
        log.info("Verifying storage system status")
        storage_system = OCP(
            kind=constants.STORAGESYSTEM, namespace=config.ENV_DATA["cluster_namespace"]
        )
        storage_system_data = storage_system.get()
        storage_system_status = {}
        for condition in storage_system_data["items"][0]["status"]["conditions"]:
            storage_system_status[condition["type"]] = condition["status"]
        log.debug(f"storage system status: {storage_system_status}")
        assert storage_system_status == constants.STORAGE_SYSTEM_STATUS, (
            f"Storage System status is not in expected state. Expected {constants.STORAGE_SYSTEM_STATUS}"
            f" but found {storage_system_status}"
        )


def verify_storage_cluster():
    """
    Verify storage cluster status
    """
    storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
    log.info("Verifying status of storage cluster: %s", storage_cluster_name)
    storage_cluster = StorageCluster(
        resource_name=storage_cluster_name,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    log.info(f"Check if StorageCluster: {storage_cluster_name} is in Succeeded phase")
    storage_cluster.wait_for_phase(phase="Ready", timeout=600)


def verify_noobaa_endpoint_count():
    """
    Verify noobaa endpoints
    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    disable_noobaa = config.COMPONENTS["disable_noobaa"]
    managed_service = (
        config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS
    )
    max_eps = (
        constants.MAX_NB_ENDPOINT_COUNT if ocs_version >= version.VERSION_4_6 else 1
    )
    if config.ENV_DATA.get("platform") == constants.IBM_POWER_PLATFORM:
        max_eps = 1
    if not (disable_noobaa or managed_service):
        nb_ep_pods = get_pods_having_label(
            label=constants.NOOBAA_ENDPOINT_POD_LABEL,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        )
        assert len(nb_ep_pods) <= max_eps, (
            f"The number of running NooBaa endpoint pods ({len(nb_ep_pods)}) "
            f"is greater than the maximum defined in the NooBaa CR ({max_eps})"
        )


def verify_storage_cluster_images():
    """
    Verify images in storage cluster
    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
    storage_cluster = StorageCluster(
        resource_name=storage_cluster_name,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    if ocs_version >= version.VERSION_4_7:
        log.info("Verifying images in storage cluster")
        verify_sc_images(storage_cluster)


def osd_encryption_verification():
    """
    Verify if OSD encryption at rest if successfully deployed on OCS

    Raises:
        UnsupportedFeatureError: OCS version is smaller than 4.6
        EnvironmentError: The OSD is not encrypted

    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    if ocs_version < version.VERSION_4_6:
        error_message = "Encryption at REST can be enabled only on OCS >= 4.6!"
        raise UnsupportedFeatureError(error_message)

    log.info("Get 'lsblk' command output on nodes where osd running")
    osd_node_names = get_osds_per_node()
    for worker_node in osd_node_names:
        lsblk_cmd = f"oc debug node/{worker_node} -- chroot /host lsblk"
        lsblk_out = run_cmd(lsblk_cmd)
        log.info(f"the output of lsblk command on node {worker_node} is:\n {lsblk_out}")
        osd_node_names[worker_node].append(lsblk_out)

    log.info("Verify 'lsblk' command results are as expected")
    for worker_node in osd_node_names:
        osd_number_per_node = len(osd_node_names[worker_node]) - 1
        lsblk_output = osd_node_names[worker_node][-1]
        lsblk_output_split = lsblk_output.split()
        log.info(f"lsblk split:{lsblk_output_split}")
        log.info(f"osd_node_names dictionary: {osd_node_names}")
        log.info(f"count crypt {lsblk_output_split.count('crypt')}")
        log.info(f"osd_number_per_node = {osd_number_per_node}")
        if lsblk_output_split.count("crypt") != osd_number_per_node:
            log.error(
                f"The output of lsblk command on node {worker_node} is not as expected:\n{lsblk_output}"
            )
            raise ValueError("OSD is not encrypted")

    # skip OCS 4.8 as the fix for luks header info is still not available on it
    if ocs_version > version.VERSION_4_6 and ocs_version != version.VERSION_4_8:
        log.info("Verify luks header label for encrypted devices")
        worker_nodes = get_osd_running_nodes()
        failures = 0
        failure_message = ""
        node_obj = OCP(kind="node")
        for node in worker_nodes:
            luks_devices = get_encrypted_osd_devices(node_obj, node)
            for luks_device_name in luks_devices:
                luks_device_name = luks_device_name.strip()
                log.info(
                    f"Checking luks header label on Luks device {luks_device_name} for node {node}"
                )
                cmd = "cryptsetup luksDump /dev/" + str(luks_device_name)
                cmd_out = node_obj.exec_oc_debug_cmd(node=node, cmd_list=[cmd])

                if "(no label)" in str(cmd_out) or "(no subsystem)" in str(cmd_out):
                    failures += 1
                    failure_message += (
                        f"\nNo label found on Luks header information for node {node}\n"
                    )

        if failures != 0:
            log.error(failure_message)
            raise ValueError("Luks header label is not found")
        log.info("Luks header info found for all the encrypted osds")


def verify_kms_ca_only():
    """
    Verify KMS deployment with only CA Certificate
    without Client Certificate and without Client Private Key

    """
    log.info("Verify KMS deployment with only CA Certificate")
    secret_names = get_secret_names()
    if (
        "ocs-kms-client-cert" in secret_names
        or "ocs-kms-client-key" in secret_names
        or "ocs-kms-ca-secret" not in secret_names
    ):
        raise ValueError(
            f"ocs-kms-client-cert and/or ocs-kms-client-key exist on ca_only mode {secret_names}"
        )


def add_capacity(osd_size_capacity_requested, add_extra_disk_to_existing_worker=True):
    """
    Add storage capacity to the cluster

    Args:
        osd_size_capacity_requested(int): Requested osd size capacity
        add_extra_disk_to_existing_worker(bool): Add Disk if True

    Returns:
        new storage device set count (int) : Returns True if all OSDs are in Running state

    Note:
    "StoragedeviceSets->count" represents the set of 3 OSDs.
    That is, if there are 3 OSDs in the system then count will be 1.
    If there are 6 OSDs then count is 2 and so on.
    By changing this value,we can add extra devices to the cluster.
    For example, if we want to expand the cluster by 3 more osds in a cluster that already has 3 osds,
    we can set count as 2. So, with each increase of count by 1,
    we get 3 OSDs extra added to the cluster.
    This is how we are going to 'add capacity' via automation.
    As we know that OCS has 3 way replica. That is, same data is placed in 3 OSDs.
    Because of this, the total usable capacity for apps from 3 OSDs
    will be the size of one OSD (all osds are of same size).
    If we want to add more capacity to the cluster then we need to add 3 OSDs of same size
    as that of the original OSD. add_capacity needs to accept the 'capacity_to_add' as an argument.
    From this we need to arrive at storagedeviceSets -> count and then
    "Patch" this count to get the required capacity to add.
    To do so, we use following formula:
    storageDeviceSets->count = (capacity reqested / osd capacity ) + existing count storageDeviceSets

    """
    lvpresent = None
    lv_set_present = None
    osd_size_existing = get_osd_size()
    device_sets_required = int(osd_size_capacity_requested / osd_size_existing)
    old_storage_devices_sets_count = get_deviceset_count()
    new_storage_devices_sets_count = int(
        device_sets_required + old_storage_devices_sets_count
    )
    is_lso = config.DEPLOYMENT.get("local_storage")
    if is_lso:
        lv_lvs_data = localstorage.check_local_volume_local_volume_set()
        if lv_lvs_data.get("localvolume"):
            lvpresent = True
        elif lv_lvs_data.get("localvolumeset"):
            lv_set_present = True
        else:
            log.info(lv_lvs_data)
            raise ResourceNotFoundError("No LocalVolume and LocalVolume Set found")
    platform = config.ENV_DATA.get("platform", "").lower()
    if lvpresent:
        ocp_obj = OCP(
            kind="localvolume", namespace=config.ENV_DATA["local_storage_namespace"]
        )
        localvolume_data = ocp_obj.get(resource_name="local-block")
        device_list = localvolume_data["spec"]["storageClassDevices"][0]["devicePaths"]
        final_device_list = localstorage.get_new_device_paths(
            device_sets_required, osd_size_capacity_requested
        )
        device_list.sort()
        final_device_list.sort()
        if device_list == final_device_list:
            raise ResourceNotFoundError("No Extra device found")
        param = f"""[{{ "op": "replace", "path": "/spec/storageClassDevices/0/devicePaths",
                                                 "value": {final_device_list}}}]"""
        log.info(f"Final device list : {final_device_list}")
        lvcr = localstorage.get_local_volume_cr()
        log.info("Patching Local Volume CR...")
        lvcr.patch(
            resource_name=lvcr.get()["items"][0]["metadata"]["name"],
            params=param.strip("\n"),
            format_type="json",
        )
        localstorage.check_pvs_created(
            int(len(final_device_list) / new_storage_devices_sets_count)
        )
    if lv_set_present:
        if check_pvs_present_for_ocs_expansion():
            log.info("Found Extra PV")
        else:
            if (
                platform == constants.VSPHERE_PLATFORM
                and add_extra_disk_to_existing_worker
            ):
                log.info("No Extra PV found")
                log.info("Adding Extra Disk to existing VSphere Worker node")
                add_new_disk_for_vsphere(sc_name=constants.LOCALSTORAGE_SC)
            else:
                raise PVNotSufficientException(
                    f"No Extra PV found in {constants.OPERATOR_NODE_LABEL}"
                )
    sc = get_storage_cluster()
    # adding the storage capacity to the cluster
    params = f"""[{{ "op": "replace", "path": "/spec/storageDeviceSets/0/count",
                "value": {new_storage_devices_sets_count}}}]"""
    sc.patch(
        resource_name=sc.get()["items"][0]["metadata"]["name"],
        params=params.strip("\n"),
        format_type="json",
    )
    return new_storage_devices_sets_count


def get_storage_cluster(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Get storage cluster name

    Args:
        namespace (str): Namespace of the resource

    Returns:
        storage cluster (obj) : Storage cluster object handler

    """
    sc_obj = OCP(kind=constants.STORAGECLUSTER, namespace=namespace)
    return sc_obj


def get_osd_count():
    """
    Get osd count from Storage cluster.

    Returns:
        int: osd count (In the case of external mode it returns 0)

    """
    sc = get_storage_cluster()
    sc_data = sc.get().get("items")[0]
    if sc_data["spec"].get("externalStorage", {}).get("enable"):
        return 0
    return int(sc_data["spec"]["storageDeviceSets"][0]["count"]) * int(
        sc.get().get("items")[0]["spec"]["storageDeviceSets"][0]["replica"]
    )


def get_osd_size():
    """
    Get osd size from Storage cluster

    Returns:
        int: osd size

    """
    sc = get_storage_cluster()
    size = (
        sc.get()
        .get("items")[0]
        .get("spec")
        .get("storageDeviceSets")[0]
        .get("dataPVCTemplate")
        .get("spec")
        .get("resources")
        .get("requests")
        .get("storage")
    )
    if size.isdigit or config.DEPLOYMENT.get("local_storage"):
        # In the case of UI deployment of LSO cluster, the value in StorageCluster CR
        # is set to 1, so we can not take OSD size from there. For LSO we will return
        # the size from PVC.
        pvc = get_deviceset_pvcs()[0]
        return int(pvc.get()["status"]["capacity"]["storage"][:-2])
    else:
        return int(size[:-2])


def get_deviceset_count():
    """
    Get storageDeviceSets count  from storagecluster

    Returns:
        int: storageDeviceSets count

    """
    sc = get_storage_cluster()
    return int(
        sc.get().get("items")[0].get("spec").get("storageDeviceSets")[0].get("count")
    )


def get_all_storageclass():
    """
    Function for getting all storageclass excluding 'gp2' and 'flex'

    Returns:
         list: list of storageclass

    """
    sc_obj = ocp.OCP(
        kind=constants.STORAGECLASS, namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    result = sc_obj.get()
    sample = result["items"]

    storageclass = [
        item
        for item in sample
        if (
            item.get("metadata").get("name")
            not in (constants.IGNORE_SC_GP2, constants.IGNORE_SC_FLEX)
        )
    ]
    return storageclass


def setup_ceph_debug():
    """
    Set Ceph to run in debug log level using a ConfigMap.
    This functionality is available starting OCS 4.7.

    """
    ceph_debug_log_configmap_data = templating.load_yaml(
        constants.CEPH_CONFIG_DEBUG_LOG_LEVEL_CONFIGMAP
    )
    ocs_version = version.get_semantic_ocs_version_from_config()
    if ocs_version == version.VERSION_4_8:
        stored_values = constants.ROOK_CEPH_CONFIG_VALUES_48.split("\n")
    elif ocs_version >= version.VERSION_4_9:
        stored_values = constants.ROOK_CEPH_CONFIG_VALUES_49.split("\n")
    else:
        stored_values = constants.ROOK_CEPH_CONFIG_VALUES.split("\n")
    ceph_debug_log_configmap_data["data"]["config"] = (
        stored_values + constants.CEPH_DEBUG_CONFIG_VALUES
    )

    ceph_configmap_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="config_map", delete=False
    )
    templating.dump_data_to_temp_yaml(
        ceph_debug_log_configmap_data, ceph_configmap_yaml.name
    )
    log.info("Setting Ceph to work in debug log level using a new ConfigMap resource")
    run_cmd(f"oc create -f {ceph_configmap_yaml.name}")


def verify_sc_images(storage_cluster):
    """
    Verifying images in storage cluster such as ceph, noobaaDB and noobaaCore

    Args:
        storage_cluster (obj): storage_cluster ocp object
    """
    images_list = list()
    images = storage_cluster.get().get("status").get("images")
    for component, images_dict in images.items():
        if len(images_dict) > 1:
            for image, image_name in images_dict.items():
                log.info(f"{component} has {image}:{image_name}")
                images_list.append(image_name)
    assert (
        len(set(images_list)) == len(images_list) / 2
    ), "actualImage and desiredImage are different"


def get_osd_replica_count():
    """
    Get OSD replication count from storagecluster cr

    Returns:
        replica_count (int): Returns OSD replication count

    """

    sc = get_storage_cluster()
    replica_count = (
        sc.get().get("items")[0].get("spec").get("storageDeviceSets")[0].get("replica")
    )
    return replica_count


def verify_multus_network():
    """
    Verify Multus network(s) created successfully and are present on relevant pods.
    """
    with open(constants.MULTUS_YAML, mode="r") as f:
        multus_public_data = yaml.load(f)
        multus_namespace = multus_public_data["metadata"]["namespace"]
        multus_name = multus_public_data["metadata"]["name"]
        multus_public_network_name = f"{multus_namespace}/{multus_name}"

    log.info("Verifying multus NetworkAttachmentDefinitions")
    ocp.OCP(
        resource_name=multus_public_network_name,
        kind="network-attachment-definitions",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    # TODO: also check if private NAD exists

    log.info("Verifying multus public network exists on ceph pods")
    osd_pods = get_osd_pods()
    for _pod in osd_pods:
        assert (
            _pod.data["metadata"]["annotations"]["k8s.v1.cni.cncf.io/networks"]
            == multus_public_network_name
        )
    # TODO: also check private network if it exists on OSD pods

    mon_pods = get_mon_pods()
    mds_pods = get_mds_pods()
    mgr_pods = get_mgr_pods()
    rgw_pods = get_rgw_pods()
    ceph_pods = [*mon_pods, *mds_pods, *mgr_pods, *rgw_pods]
    for _pod in ceph_pods:
        assert (
            _pod.data["metadata"]["annotations"]["k8s.v1.cni.cncf.io/networks"]
            == multus_public_network_name
        )

    log.info("Verifying multus public network exists on CSI pods")
    csi_pods = []
    interfaces = [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]
    for interface in interfaces:
        plugin_pods = get_plugin_pods(interface)
        csi_pods += plugin_pods

    cephfs_provisioner_pods = get_cephfsplugin_provisioner_pods()
    rbd_provisioner_pods = get_rbdfsplugin_provisioner_pods()

    csi_pods += cephfs_provisioner_pods
    csi_pods += rbd_provisioner_pods

    for _pod in csi_pods:
        assert (
            _pod.data["metadata"]["annotations"]["k8s.v1.cni.cncf.io/networks"]
            == multus_public_network_name
        )

    log.info("Verifying StorageCluster multus network data")
    sc = get_storage_cluster()
    sc_data = sc.get().get("items")[0]
    network_data = sc_data["spec"]["network"]
    assert network_data["provider"] == "multus"
    selectors = network_data["selectors"]
    assert selectors["public"] == f"{defaults.ROOK_CLUSTER_NAMESPACE}/ocs-public"
    # TODO: also check private network if it exists


def verify_managed_service_resources():
    """
    Verify creation and status of resources specific to OSD and ROSA deployments:
    1. ocs-operator, ocs-osd-deployer, ose-prometheus-operator csvs are Succeeded
    2. 1 prometheus pod and 3 alertmanager pods are in Running state
    3. Managedocs components alertmanager, prometheus, storageCluster are in Ready state
    4. Verify that noobaa-operator replicas is set to 0
    5. Verify managed ocs secrets
    6. If cluster is Provider, verify resources specific to provider clusters
    7. [temporarily left out] Verify Networkpolicy and EgressNetworkpolicy creation
    """
    # Verify CSV status
    for managed_csv in {
        constants.OCS_CSV_PREFIX,
        constants.OSD_DEPLOYER,
        constants.OSE_PROMETHEUS_OPERATOR,
    }:
        csvs = csv.get_csvs_start_with_prefix(
            managed_csv, constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        assert (
            len(csvs) == 1
        ), f"Unexpected number of CSVs with {managed_csv} prefix: {len(csvs)}"
        csv_name = csvs[0]["metadata"]["name"]
        csv_obj = csv.CSV(
            resource_name=csv_name, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        log.info(f"Check if {csv_name} is in Succeeded phase.")
        csv_obj.wait_for_phase(phase="Succeeded", timeout=600)

    # Verify alerting secrets creation
    verify_managed_alerting_secrets()

    # Verify alerting pods are Running
    pod_obj = OCP(
        kind="pod",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    for alert_pod in {
        (constants.MANAGED_PROMETHEUS_LABEL, 1),
        (constants.MANAGED_ALERTMANAGER_LABEL, 3),
    }:
        pod_obj.wait_for_resource(
            condition="Running", selector=alert_pod[0], resource_count=alert_pod[1]
        )

    # Verify managedocs components are Ready
    log.info("Getting managedocs components data")
    managedocs_obj = OCP(
        kind="managedocs",
        resource_name="managedocs",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    for component in {"alertmanager", "prometheus", "storageCluster"}:
        assert (
            managedocs_obj.get()["status"]["components"][component]["state"] == "Ready"
        ), f"{component} status is {managedocs_obj.get()['status']['components'][component]['state']}"

    # Verify that noobaa-operator replicas is set to 0
    noobaa_deployment = deployment.get_deployments_having_label(
        "operators.coreos.com/mcg-operator.openshift-storage=",
        constants.OPENSHIFT_STORAGE_NAMESPACE,
    )[0]
    log.info(f"Noobaa replicas count: {noobaa_deployment.replicas}")
    assert noobaa_deployment.replicas == 0

    if config.ENV_DATA["cluster_type"].lower() == "provider":
        verify_provider_resources()


def verify_provider_resources():
    """
    Verify resources specific to managed OCS provider:
    1. Ocs-provider-server pod is Running
    2. cephcluster is Ready and its hostNetworking is set to True
    3. Security groups are set up correctly
    4. Storagecluster has the correct properties
    """
    # Verify ocs-provider-server pod is Running
    pod_obj = OCP(
        kind="pod",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    pod_obj.wait_for_resource(
        condition="Running", selector="app=ocsProviderApiServer", resource_count=1
    )

    # Verify that cephcluster is Ready and hostNetworking is True
    cephcluster = OCP(kind="CephCluster", namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    cephcluster_yaml = cephcluster.get().get("items")[0]
    log.info("Verifying that cephcluster is Ready and hostNetworking is True")
    assert (
        cephcluster_yaml["status"]["phase"] == "Ready"
    ), f"Status of cephcluster ocs-storagecluster-cephcluster is {cephcluster_yaml['status']['phase']}"
    assert cephcluster_yaml["spec"]["network"][
        "hostNetwork"
    ], f"hostNetwork is {cephcluster_yaml['spec']['network']['hostNetwork']}"

    assert verify_worker_nodes_security_groups()

    verify_provider_storagecluster()


def verify_managed_service_networkpolicy():
    """
    Verify Networkpolicy and EgressNetworkpolicy creation
    Temporarily left out for V2 offering
    """
    for policy in {
        ("Networkpolicy", "ceph-ingress-rule"),
        ("EgressNetworkpolicy", "egress-rule"),
    }:
        policy_obj = OCP(
            kind=policy[0],
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert policy_obj.is_exist(
            resource_name=policy[1]
        ), f"{policy[0]} {policy}[1] does not exist in openshift-storage namespace"


def verify_managed_alerting_secrets():
    """
    Verify that ocs-converged-pagerduty, ocs-converged-smtp, ocs-converged-deadmanssnitch,
    addon-ocs-provider-qe-parameters, alertmanager-managed-ocs-alertmanager-generated secrets
    exist in openshift-storage namespace.
    For a provider cluster verify existence of onboarding-ticket-key, ocs-provider-server
    and rook-ceph-mon secrets.
    """
    secret_ocp_obj = OCP(
        kind=constants.SECRET, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    for secret_name in {
        managedservice.get_pagerduty_secret_name(),
        managedservice.get_smtp_secret_name(),
        managedservice.get_dms_secret_name(),
        managedservice.get_parameters_secret_name(),
        constants.MANAGED_ALERTMANAGER_SECRET,
    }:
        assert secret_ocp_obj.is_exist(
            resource_name=secret_name
        ), f"{secret_name} does not exist in {constants.OPENSHIFT_STORAGE_NAMESPACE} namespace"
    if config.ENV_DATA["cluster_type"].lower() == "provider":
        for secret_name in {
            constants.MANAGED_ONBOARDING_SECRET,
            constants.MANAGED_PROVIDER_SERVER_SECRET,
            constants.MANAGED_MON_SECRET,
        }:
            assert secret_ocp_obj.is_exist(
                resource_name=secret_name
            ), f"{secret_name} does not exist in {constants.OPENSHIFT_STORAGE_NAMESPACE} namespace"


def verify_provider_storagecluster():
    """
    Verify that storagecluster of the provider passes the following checks:
    1. allowRemoteStorageConsumers: true
    2. hostNetwork: true
    3. matchExpressions:
    key: node-role.kubernetes.io/worker
    operator: Exists
    key: node-role.kubernetes.io/infra
    operator: DoesNotExist
    4. storageProviderEndpoint: IP:31659
    5. annotations:
    uninstall.ocs.openshift.io/cleanup-policy: delete
    uninstall.ocs.openshift.io/mode: graceful
    """
    sc = get_storage_cluster()
    sc_data = sc.get()["items"][0]
    log.info(
        f"allowRemoteStorageConsumers: {sc_data['spec']['allowRemoteStorageConsumers']}"
    )
    assert sc_data["spec"]["allowRemoteStorageConsumers"] == True
    log.info(f"hostNetwork: {sc_data['spec']['hostNetwork']}")
    assert sc_data["spec"]["hostNetwork"] == True
    expressions = sc_data["spec"]["labelSelector"]["matchExpressions"]
    for item in expressions:
        log.info(f"Verifying {item}")
        if item["key"] == "node-role.kubernetes.io/worker":
            assert item["operator"] == "Exists"
        else:
            assert item["operator"] == "DoesNotExist"
    log.info(f"storageProviderEndpoint: {sc_data['status']['storageProviderEndpoint']}")
    assert re.match(
        "\\d+(\\.\\d+){3}:31659", sc_data["status"]["storageProviderEndpoint"]
    )
    annotations = sc_data["metadata"]["annotations"]
    log.info(f"Annotations: {annotations}")
    assert annotations["uninstall.ocs.openshift.io/cleanup-policy"] == "delete"
    assert annotations["uninstall.ocs.openshift.io/mode"] == "graceful"
