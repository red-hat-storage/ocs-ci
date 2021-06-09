"""
StorageCluster related functionalities
"""
import re
import logging
import tempfile

from jsonschema import validate
from semantic_version import Version

from ocs_ci.deployment.helpers.lso_helpers import add_disk_for_vsphere_platform
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.exceptions import ResourceNotFoundError, UnsupportedFeatureError
from ocs_ci.ocs.ocp import get_images, OCP
from ocs_ci.ocs.resources.ocs import get_ocs_csv
from ocs_ci.ocs.resources.pod import get_pods_having_label, get_osd_pods
from ocs_ci.ocs.resources.pv import check_pvs_present_for_ocs_expansion
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs
from ocs_ci.ocs.node import get_osds_per_node
from ocs_ci.utility import localstorage, utils, templating, kms as KMS
from ocs_ci.utility.rgwutils import get_rgw_count
from ocs_ci.utility.utils import run_cmd, get_ocp_version
from ocs_ci.ocs.ui.add_replace_device_ui import AddReplaceDeviceUI
from ocs_ci.ocs.ui.base_ui import login_ui, close_browser

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

    # Verify OCS CSV is in Succeeded phase
    log.info("verifying ocs csv")
    ocs_csv = get_ocs_csv()
    # Verify if OCS CSV has proper version.
    csv_version = ocs_csv.data["spec"]["version"]
    ocs_version = config.ENV_DATA["ocs_version"]
    log.info(f"Check if OCS version: {ocs_version} matches with CSV: {csv_version}")
    assert (
        ocs_version in csv_version
    ), f"OCS version: {ocs_version} mismatch with CSV version {csv_version}"
    # Verify if OCS CSV has the same version in provided CI build.
    ocs_registry_image = ocs_registry_image or config.DEPLOYMENT.get(
        "ocs_registry_image"
    )
    if ocs_registry_image and ocs_registry_image.endswith(".ci"):
        ocs_registry_image = ocs_registry_image.rsplit(":", 1)[1]
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

    # Verify OCS Cluster Service (ocs-storagecluster) is Ready
    storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
    log.info("Verifying status of storage cluster: %s", storage_cluster_name)
    storage_cluster = StorageCluster(
        resource_name=storage_cluster_name,
        namespace=namespace,
    )
    log.info(
        f"Check if StorageCluster: {storage_cluster_name} is in" f"Succeeded phase"
    )
    storage_cluster.wait_for_phase(phase="Ready", timeout=timeout)

    # Verify pods in running state and proper counts
    log.info("Verifying pod states and counts")
    pod = OCP(kind=constants.POD, namespace=namespace)
    if not config.DEPLOYMENT["external_mode"]:
        osd_count = int(
            storage_cluster.data["spec"]["storageDeviceSets"][0]["count"]
        ) * int(storage_cluster.data["spec"]["storageDeviceSets"][0]["replica"])
    rgw_count = None
    if config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS:
        if not disable_rgw:
            rgw_count = get_rgw_count(
                ocs_version, post_upgrade_verification, version_before_upgrade
            )

    min_eps = constants.MIN_NB_ENDPOINT_COUNT_POST_DEPLOYMENT
    max_eps = (
        constants.MAX_NB_ENDPOINT_COUNT
        if float(config.ENV_DATA["ocs_version"]) >= 4.6
        else 1
    )

    if config.ENV_DATA.get("platform") == constants.IBM_POWER_PLATFORM:
        min_eps = 1
        max_eps = 1

    nb_db_label = (
        constants.NOOBAA_DB_LABEL_46_AND_UNDER
        if float(config.ENV_DATA["ocs_version"]) < 4.7
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
    if not config.DEPLOYMENT["external_mode"]:
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

    for label, count in resources_dict.items():
        if label == constants.RGW_APP_LABEL:
            if (
                not config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS
                or disable_rgw
            ):
                continue
        if "noobaa" in label and disable_noobaa:
            continue
        if "mds" in label and disable_cephfs:
            continue

        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=label,
            resource_count=count,
            timeout=timeout,
        )

    if not disable_noobaa:
        nb_ep_pods = get_pods_having_label(
            label=constants.NOOBAA_ENDPOINT_POD_LABEL,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        )
        assert len(nb_ep_pods) <= max_eps, (
            f"The number of running NooBaa endpoint pods ({len(nb_ep_pods)}) "
            f"is greater than the maximum defined in the NooBaa CR ({max_eps})"
        )

    # Verify StorageClasses (1 ceph-fs, 1 ceph-rbd)
    log.info("Verifying storage classes")
    storage_class = OCP(kind=constants.STORAGECLASS, namespace=namespace)
    storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
    required_storage_classes = {
        f"{storage_cluster_name}-cephfs",
        f"{storage_cluster_name}-ceph-rbd",
    }
    if Version.coerce(ocs_version) >= Version.coerce("4.8"):
        required_storage_classes.update({f"{storage_cluster_name}-ceph-rbd-thick"})
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
    assert required_storage_classes.issubset(storage_class_names)

    # Verify OSDs are distributed
    if not config.DEPLOYMENT["external_mode"]:
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
        assert (
            sc_rbd["parameters"]["csi.storage.k8s.io/node-stage-secret-name"]
            == constants.RBD_NODE_SECRET
        )
        assert (
            sc_rbd["parameters"]["csi.storage.k8s.io/provisioner-secret-name"]
            == constants.RBD_PROVISIONER_SECRET
        )
    if not disable_cephfs:
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
        config.DEPLOYMENT.get("ui_deployment") or config.DEPLOYMENT["external_mode"]
    ):
        log.info(
            "Verifying ceph osd tree output and checking for device set PVC names "
            "in the output."
        )
        if config.DEPLOYMENT.get("local_storage"):
            deviceset_pvcs = [osd.get_node() for osd in get_osd_pods()]
            # removes duplicate hostname
            deviceset_pvcs = list(set(deviceset_pvcs))
            if config.ENV_DATA.get("platform") == constants.BAREMETAL_PLATFORM:
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

    # Verify CSI snapshotter sidecar container is not present
    # if the OCS version is < 4.6
    if float(config.ENV_DATA["ocs_version"]) < 4.6:
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


def osd_encryption_verification():
    """
    Verify if OSD encryption at rest if successfully deployed on OCS

    Raises:
        UnsupportedFeatureError: OCS version is smaller than 4.6
        EnvironmentError: The OSD is not encrypted

    """
    ocs_version = float(config.ENV_DATA["ocs_version"])
    if ocs_version < 4.6:
        error_message = "Encryption at REST can be enabled only on OCS >= 4.6!"
        raise UnsupportedFeatureError(error_message)
    osd_size = get_osd_size()

    log.info("Get 'lsblk' command output on nodes where osd running")
    osd_node_names = get_osds_per_node()
    lsblk_output_list = []
    for worker_node in osd_node_names:
        lsblk_cmd = "oc debug node/" + worker_node + " -- chroot /host lsblk"
        out = run_cmd(lsblk_cmd)
        log.info(f"the output from lsblk command is {out}")
        lsblk_output_list.append((out, len(osd_node_names[worker_node])))

    log.info("Verify 'lsblk' command results are as expected")
    for node_output_lsblk in lsblk_output_list:
        node_lsb = node_output_lsblk[0].split()

        log.info("Search 'crypt' in node_lsb list")
        all_occurrences_crypt = [
            index for index, element in enumerate(node_lsb) if element == "crypt"
        ]

        log.info("Verify all OSDs encrypted on node")
        if len(all_occurrences_crypt) != node_output_lsblk[1]:
            raise EnvironmentError("OSD is not encrypted")

        log.info("Verify that OSD is encrypted, and not another component like sda")
        for index_crypt in all_occurrences_crypt:
            encrypted_component_size = int(
                (re.findall(r"\d+", node_lsb[index_crypt - 2]))[0]
            )
            if encrypted_component_size != osd_size:
                raise EnvironmentError(
                    "The OSD is not encrypted, another mount encrypted."
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
    lv_lvs_data = localstorage.check_local_volume_local_volume_set()
    if lv_lvs_data.get("localvolume"):
        lvpresent = True
    elif lv_lvs_data.get("localvolumeset"):
        lv_set_present = True
    else:
        log.info(lv_lvs_data)
        raise ResourceNotFoundError("No LocalVolume and LocalVolume Set found")
    ocp_version = get_ocp_version()
    platform = config.ENV_DATA.get("platform", "").lower()
    is_lso = config.DEPLOYMENT.get("local_storage")
    if (
        ocp_version == "4.7"
        and (
            platform == constants.AWS_PLATFORM or platform == constants.VSPHERE_PLATFORM
        )
        and (not is_lso)
    ):
        logging.info("Add capacity via UI")
        setup_ui = login_ui()
        add_ui_obj = AddReplaceDeviceUI(setup_ui)
        add_ui_obj.add_capacity_ui()
        close_browser(setup_ui)
    else:
        if lvpresent:
            ocp_obj = OCP(
                kind="localvolume", namespace=config.ENV_DATA["local_storage_namespace"]
            )
            localvolume_data = ocp_obj.get(resource_name="local-block")
            device_list = localvolume_data["spec"]["storageClassDevices"][0][
                "devicePaths"
            ]
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
            if platform == constants.VSPHERE_PLATFORM and add_extra_disk_to_existing_worker:
                log.info("Adding Extra Disk to existing VSphere Worker nodes")
                add_disk_for_vsphere_platform()
            check_pvs_present_for_ocs_expansion()
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
    ceph_debug_log_configmap_data["data"]["config"] = (
        constants.ROOK_CEPH_CONFIG_VALUES + constants.CEPH_DEBUG_CONFIG_VALUES
    )

    ceph_configmap_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="config_map", delete=False
    )
    templating.dump_data_to_temp_yaml(
        ceph_debug_log_configmap_data, ceph_configmap_yaml.name
    )
    log.info("Setting Ceph to work in debug log level using a new ConfigMap resource")
    run_cmd(f"oc create -f {ceph_configmap_yaml.name}")
