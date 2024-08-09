"""
StorageCluster related functionalities
"""

import copy
import ipaddress
import logging
import re
import tempfile
import json

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from ocs_ci.framework import config

from ocs_ci.deployment.helpers.external_cluster_helpers import (
    ExternalCluster,
    get_external_cluster_client,
)
from ocs_ci.helpers.managed_services import (
    verify_provider_topology,
    get_ocs_osd_deployer_version,
    verify_faas_resources,
)
from ocs_ci.ocs import constants, defaults, ocp, managedservice
from ocs_ci.ocs.exceptions import (
    CommandFailed,
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
    get_ceph_tools_pod,
    get_osd_pod_id,
)
from ocs_ci.ocs.resources.pv import check_pvs_present_for_ocs_expansion
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs
from ocs_ci.ocs.node import (
    get_osds_per_node,
    add_new_disk_for_vsphere,
    get_osd_running_nodes,
    get_encrypted_osd_devices,
    verify_worker_nodes_security_groups,
    add_disk_to_node,
    get_nodes,
    get_nodes_where_ocs_pods_running,
    get_provider_internal_node_ips,
    add_disk_stretch_arbiter,
)
from ocs_ci.ocs.version import get_ocp_version
from ocs_ci.utility.version import get_semantic_version, VERSION_4_11
from ocs_ci.helpers.helpers import (
    get_secret_names,
    get_cephfs_name,
    get_logs_rook_ceph_operator,
)
from ocs_ci.utility import (
    localstorage,
    utils,
    templating,
    kms as KMS,
    version,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.rgwutils import get_rgw_count
from ocs_ci.utility.utils import run_cmd, TimeoutSampler, convert_device_size
from ocs_ci.utility.decorators import switch_to_orig_index_at_last
from ocs_ci.helpers.helpers import storagecluster_independent_check
from ocs_ci.deployment.helpers.mcg_helpers import check_if_mcg_root_secret_public

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


def verify_osd_tree_schema(ct_pod, deviceset_pvcs):
    """
    Verify Ceph OSD tree schema

    Args:
        ct_pod (:obj:`OCP`):  Object of the Ceph tools pod
        deviceset_pvcs (list): List of strings of deviceset PVC names

    """
    _deviceset_pvcs = copy.deepcopy(deviceset_pvcs)
    osd_tree = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree", format="json")
    schemas = {
        "root": constants.OSD_TREE_ROOT,
        "rack": constants.OSD_TREE_RACK,
        "host": constants.OSD_TREE_HOST,
        "osd": constants.OSD_TREE_OSD,
        "region": constants.OSD_TREE_REGION,
        "zone": constants.OSD_TREE_ZONE,
    }
    schemas["host"]["properties"]["name"] = {"enum": _deviceset_pvcs}
    for item in osd_tree["nodes"]:
        validate(instance=item, schema=schemas[item["type"]])
        if item["type"] == "host":
            _deviceset_pvcs.remove(item["name"])
    assert not _deviceset_pvcs, (
        f"These device set PVCs are not given in ceph osd tree output "
        f"- {_deviceset_pvcs}"
    )
    log.info(
        "Verified ceph osd tree output. Device set PVC names are given in the "
        "output."
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
    hci_cluster = (
        config.ENV_DATA.get("platform") in constants.HCI_PROVIDER_CLIENT_PLATFORMS
    )
    provider_cluster = (managed_service or hci_cluster) and config.ENV_DATA[
        "cluster_type"
    ].lower() == "provider"
    consumer_cluster = (
        managed_service
        and config.ENV_DATA["cluster_type"].lower() == constants.MS_CONSUMER_TYPE
    )
    client_cluster = (
        hci_cluster and config.ENV_DATA["cluster_type"].lower() == constants.HCI_CLIENT
    )
    ocs_version = version.get_semantic_ocs_version_from_config()
    external = config.DEPLOYMENT["external_mode"] or consumer_cluster or client_cluster
    fusion_aas = config.ENV_DATA.get("platform") == constants.FUSIONAAS_PLATFORM
    fusion_aas_consumer = fusion_aas and consumer_cluster
    fusion_aas_provider = fusion_aas and provider_cluster

    # Basic Verification for cluster
    if not (fusion_aas_consumer or client_cluster):
        basic_verification(ocs_registry_image)
    if client_cluster:
        verify_ocs_csv(ocs_registry_image=None)

    # Verify pods in running state and proper counts
    log.info("Verifying pod states and counts")
    exporter_pod_count = len(get_nodes_where_ocs_pods_running())
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
    if config.ENV_DATA.get("noobaa_external_pgsql"):
        del resources_dict[nb_db_label]

    if provider_cluster:
        resources_dict.update(
            {
                constants.MON_APP_LABEL: 3,
                constants.OSD_APP_LABEL: osd_count,
                constants.MGR_APP_LABEL: 1,
                constants.MDS_APP_LABEL: 2,
            }
        )
    elif consumer_cluster or client_cluster:
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
                constants.EXPORTER_APP_LABEL: exporter_pod_count,
            }
        )

    if config.DEPLOYMENT.get("arbiter_deployment"):
        resources_dict.update(
            {
                constants.MON_APP_LABEL: 5,
            }
        )

    if fusion_aas_consumer or client_cluster:
        del resources_dict[constants.OCS_OPERATOR_LABEL]
        del resources_dict[constants.OPERATOR_LABEL]

    if ocs_version >= version.VERSION_4_9:
        resources_dict.update(
            {
                constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL: 1,
            }
        )

    if ocs_version >= version.VERSION_4_15 and not client_cluster:
        resources_dict.update(
            {
                constants.UX_BACKEND_APP_LABEL: 1,
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
        if "noobaa" in label and (disable_noobaa or managed_service or client_cluster):
            continue
        if "mds" in label and disable_cephfs:
            continue
        if label == constants.MANAGED_CONTROLLER_LABEL:
            if fusion_aas_provider:
                service_pod = OCP(
                    kind=constants.POD, namespace=config.ENV_DATA["service_namespace"]
                )
                assert service_pod.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    selector=label,
                    resource_count=count,
                    timeout=timeout,
                )
                continue

        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=label,
            resource_count=count,
            timeout=timeout,
        )

    # Checks for FaaS
    if fusion_aas:
        verify_faas_resources()

    # Verify StorageClasses (1 ceph-fs, 1 ceph-rbd)
    log.info("Verifying storage classes")
    storage_class = OCP(kind=constants.STORAGECLASS, namespace=namespace)
    storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
    if config.ENV_DATA.get("custom_default_storageclass_names"):
        custom_sc = get_storageclass_names_from_storagecluster_spec()
        if not all(
            sc in custom_sc
            for sc in [
                constants.OCS_COMPONENTS_MAP["blockpools"],
                constants.OCS_COMPONENTS_MAP["cephfs"],
            ]
        ):
            raise ValueError(
                "Custom StorageClass are not defined in Storagecluster Spec."
            )

        required_storage_classes = {
            custom_sc[constants.OCS_COMPONENTS_MAP["cephfs"]],
            custom_sc[constants.OCS_COMPONENTS_MAP["blockpools"]],
        }
    else:
        required_storage_classes = {
            f"{storage_cluster_name}-cephfs",
            f"{storage_cluster_name}-ceph-rbd",
        }
    skip_storage_classes = set()
    if disable_cephfs or provider_cluster:
        skip_storage_classes.update(
            {
                f"{storage_cluster_name}-cephfs",
            }
        )
    if disable_blockpools or provider_cluster:
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
    if not provider_cluster:
        if fusion_aas_consumer or client_cluster:
            {
                f"{namespace}.cephfs.csi.ceph.com",
                f"{namespace}.rbd.csi.ceph.com",
            }.issubset(csi_drivers)
        else:
            assert defaults.CSI_PROVISIONERS.issubset(csi_drivers)

    # Verify node and provisioner secret names in storage class
    log.info("Verifying node and provisioner secret names in storage class.")
    cluster_name = config.ENV_DATA["cluster_name"]
    if config.ENV_DATA.get("custom_default_storageclass_names"):
        sc_rbd = storage_class.get(
            resource_name=custom_sc[constants.OCS_COMPONENTS_MAP["blockpools"]]
        )
        sc_cephfs = storage_class.get(
            resource_name=custom_sc[constants.OCS_COMPONENTS_MAP["cephfs"]]
        )

    elif config.DEPLOYMENT["external_mode"]:
        sc_rbd = storage_class.get(
            resource_name=constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
        )
        sc_cephfs = storage_class.get(
            resource_name=(constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS)
        )
    else:
        if not disable_blockpools and not provider_cluster:
            sc_rbd = storage_class.get(resource_name=constants.DEFAULT_STORAGECLASS_RBD)
        if not disable_cephfs and not provider_cluster:
            sc_cephfs = storage_class.get(
                resource_name=constants.DEFAULT_STORAGECLASS_CEPHFS
            )
    if not disable_blockpools and not provider_cluster:
        if consumer_cluster or client_cluster:
            assert (
                "rook-ceph-client"
                in sc_rbd["parameters"]["csi.storage.k8s.io/node-stage-secret-name"]
            )
            assert (
                "rook-ceph-client"
                in sc_rbd["parameters"]["csi.storage.k8s.io/provisioner-secret-name"]
            )
        else:
            if (
                config.DEPLOYMENT["external_mode"]
                and config.ENV_DATA["restricted-auth-permission"]
            ):
                if config.ENV_DATA.get("alias_rbd_name"):
                    rbd_name = config.ENV_DATA["alias_rbd_name"]
                else:
                    rbd_name = config.ENV_DATA.get("rbd_name") or defaults.RBD_NAME
                rbd_node_secret = (
                    f"{constants.RBD_NODE_SECRET}-{cluster_name}-{rbd_name}"
                )
                rbd_provisioner_secret = (
                    f"{constants.RBD_PROVISIONER_SECRET}-{cluster_name}-{rbd_name}"
                )
                assert (
                    sc_rbd["parameters"]["csi.storage.k8s.io/node-stage-secret-name"]
                    == rbd_node_secret
                )
                assert (
                    sc_rbd["parameters"]["csi.storage.k8s.io/provisioner-secret-name"]
                    == rbd_provisioner_secret
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

    if not disable_cephfs and not provider_cluster:
        if consumer_cluster or client_cluster:
            assert (
                "rook-ceph-client"
                in sc_cephfs["parameters"]["csi.storage.k8s.io/node-stage-secret-name"]
            )
            assert (
                "rook-ceph-client"
                in sc_cephfs["parameters"]["csi.storage.k8s.io/provisioner-secret-name"]
            )
        else:
            if (
                config.DEPLOYMENT["external_mode"]
                and config.ENV_DATA["restricted-auth-permission"]
            ):
                cephfs_name = config.ENV_DATA.get("cephfs_name") or get_cephfs_name()
                cephfs_node_secret = (
                    f"{constants.CEPHFS_NODE_SECRET}-{cluster_name}-{cephfs_name}"
                )
                cephfs_provisioner_secret = f"{constants.CEPHFS_PROVISIONER_SECRET}-{cluster_name}-{cephfs_name}"
                assert (
                    sc_cephfs["parameters"]["csi.storage.k8s.io/node-stage-secret-name"]
                    == cephfs_node_secret
                )
                assert (
                    sc_cephfs["parameters"][
                        "csi.storage.k8s.io/provisioner-secret-name"
                    ]
                    == cephfs_provisioner_secret
                )
            else:
                assert (
                    sc_cephfs["parameters"]["csi.storage.k8s.io/node-stage-secret-name"]
                    == constants.CEPHFS_NODE_SECRET
                )
                assert (
                    sc_cephfs["parameters"][
                        "csi.storage.k8s.io/provisioner-secret-name"
                    ]
                    == constants.CEPHFS_PROVISIONER_SECRET
                )

    log.info("Verified node and provisioner secret names in storage class.")

    # TODO: Enable the tools pod check when a solution is identified for tools pod on FaaS consumer
    if not (fusion_aas_consumer or client_cluster):
        ct_pod = get_ceph_tools_pod()

    # https://github.com/red-hat-storage/ocs-ci/issues/3820
    # Verify ceph osd tree output
    if not (
        config.DEPLOYMENT.get("ui_deployment")
        or config.DEPLOYMENT["external_mode"]
        or managed_service
        or hci_cluster
    ):
        log.info(
            "Verifying ceph osd tree output and checking for device set PVC names "
            "in the output."
        )
        if config.DEPLOYMENT.get("local_storage"):
            deviceset_pvcs = [osd.get_node() for osd in get_osd_pods()]
            # removes duplicate hostname
            deviceset_pvcs = list(set(deviceset_pvcs))
            if (
                config.ENV_DATA.get("platform")
                in [constants.BAREMETAL_PLATFORM, constants.HCI_BAREMETAL]
                or config.ENV_DATA.get("platform") == constants.AWS_PLATFORM
            ):
                deviceset_pvcs = [
                    deviceset.replace(".", "-") for deviceset in deviceset_pvcs
                ]
        else:
            deviceset_pvcs = [pvc.name for pvc in get_deviceset_pvcs()]
        # Allowing re-try here in the deployment, as there might be a case in RDR
        # scenario, that OSD is getting delayed for few seconds and is not UP yet.
        # Issue: https://github.com/red-hat-storage/ocs-ci/issues/9666
        retry((ValidationError), tries=3, delay=60)(verify_osd_tree_schema)(
            ct_pod, deviceset_pvcs
        )

    # TODO: Verify ceph osd tree output have osd listed as ssd
    # TODO: Verify ceph osd tree output have zone or rack based on AZ

    # verify caps for external cluster
    log.info("Verify CSI users and caps for external cluster")
    if config.DEPLOYMENT["external_mode"] and ocs_version >= version.VERSION_4_10:
        if config.ENV_DATA["restricted-auth-permission"]:
            ceph_csi_users = [
                f"client.csi-cephfs-node-{cluster_name}-{cephfs_name}",
                f"client.csi-cephfs-provisioner-{cluster_name}-{cephfs_name}",
                f"client.csi-rbd-node-{cluster_name}-{rbd_name}",
                f"client.csi-rbd-provisioner-{cluster_name}-{rbd_name}",
            ]
            log.debug(f"CSI users for restricted auth permissions are {ceph_csi_users}")
            expected_csi_users = copy.deepcopy(ceph_csi_users)
        else:
            ceph_csi_users = copy.deepcopy(defaults.ceph_csi_users)
            expected_csi_users = copy.deepcopy(defaults.ceph_csi_users)

        ceph_auth_data = ct_pod.exec_cmd_on_pod("ceph auth ls -f json")
        for each in ceph_auth_data["auth_dump"]:
            if each["entity"] in expected_csi_users:
                assert (
                    "osd blocklist" in each["caps"]["mon"]
                ), f"osd blocklist caps are not present for user {each['entity']}"
                ceph_csi_users.remove(each["entity"])
        assert (
            not ceph_csi_users
        ), f"CSI users {ceph_csi_users} not created in external cluster"
        log.debug("All CSI users exists and have expected caps")

        if config.ENV_DATA.get("rgw-realm"):
            log.info("Verify user is created in realm")
            object_store_user = defaults.EXTERNAL_CLUSTER_OBJECT_STORE_USER
            realm = config.ENV_DATA.get("rgw-realm")
            host, user, password, ssh_key = get_external_cluster_client()
            external_cluster = ExternalCluster(host, user, password, ssh_key)
            assert external_cluster.is_object_store_user_exists(
                user=object_store_user, realm=realm
            ), f"{object_store_user} doesn't exist in realm {realm}"

    # Verify CSI snapshotter sidecar container is not present
    # if the OCS version is < 4.6
    if ocs_version < version.VERSION_4_6:
        log.info("Verifying CSI snapshotter is not present.")
        provisioner_pods = get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
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
    # TODO: Enable the check when a solution is identified for tools pod on FaaS consumer
    if utils.get_az_count() == 3 and not fusion_aas_consumer:
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
    if not (managed_service or hci_cluster):
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

    # TODO: Enable the check when a solution is identified for tools pod on FaaS consumer
    if not (fusion_aas_consumer or hci_cluster):
        # Temporarily disable health check for hci until we have enough healthy clusters
        assert utils.ceph_health_check(
            namespace, health_check_tries, health_check_delay
        )
    # Let's wait for storage system after ceph health is OK to prevent fails on
    # Progressing': 'True' state.

    if not (fusion_aas or client_cluster):
        verify_storage_system()

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

    if not (fusion_aas_consumer or client_cluster):
        storage_cluster_obj = get_storage_cluster()
        is_flexible_scaling = (
            storage_cluster_obj.get()["items"][0]
            .get("spec")
            .get("flexibleScaling", False)
        )
        if is_flexible_scaling is True:
            failure_domain = storage_cluster_obj.data["items"][0]["status"][
                "failureDomain"
            ]
            assert failure_domain == "host", (
                f"The expected failure domain on cluster with flexible scaling is 'host',"
                f" the actaul failure domain is {failure_domain}"
            )

    if config.ENV_DATA.get("is_multus_enabled"):
        verify_multus_network()

    # validation in case of openshift-cert-manager installed
    if config.DEPLOYMENT.get("install_cert_manager"):
        # get webhooks
        webhook = OCP(kind=constants.WEBHOOK, namespace=defaults.CERT_MANAGER_NAMESPACE)
        webhook_names = [
            each_webhook["metadata"]["name"] for each_webhook in webhook.get()["items"]
        ]
        log.debug(f"webhooks in the cluster: {webhook_names}")
        assert (
            constants.ROOK_CEPH_WEBHOOK not in webhook_names
        ), f"webhook {constants.ROOK_CEPH_WEBHOOK} should be disabled"
        log.info(f"[Expected]: {constants.ROOK_CEPH_WEBHOOK} not found in webhooks")

        # check rook-ceph-operator logs
        rook_ceph_operator_logs = get_logs_rook_ceph_operator()
        for line in rook_ceph_operator_logs.splitlines():
            if "delete webhook resources since webhook is disabled" in line:
                break
        else:
            assert (
                False
            ), "deleting webhook messages not found in rook-ceph-operator logs"

    # Verify in-transit encryption is enabled.
    if config.ENV_DATA.get("in_transit_encryption"):
        in_transit_encryption_verification()

    # Verify Custome Storageclass Names
    if config.ENV_DATA.get("custom_default_storageclass_names"):
        assert (
            check_custom_storageclass_presence()
        ), "Custom Storageclass Verification Failed."

    # Verify olm.maxOpenShiftVersion property
    # check ODF version due to upgrades
    if ocs_version >= version.VERSION_4_14 and not hci_cluster:
        verify_max_openshift_version()
        if config.RUN["cli_params"].get("deploy") and not (
            config.DEPLOYMENT["external_mode"]
            or config.UPGRADE.get("upgrade_ocs_version")
            or config.UPGRADE.get("upgrade_ocs_registry_image")
        ):
            device_class = get_device_class()
            verify_storage_device_class(device_class)
            verify_device_class_in_osd_tree(ct_pod, device_class)

    # RDR with globalnet submariner
    if (
        config.ENV_DATA.get("enable_globalnet", True)
        and config.MULTICLUSTER.get("multicluster_mode") == "regional-dr"
    ):
        validate_serviceexport()

    # check that noobaa root secrets are not public
    if not (client_cluster or managed_service):
        assert (
            check_if_mcg_root_secret_public() is False
        ), "Seems like MCG root secrets are public, please check"
        log.info("Noobaa root secrets are not public")

    # Verify the owner of CSI deployments and daemonsets if not provider mode
    if not (managed_service or hci_cluster):
        deployment_kind = OCP(kind=constants.DEPLOYMENT, namespace=namespace)
        daemonset_kind = OCP(kind=constants.DAEMONSET, namespace=namespace)
        for provisioner_name in [
            "csi-cephfsplugin-provisioner",
            "csi-rbdplugin-provisioner",
        ]:
            provisioner_deployment = deployment_kind.get(resource_name=provisioner_name)
            owner_references = provisioner_deployment["metadata"].get("ownerReferences")
            assert (
                len(owner_references) == 1
            ), f"Found more than 1 or none owner reference for {constants.DEPLOYMENT} {provisioner_name}"
            assert (
                owner_references[0].get("kind") == constants.DEPLOYMENT
            ), f"Owner reference of {constants.DEPLOYMENT} {provisioner_name} is not of kind {constants.DEPLOYMENT}"
            assert owner_references[0].get("name") == constants.ROOK_CEPH_OPERATOR, (
                f"Owner reference of {constants.DEPLOYMENT} {provisioner_name} "
                f"is not {constants.ROOK_CEPH_OPERATOR} {constants.DEPLOYMENT}"
            )
        log.info("Verified the ownerReferences CSI provisioner deployemts")
        for plugin_name in ["csi-cephfsplugin", "csi-rbdplugin"]:
            plugin_daemonset = daemonset_kind.get(resource_name=plugin_name)
            owner_references = plugin_daemonset["metadata"].get("ownerReferences")
            assert (
                len(owner_references) == 1
            ), f"Found more than 1 or none owner reference for {constants.DAEMONSET} {plugin_name}"
            assert (
                owner_references[0].get("kind") == constants.DEPLOYMENT
            ), f"Owner reference of {constants.DAEMONSET} {plugin_name} is not of kind {constants.DEPLOYMENT}"
            assert owner_references[0].get("name") == constants.ROOK_CEPH_OPERATOR, (
                f"Owner reference of {constants.DAEMONSET} {plugin_name} "
                f"is not {constants.ROOK_CEPH_OPERATOR} {constants.DEPLOYMENT}"
            )
        log.info("Verified the ownerReferences CSI plugin daemonsets")


def mcg_only_install_verification(ocs_registry_image=None):
    """
    Verification for successful MCG only deployment

    Args:
        ocs_registry_image (str): Specific image to check if it was installed
            properly.

    """
    log.info("Verifying MCG Only installation")
    basic_verification(ocs_registry_image)
    verify_storage_system()
    verify_backing_store()
    verify_mcg_only_pods()


def basic_verification(ocs_registry_image=None):
    """
    Basic verification which is needed for Full deployment and MCG only deployment

    Args:
        ocs_registry_image (str): Specific image to check if it was installed
            properly.

    """
    verify_ocs_csv(ocs_registry_image)
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
    hci_managed_service = (
        config.ENV_DATA["platform"].lower() in constants.HCI_PC_OR_MS_PLATFORM
    )
    log.info("verifying ocs csv")
    # Verify if OCS CSV has proper version.
    ocs_csv = get_ocs_csv()
    csv_version = ocs_csv.data["spec"]["version"]
    ocs_version = version.get_semantic_ocs_version_from_config()
    if not hci_managed_service:
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
    hci_managed_service = (
        config.ENV_DATA["platform"].lower() in constants.HCI_PC_OR_MS_PLATFORM
    )
    live_deployment = config.DEPLOYMENT.get("live_deployment")
    ocp_version = version.get_semantic_ocp_version_from_config()
    ocs_version = version.get_semantic_ocs_version_from_config()
    if live_deployment and (
        (ocp_version == version.VERSION_4_10 and ocs_version == version.VERSION_4_9)
        or (ocp_version == version.VERSION_4_11 and ocs_version == version.VERSION_4_10)
    ):
        log.warning(
            "Because of the BZ 2075422, we are skipping storage system validation!"
        )
        return
    if config.UPGRADE.get("upgrade_ocs_version"):
        upgrade_ocs_version = version.get_semantic_version(
            config.UPGRADE.get("upgrade_ocs_version"), only_major_minor=True
        )
        if live_deployment and (
            (
                ocp_version == version.VERSION_4_10
                and upgrade_ocs_version == version.VERSION_4_10
            )
            or (
                ocp_version == version.VERSION_4_11
                and upgrade_ocs_version == version.VERSION_4_11
            )
        ):
            log.warning(
                "Because of the BZ 2075422, we are skipping storage system validation after upgrade"
            )
            return
    if ocs_version >= version.VERSION_4_9 and not hci_managed_service:
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
    if config.ENV_DATA.get("platform") == constants.FUSIONAAS_PLATFORM:
        timeout = 1000
    elif storage_cluster.data["spec"].get("resourceProfile") != storage_cluster.data[
        "status"
    ].get("lastAppliedResourceProfile"):
        timeout = 1200
    else:
        timeout = 600
    storage_cluster.wait_for_phase(phase="Ready", timeout=timeout)

    # verify storage cluster version
    if not config.ENV_DATA.get("disable_storage_cluster_version_check"):
        verify_storage_cluster_version(storage_cluster)


def verify_storage_cluster_version(storage_cluster):
    """
    Verifies the storage cluster version

    Args:
        storage_cluster (obj): storage cluster object

    """
    # verify storage cluster version
    if config.RUN["cli_params"].get("deploy") and not config.UPGRADE.get(
        "upgrade_ocs_version"
    ):
        log.info("Verifying storage cluster version")
        try:
            storage_cluster_version = storage_cluster.get()["status"]["version"]
            ocs_csv = get_ocs_csv()
            csv_version = ocs_csv.data["spec"]["version"]
            assert (
                storage_cluster_version in csv_version
            ), f"storage cluster version {storage_cluster_version} is not same as csv version {csv_version}"
        except KeyError as e:
            if (
                config.ENV_DATA.get("platform", "").lower()
                in constants.MANAGED_SERVICE_PLATFORMS
            ):
                # This is a workaround. The issue for tracking is
                # https://github.com/red-hat-storage/ocs-ci/issues/8390
                log.warning(f"Can't get the sc version due to the error: {str(e)}")
            else:
                raise e


def verify_storage_device_class(device_class):
    """
    Verifies the parameters of storageClassDeviceSets in CephCluster.

    For internal deployments, if user is not specified any DeviceClass in the StorageDeviceSet, then
    tunefastDeviceClass will be true and
    crushDeviceClass will set to "ssd"

    Args:
        device_class (str): Name of the device class

    """
    # If the user has not provided any specific DeviceClass in the StorageDeviceSet for internal deployment then
    # tunefastDeviceClass will be true and crushDeviceClass will set to "ssd"
    log.info("Verifying crushDeviceClass for storageClassDeviceSets")
    cephcluster = OCP(
        kind="CephCluster", namespace=config.ENV_DATA["cluster_namespace"]
    )
    cephcluster_data = cephcluster.get()
    storage_class_device_sets = cephcluster_data["items"][0]["spec"]["storage"][
        "storageClassDeviceSets"
    ]

    for each_devise_set in storage_class_device_sets:
        # check tuneFastDeviceClass
        device_set_name = each_devise_set["name"]
        if config.ENV_DATA.get("tune_fast_device_class"):
            tune_fast_device_class = each_devise_set["tuneFastDeviceClass"]
            msg = f"tuneFastDeviceClass for {device_set_name} is set to {tune_fast_device_class}"
            log.debug(msg)
            assert (
                tune_fast_device_class
            ), f"{msg} when {constants.DEVICECLASS} is not selected explicitly"

        # check crushDeviceClass
        crush_device_class = each_devise_set["volumeClaimTemplates"][0]["metadata"][
            "annotations"
        ]["crushDeviceClass"]
        crush_device_class_msg = (
            f"crushDeviceClass for {device_set_name} is set to {crush_device_class}"
        )
        log.debug(crush_device_class_msg)
        assert (
            crush_device_class == device_class
        ), f"{crush_device_class_msg} but it should be set to {device_class}"

    # get deviceClasses for overall storage
    device_classes = cephcluster_data["items"][0]["status"]["storage"]["deviceClasses"]
    log.debug(f"deviceClasses are {device_classes}")
    for each_device_class in device_classes:
        device_class_name = each_device_class["name"]
        assert (
            device_class_name == device_class
        ), f"deviceClass is set to {device_class_name} but it should be set to {device_class}"


def verify_device_class_in_osd_tree(ct_pod, device_class):
    """
    Verifies device class in ceph osd tree output

    Args:
        ct_pod (:obj:`OCP`):  Object of the Ceph tools pod
        device_class (str): Name of the device class

    """
    log.info("Verifying DeviceClass in ceph osd tree")
    osd_tree = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree")
    for each in osd_tree["nodes"]:
        if each["type"] == "osd":
            osd_name = each["name"]
            device_class_in_osd_tree = each["device_class"]
            log.debug(f"DeviceClass for {osd_name} is {device_class_in_osd_tree}")
            assert (
                device_class_in_osd_tree == device_class
            ), f"DeviceClass for {osd_name} is {device_class_in_osd_tree} but expected value is {device_class}"


def get_device_class():
    """
    Fetches the device class from storage cluster

    Returns:
        str: Device class name

    """
    storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
    storage_cluster = StorageCluster(
        resource_name=storage_cluster_name,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    storage_device_sets = storage_cluster.get()["spec"]["storageDeviceSets"][0]

    # If the user has not provided any specific DeviceClass in the StorageDeviceSet for internal deployment then
    # DeviceClass will set to "ssd"
    device_class = storage_device_sets.get(constants.DEVICECLASS)
    if not device_class:
        device_class = defaults.CRUSH_DEVICE_CLASS
        config.ENV_DATA["tune_fast_device_class"] = True
    return device_class


def verify_noobaa_endpoint_count():
    """
    Verify noobaa endpoints
    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    disable_noobaa = config.COMPONENTS["disable_noobaa"]
    hci_managed_service = (
        config.ENV_DATA["platform"].lower() in constants.HCI_PC_OR_MS_PLATFORM
    )
    max_eps = (
        constants.MAX_NB_ENDPOINT_COUNT if ocs_version >= version.VERSION_4_6 else 1
    )
    if config.ENV_DATA.get("platform") == constants.IBM_POWER_PLATFORM:
        max_eps = 1
    if not (disable_noobaa or hci_managed_service):
        nb_ep_pods = get_pods_having_label(
            label=constants.NOOBAA_ENDPOINT_POD_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
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


def verify_max_openshift_version():
    """
    Verify the maximum OpenShift version supported for ODF
    """
    log.info("Verifying maxOpenShiftVersion")
    odf_csv = csv.get_csvs_start_with_prefix(
        defaults.ODF_OPERATOR_NAME, config.ENV_DATA["cluster_namespace"]
    )
    odf_full_version = odf_csv[0]["metadata"]["labels"]["full_version"]
    olm_properties = odf_csv[0]["metadata"]["annotations"].get("olm.properties")

    # assert if olm_properties is empty
    assert (
        olm_properties
    ), f"olm.maxOpenShiftVersion is not set for {defaults.ODF_OPERATOR_NAME}"

    max_openshift_version = eval(olm_properties)[0]["value"]
    log.debug(f"olm.maxOpenShiftVersion is: {max_openshift_version}")
    log.debug(f"ODF full version is: {odf_full_version}")
    max_openshift_sem_version = get_semantic_version(version=max_openshift_version)
    odf_sem_version = get_semantic_version(
        version=odf_full_version, ignore_pre_release=True
    )
    expected_max_openshift_sem_version = odf_sem_version.next_minor()
    assert max_openshift_sem_version == expected_max_openshift_sem_version, (
        f"olm.maxOpenShiftVersion is {max_openshift_version} but expected "
        f"version is {expected_max_openshift_sem_version}"
    )


def verify_backing_store():
    """
    Verify backingstore
    """
    log.info("Verifying backingstore")
    backingstore_obj = OCP(
        kind="backingstore", namespace=config.ENV_DATA["cluster_namespace"]
    )
    # backingstore creation will take time, so keeping timeout as 600
    assert backingstore_obj.wait_for_resource(
        condition=constants.STATUS_READY, column="PHASE", timeout=600
    )


def verify_mcg_only_pods():
    """
    Verify pods in MCG Only deployment
    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    min_eps = constants.MIN_NB_ENDPOINT_COUNT_POST_DEPLOYMENT
    resources_dict = {
        constants.CSI_ADDONS_CONTROLLER_MANAGER_LABEL: 1,
        constants.NOOBAA_CORE_POD_LABEL: 1,
        constants.NOOBAA_DB_LABEL_47_AND_ABOVE: 1,
        constants.NOOBAA_ENDPOINT_POD_LABEL: min_eps,
        constants.NOOBAA_OPERATOR_POD_LABEL: 1,
        constants.OCS_METRICS_EXPORTER: 1,
        constants.OCS_OPERATOR_LABEL: 1,
        constants.ODF_CONSOLE: 1,
        constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL: 1,
        constants.OPERATOR_LABEL: 1,
    }
    if config.ENV_DATA.get("noobaa_external_pgsql"):
        del resources_dict[constants.NOOBAA_DB_LABEL_47_AND_ABOVE]
    if config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
        resources_dict.update(
            {
                constants.NOOBAA_DEFAULT_BACKINGSTORE_LABEL: 1,
            }
        )
    if ocs_version >= version.VERSION_4_15:
        resources_dict.update(
            {
                constants.UX_BACKEND_APP_LABEL: 1,
            }
        )
    for label, count in resources_dict.items():
        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=label,
            resource_count=count,
            timeout=600,
        )


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
        lsblk_cmd = (
            f"oc debug node/{worker_node} --to-namespace={config.ENV_DATA['cluster_namespace']} "
            "-- chroot /host lsblk"
        )
        # It happens from time to time that we see this error:
        # error: unable to create the debug pod "node-name-internal-debug
        # Hence we need to add some re-try logic here
        lsblk_out = retry(
            (CommandFailed),
            tries=3,
            delay=60,
            text_in_exception="unable to create the debug pod",
        )(run_cmd)(lsblk_cmd)
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
    if ocs_version > version.VERSION_4_6:
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


def ceph_config_dump():
    """
    Get the 'ceph config dump' output.

    Returns:
        dict: The output of the 'ceph config dump' command as a dict.

    """
    log.info("Getting 'ceph config dump' output.")
    toolbox = get_ceph_tools_pod()

    return toolbox.exec_ceph_cmd("ceph config dump")


def ceph_mon_dump():
    """
    Get the 'ceph mon dump' output.

    Returns:
        dict: The output of the 'ceph mon dump' command as a dictionary.

    """
    log.info("Getting 'ceph mon dump' output.")
    toolbox = get_ceph_tools_pod()

    return toolbox.exec_ceph_cmd("ceph mon dump")


def in_transit_encryption_verification():
    """
    Verifies in-transit encryption is enabled and ceph mons are configured with 'v2' protocol version.

    Raises:
        ValueError: if in-transit encryption is not configured or ceph mon protocol is not configured with 'v2' version.

    """
    log.info("in-transit encryption is about to be validated.")
    keys_to_match = ["ms_client_mode", "ms_cluster_mode", "ms_service_mode"]
    intransit_config_state = get_in_transit_encryption_config_state()

    def search_secure_keys():
        ceph_dump_data = ceph_config_dump()
        keys_found = [
            record["name"]
            for record in ceph_dump_data
            if record["name"] in keys_to_match
        ]

        if (intransit_config_state) and (len(keys_found) != len(keys_to_match)):
            raise ValueError("Not all secure keys are present in the config")

        if (not intransit_config_state) and (len(keys_found) > 0):
            raise ValueError("Some secure keys are Still in the config")

        return keys_found

    keys_found = retry(
        (ValueError),
        tries=10,
        delay=5,
    )(search_secure_keys)()

    if len(keys_to_match) != len(keys_found):
        log.error("in-transit encryption is not configured.")
        raise ValueError(
            f"in-transit encryption keys {','.join(list(set(keys_to_match) - set(keys_found)))} \
                are not found in 'ceph config dump' output."
        )

    log.info(
        "in-transit encryption is configured,"
        "'ceph config dump' output has"
        f" {','.join(keys_found)} keys configured."
    )

    return True


def get_in_transit_encryption_config_state():
    """
    Returns the state of in-transit encryption for the OCS cluster.

    Returns:
        bool: True if in-transit encryption is enabled, False if it is disabled, or None if an error occurred.

    """
    cluster_name = (
        constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE
        if storagecluster_independent_check()
        else constants.DEFAULT_CLUSTERNAME
    )

    ocp_obj = StorageCluster(
        resource_name=cluster_name,
        namespace=config.ENV_DATA["cluster_namespace"],
    )

    try:
        return ocp_obj.data["spec"]["network"]["connections"]["encryption"]["enabled"]
    except KeyError as e:
        log.error(f"In-transit Encryption key {e}. not present in the storagecluster.")
        return False


def set_in_transit_encryption(enabled=True):
    """
    Enable or disable in-transit encryption for the default storage cluster.

    Args:
        enabled (bool, optional): A boolean indicating whether to enable or disable in-transit encryption.
            Defaults to True, i.e., enabling in-transit encryption.

    Returns:
        bool: True if in-transit encryption was successfully enabled or disabled, False otherwise.

    """

    # First confirming the existing status of the in-transit encryption
    # on storage cluster If its same as desire state then returning.
    if get_in_transit_encryption_config_state() == enabled:
        log.info("Existing in-transit encryption state is same as desire state.")
        return True

    cluster_name = (
        constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE
        if storagecluster_independent_check()
        else constants.DEFAULT_CLUSTERNAME
    )

    ocp_obj = StorageCluster(
        resource_name=cluster_name,
        namespace=config.ENV_DATA["cluster_namespace"],
    )

    patch = {"spec": {"network": {"connections": {"encryption": {"enabled": enabled}}}}}
    action = "enable" if enabled else "disable"
    log.info(f"Patching storage class to {action} in-transit encryption.")

    if not ocp_obj.patch(params=json.dumps(patch), format_type="merge"):
        log.error(f"Error {action} in-transit encryption.")
        return False

    log.info(f"In-transit encryption is {action}d successfully.")
    ocp_obj.wait_for_phase("Progressing", timeout=60)
    verify_storage_cluster()
    return True


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


def add_capacity_lso(ui_flag=False):
    """
    Add capacity on LSO cluster.

    In this procedure we need to add the disk before add capacity via UI.
    Because the UI backend check the pv and available state and base on it
    change the count param on StorageCluster.

    Args:
        ui_flag(bool): add capacity via ui [true] or via cli [false]
    """
    from ocs_ci.ocs.cluster import (
        is_flexible_scaling_enabled,
        check_ceph_health_after_add_capacity,
    )
    from ocs_ci.ocs.ui.helpers_ui import ui_add_capacity_conditions, ui_add_capacity

    osd_numbers = get_osd_count()
    node_objs = get_nodes(node_type=constants.WORKER_MACHINE)
    deviceset_count = get_deviceset_count()
    if is_flexible_scaling_enabled():
        log.info("Add 2 disk to same node")
        add_disk_to_node(node_objs[0])
        add_disk_to_node(node_objs[0])
        num_available_pv = 2
        set_count = deviceset_count + 2
    else:
        num_available_pv = get_osd_replica_count()
        if (
            config.DEPLOYMENT.get("arbiter_deployment") is True
            and num_available_pv == 4
        ):
            add_disk_stretch_arbiter()
        else:
            add_new_disk_for_vsphere(sc_name=constants.LOCALSTORAGE_SC)
        set_count = deviceset_count + 1
    localstorage.check_pvs_created(num_pvs_required=num_available_pv)
    if ui_add_capacity_conditions() and ui_flag:
        osd_size = get_osd_size()
        ui_add_capacity(osd_size)
    else:
        set_deviceset_count(set_count)

    pod_obj = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    pod_obj.wait_for_resource(
        timeout=600,
        condition=constants.STATUS_RUNNING,
        selector=constants.OSD_APP_LABEL,
        resource_count=osd_numbers + num_available_pv,
    )

    # Verify OSDs are encrypted
    if config.ENV_DATA.get("encryption_at_rest"):
        osd_encryption_verification()

    check_ceph_health_after_add_capacity(ceph_rebalance_timeout=3600)


def set_deviceset_count(count):
    """
    Set osd count for Storage cluster.

    Args:
        count (int): the count param is storagecluster

    """
    sc = get_storage_cluster()
    params = f"""[{{ "op": "replace", "path": "/spec/storageDeviceSets/0/count",
                "value": {count}}}]"""
    sc.patch(
        resource_name=sc.get()["items"][0]["metadata"]["name"],
        params=params.strip("\n"),
        format_type="json",
    )


def get_storage_cluster(namespace=None):
    """
    Get storage cluster name

    Args:
        namespace (str): Namespace of the resource

    Returns:
        storage cluster (obj) : Storage cluster object handler

    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]
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
    return int(get_storage_size()[:-2])


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
        kind=constants.STORAGECLASS, namespace=config.ENV_DATA["cluster_namespace"]
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

    public_net_created = config.ENV_DATA["multus_create_public_net"]
    public_net_name = config.ENV_DATA["multus_public_net_name"]
    public_net_namespace = config.ENV_DATA["multus_public_net_namespace"]
    public_net_full_name = f"{public_net_namespace}/{public_net_name}"

    cluster_net_created = config.ENV_DATA["multus_create_cluster_net"]
    cluster_net_name = config.ENV_DATA["multus_cluster_net_name"]
    cluster_net_namespace = config.ENV_DATA["multus_cluster_net_namespace"]
    cluster_net_full_name = f"{cluster_net_namespace}/{cluster_net_name}"

    log.info("Verifying multus NetworkAttachmentDefinitions")
    if public_net_created:
        ocp.OCP(
            resource_name=public_net_full_name,
            kind="network-attachment-definitions",
            namespace=public_net_namespace,
        )
    if cluster_net_created:
        ocp.OCP(
            resource_name=cluster_net_full_name,
            kind="network-attachment-definitions",
            namespace=cluster_net_namespace,
        )

    log.info("Verifying multus networks exist on OSD pods")
    osd_pods = get_osd_pods()
    osd_addresses = dict()
    for _pod in osd_pods:
        pod_networks = _pod.data["metadata"]["annotations"][
            "k8s.v1.cni.cncf.io/networks"
        ]
        if public_net_created:
            assert verify_networks_in_ceph_pod(
                pod_networks, public_net_name, public_net_namespace
            ), f"{public_net_name} not in {pod_networks}"

        osd_id = get_osd_pod_id(_pod)
        log.info(
            "Verify %s and %s ip addresses exists on osd %s.",
            cluster_net_name,
            public_net_name,
            osd_id,
        )
        osd_addresses[osd_id] = dict()
        networks = json.loads(
            _pod.data["metadata"]["annotations"]["k8s.v1.cni.cncf.io/network-status"]
        )
        for network in networks:
            if network["name"] == public_net_full_name:
                for network_ip in network["ips"]:
                    osd_addresses[osd_id]["public_address"] = network_ip
            if network["name"] == cluster_net_full_name:
                for network_ip in network["ips"]:
                    osd_addresses[osd_id]["internal_address"] = network_ip

    log.info("Verifying ceph OSD dump")
    osd_dump_dict = get_ceph_tools_pod().exec_ceph_cmd("ceph osd dump --format json")
    osds_data = osd_dump_dict["osds"]
    for osd_data in osds_data:
        expected_addresses = osd_addresses[str(osd_data["osd"])]
        if public_net_created:
            assert expected_addresses["public_address"] in str(
                osd_data["public_addr"]
            ), (
                f"\nExpected public ip address: {expected_addresses['public_address']}"
                f"\nActual public ip address: {osd_data['public_addr']}"
            )
        if cluster_net_created:
            assert expected_addresses["internal_address"] in str(
                osd_data["cluster_addrs"]
            ), (
                f"\nExpected internal ip address: {expected_addresses['cluster_addrs']}"
                f"\nActual internal ip address: {osd_data['internal_address']}"
            )

    if public_net_created:
        log.info("Verifying multus public network exists on ceph pods")
        mon_pods = get_mon_pods()
        mds_pods = get_mds_pods()
        mgr_pods = get_mgr_pods()
        rgw_pods = get_rgw_pods()
        ceph_pods = [*mon_pods, *mds_pods, *mgr_pods, *rgw_pods]
        for _pod in ceph_pods:
            pod_networks = _pod.data["metadata"]["annotations"][
                "k8s.v1.cni.cncf.io/networks"
            ]
            assert verify_networks_in_ceph_pod(
                pod_networks, public_net_name, public_net_namespace
            ), f"{public_net_name} not in {pod_networks}"

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
            pod_networks = _pod.data["metadata"]["annotations"][
                "k8s.v1.cni.cncf.io/networks"
            ]
            assert verify_networks_in_ceph_pod(
                pod_networks, public_net_name, public_net_namespace
            ), f"{public_net_name} not in {pod_networks}"

        log.info("Verifying MDS Map IPs are in the multus public network range")
        ceph_fs_dump_data = get_ceph_tools_pod().exec_ceph_cmd(
            "ceph fs dump --format json"
        )
        mds_map = ceph_fs_dump_data["filesystems"][0]["mdsmap"]
        for _, gid_data in mds_map["info"].items():
            ip = gid_data["addr"].split(":")[0]
            range = config.ENV_DATA["multus_public_net_range"]
            assert ipaddress.ip_address(ip) in ipaddress.ip_network(range)

    log.info("Verifying StorageCluster multus network data")
    sc = get_storage_cluster()
    sc_data = sc.get().get("items")[0]
    network_data = sc_data["spec"]["network"]
    assert network_data["provider"] == "multus"
    selectors = network_data["selectors"]
    if public_net_created:
        assert selectors["public"] == (
            f"{config.ENV_DATA['multus_public_net_namespace']}/{config.ENV_DATA['multus_public_net_name']}"
        )
    if cluster_net_created:
        assert selectors["cluster"] == (
            f"{config.ENV_DATA['multus_cluster_net_namespace']}/{config.ENV_DATA['multus_cluster_net_name']}"
        )


def verify_networks_in_ceph_pod(pod_networks, net_name, net_namespace):
    """
    Verify network configuration on ceph pod

    Args:
        pod_networks (str): the value of k8s.v1.cni.cncf.io/networks param
        net_name (str): the network-attachment-definitions name
        net_namespace (str): the network-attachment-definitions namespace

    Returns:
        bool: return True if net_name and net_namespce exist in pod_networks otherwise False

    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    if ocs_version >= version.VERSION_4_14:
        pod_networks_list = json.loads(pod_networks)
        return any(
            (
                pod_network["name"] == net_name
                and pod_network["namespace"] == net_namespace
            )
            for pod_network in pod_networks_list
        )
    else:
        return f"{net_namespace}/{net_name}" in pod_networks


def verify_managed_service_resources():
    """
    Verify creation and status of resources specific to OSD and ROSA deployments:
    1. ocs-operator, ose-prometheus-operator csvs are Succeeded
    2. 1 prometheus and 1 alertmanager pods are in Running state
    3. Managedocs components alertmanager, prometheus, storageCluster are in Ready state
    4. Verify that noobaa-operator replicas is set to 0
    5. Verify managed ocs secrets
    6. If cluster is Provider, verify resources specific to provider clusters
    7. Verify that version of Prometheus is 4.10
    8. Verify security restrictions are in place
    9. [temporarily left out] Verify Networkpolicy and EgressNetworkpolicy creation
    """
    # Verify CSV status
    for managed_csv in {
        constants.OCS_CSV_PREFIX,
        constants.OSE_PROMETHEUS_OPERATOR,
    }:
        csvs = csv.get_csvs_start_with_prefix(
            managed_csv, config.ENV_DATA["cluster_namespace"]
        )
        assert (
            len(csvs) == 1
        ), f"Unexpected number of CSVs with {managed_csv} prefix: {len(csvs)}"
        csv_name = csvs[0]["metadata"]["name"]
        csv_obj = csv.CSV(
            resource_name=csv_name, namespace=config.ENV_DATA["cluster_namespace"]
        )
        log.info(f"Check if {csv_name} is in Succeeded phase.")
        csv_obj.wait_for_phase(phase="Succeeded", timeout=600)

    # Verify alerting secrets creation
    verify_managed_secrets()

    # Verify alerting pods are Running
    pod_obj = OCP(
        kind="pod",
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    for alert_pod in {
        (constants.MANAGED_PROMETHEUS_LABEL, 1),
        (constants.MANAGED_ALERTMANAGER_LABEL, 1),
    }:
        pod_obj.wait_for_resource(
            condition="Running", selector=alert_pod[0], resource_count=alert_pod[1]
        )

    # Verify managedocs components are Ready
    log.info("Getting managedocs components data")
    for component_name in {"alertmanager", "prometheus", "storageCluster"}:
        for state in TimeoutSampler(
            timeout=600,
            sleep=10,
            func=managedservice.get_managedocs_component_state,
            component=component_name,
        ):
            log.info(f"State of {component_name} is {state}")
            if state == constants.STATUS_READY:
                break

    # Verify that noobaa-operator replicas is set to 0
    noobaa_deployment = deployment.get_deployments_having_label(
        "operators.coreos.com/mcg-operator.openshift-storage=",
        config.ENV_DATA["cluster_namespace"],
    )[0]
    log.info(f"Noobaa replicas count: {noobaa_deployment.replicas}")
    assert noobaa_deployment.replicas == 0

    # Verify attributes specific to cluster types
    sc = get_storage_cluster()
    sc_data = sc.get()["items"][0]
    if config.ENV_DATA["cluster_type"].lower() == "provider":
        verify_provider_storagecluster(sc_data)
        verify_provider_resources()
        if get_ocs_osd_deployer_version() >= get_semantic_version("2.0.11-0"):
            verify_provider_topology()
    else:
        verify_consumer_storagecluster(sc_data)
        verify_consumer_resources()
    ocp_version = get_semantic_version(get_ocp_version(), only_major_minor=True)
    if ocp_version < VERSION_4_11:
        prometheus_csv = csv.get_csvs_start_with_prefix(
            constants.OSE_PROMETHEUS_OPERATOR, config.ENV_DATA["cluster_namespace"]
        )
        prometheus_version = prometheus_csv[0]["spec"]["version"]
        assert prometheus_version.startswith("4.10.")
    verify_managedocs_security()


def verify_provider_resources():
    """
    Verify resources specific to managed OCS provider:
    1. Ocs-provider-server pod is Running
    2. cephcluster is Ready and its hostNetworking is set to True
    3. Security groups are set up correctly
    """
    # Verify ocs-provider-server pod is Running
    pod_obj = OCP(
        kind="pod",
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    pod_obj.wait_for_resource(
        condition="Running", selector="app=ocsProviderApiServer", resource_count=1
    )

    # Verify that cephcluster is Ready and hostNetworking is True
    cephcluster = OCP(
        kind="CephCluster", namespace=config.ENV_DATA["cluster_namespace"]
    )
    log.info("Waiting for Cephcluster to be Ready")
    cephcluster.wait_for_phase(phase=constants.STATUS_READY, timeout=600)
    cephcluster_yaml = cephcluster.get().get("items")[0]
    log.info("Verifying that cephcluster's hostNetworking is True")
    assert cephcluster_yaml["spec"]["network"][
        "hostNetwork"
    ], f"hostNetwork is {cephcluster_yaml['spec']['network']['hostNetwork']}"

    assert verify_worker_nodes_security_groups()


def verify_consumer_resources():
    """
    Verify resources specific to managed OCS consumer:
    1. MGR endpoint
    2. monitoring endpoint in cephcluster yaml
    3. Verify the default Storageclassclaims
    """
    mgr_endpoint = OCP(
        kind="endpoints",
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.MGR_APP_LABEL,
    )
    mgr_ep_yaml = mgr_endpoint.get().get("items")[0]
    log.info("Verifying that MGR endpoint has an IP address")
    mgr_ip = mgr_ep_yaml["subsets"][0]["addresses"][0]["ip"]
    log.info(f"MGR endpoint IP is {mgr_ip}")
    assert re.match("\\d+(\\.\\d+){3}", mgr_ip)
    cephcluster = OCP(
        kind="CephCluster", namespace=config.ENV_DATA["cluster_namespace"]
    )
    cephcluster_yaml = cephcluster.get().get("items")[0]
    monitoring_endpoint = cephcluster_yaml["spec"]["monitoring"][
        "externalMgrEndpoints"
    ][0]["ip"]
    log.info(f"Monitoring endpoint of cephcluster yaml: {monitoring_endpoint}")
    assert re.match("\\d+(\\.\\d+){3}", monitoring_endpoint)

    ocs_version = version.get_semantic_ocs_version_from_config()

    # Verify the default Storageclassclaims
    if ocs_version >= version.VERSION_4_11:
        storage_class_claim = OCP(
            kind=constants.STORAGECLASSCLAIM,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        for sc_claim in [
            constants.DEFAULT_STORAGECLASS_RBD,
            constants.DEFAULT_STORAGECLASS_CEPHFS,
        ]:
            sc_claim_phase = storage_class_claim.get_resource(
                resource_name=sc_claim, column="PHASE"
            )
            assert sc_claim_phase == constants.STATUS_READY, (
                f"The phase of the storageclassclaim {sc_claim} is {sc_claim_phase}. "
                f"Expected phase is '{constants.STATUS_READY}'"
            )
            log.info(f"Storageclassclaim {sc_claim} is {constants.STATUS_READY}")
        log.info("Verified the status of the default storageclassclaims")


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
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        assert policy_obj.is_exist(
            resource_name=policy[1]
        ), f"{policy[0]} {policy}[1] does not exist in openshift-storage namespace"


def verify_managed_secrets():
    """
    Verify that ocs-converged-pagerduty, ocs-converged-smtp, ocs-converged-deadmanssnitch,
    addon-ocs-provider-parameters, alertmanager-managed-ocs-alertmanager-generated,
    rook-ceph-mon secrets exist in openshift-storage namespace.
    For a provider cluster verify existence of onboarding-ticket-key and ocs-provider-server
    secrets.
    For a consumer cluster verify existence of 5 rook-ceph-client secrets
    """
    secret_ocp_obj = OCP(
        kind=constants.SECRET, namespace=config.ENV_DATA["cluster_namespace"]
    )
    for secret_name in {
        managedservice.get_pagerduty_secret_name(),
        managedservice.get_smtp_secret_name(),
        managedservice.get_dms_secret_name(),
        managedservice.get_parameters_secret_name(),
        constants.MANAGED_ALERTMANAGER_SECRET,
        constants.MANAGED_MON_SECRET,
    }:
        assert secret_ocp_obj.is_exist(
            resource_name=secret_name
        ), f"{secret_name} does not exist in {config.ENV_DATA['cluster_namespace']} namespace"
    if config.ENV_DATA["cluster_type"].lower() == "provider":
        for secret_name in {
            constants.MANAGED_ONBOARDING_SECRET,
            constants.MANAGED_PROVIDER_SERVER_SECRET,
        }:
            assert secret_ocp_obj.is_exist(
                resource_name=secret_name
            ), f"{secret_name} does not exist in {config.ENV_DATA['cluster_namespace']} namespace"
    else:
        secrets = secret_ocp_obj.get().get("items")
        client_secrets = []
        for secret in secrets:
            if secret["metadata"]["name"].startswith("rook-ceph-client"):
                client_secrets.append(secret["metadata"]["name"])
        log.info(f"rook-ceph-client secrets: {client_secrets}")
        assert len(client_secrets) == 5


def verify_provider_storagecluster(sc_data):
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
        "(\\d+(\\.\\d+){3}|[\\w-]+(\\.[\\w-]+)+):\\d{5}",
        sc_data["status"]["storageProviderEndpoint"],
    )
    annotations = sc_data["metadata"]["annotations"]
    log.info(f"Annotations: {annotations}")
    assert annotations["uninstall.ocs.openshift.io/cleanup-policy"] == "delete"
    assert annotations["uninstall.ocs.openshift.io/mode"] == "graceful"


def verify_consumer_storagecluster(sc_data):
    """
    Verify that Storagecluster is has:
    1. externalStorage: enable: true
    2. storageProviderEndpoint: IP:31659
    3. onboardingTicket is present
    4. catsrc existence
    5. requested capacity matches granted capacity
    6. requested and granted capacity fields have a valid value

    Args:
    sc_data (dict): storagecluster data dictionary
    """
    log.info(f"externalStorage: enable: {sc_data['spec']['externalStorage']['enable']}")
    assert sc_data["spec"]["externalStorage"]["enable"]
    log.info(
        f"storageProviderEndpoint: {sc_data['spec']['externalStorage']['storageProviderEndpoint']}"
    )
    assert re.match(
        "\\d+(\\.\\d+){3}:31659",
        sc_data["spec"]["externalStorage"]["storageProviderEndpoint"],
    )
    ticket = sc_data["spec"]["externalStorage"]["onboardingTicket"]
    log.info(
        f"Onboarding ticket begins with: {ticket[:10]} and ends with: {ticket[-10:]}"
    )
    assert len(ticket) > 500
    catsrc = ocp.OCP(
        kind=constants.CATSRC, namespace=config.ENV_DATA["cluster_namespace"]
    )
    catsrc_info = catsrc.get().get("items")[0]
    log.info(f"Catalogsource: {catsrc_info}")
    assert catsrc_info["spec"]["displayName"].startswith(
        "Red Hat OpenShift Data Foundation Managed Service Consumer"
    )
    requested_capacity = sc_data["spec"]["externalStorage"]["requestedCapacity"]
    granted_capacity = sc_data["status"]["externalStorage"]["grantedCapacity"]
    log.info(
        f"Requested capacity: {requested_capacity}. Granted capacity: {granted_capacity}"
    )
    assert requested_capacity == granted_capacity
    assert re.match("\\d+[PT]", granted_capacity)


def verify_managedocs_security():
    """
    Check ocs-osd-deployer-operator permissions:
    1. Verify `runAsUser` is not 0
    2. Verify `SecurityContext.allowPrivilegeEscalation` is set to false
    3. Verify `SecurityContext.capabilities.drop` contains ALL
    """
    pod_obj = OCP(
        kind="pod",
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.MANAGED_CONTROLLER_LABEL,
    )
    deployer_yaml = pod_obj.get().get("items")[0]
    containers = deployer_yaml["spec"]["containers"]
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


def get_ceph_clients():
    """
    Get the yamls of all ceph clients.
    Runs on provider cluster

    Returns:
        list: yamls of all ceph clients
    """
    consumer = ocp.OCP(
        kind="CephClient", namespace=config.ENV_DATA["cluster_namespace"]
    )
    return consumer.get().get("items")


def get_storage_cluster_state(sc_name, namespace=None):
    """
    Get the storage cluster state

    Args:
        sc_name (str): The storage cluster name
        namespace (str): Namespace of the resource. The default value is:
            'config.ENV_DATA["cluster_namespace"]' if None provided

    Returns:
        str: The storage cluster state

    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]
    sc_obj = ocp.OCP(
        kind=constants.STORAGECLUSTER,
        namespace=namespace,
    )
    return sc_obj.get_resource(resource_name=sc_name, column="PHASE")


def get_rook_ceph_mon_per_endpoint_ip():
    """
    Get a dictionary of the rook ceph mon per endpoint ip

    Returns:
        dict: A dictionary of the rook ceph mon per endpoint ip

    """
    configmap_obj = ocp.OCP(
        kind=constants.CONFIGMAP,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.ROOK_CEPH_MON_ENDPOINTS,
    )
    cm_data = configmap_obj.get().get("data").get("data")
    cm_data_list = cm_data.split(sep=",")
    return {d[0]: d[2 : d.index(":")] for d in cm_data_list}


@switch_to_orig_index_at_last
def check_consumer_rook_ceph_mon_endpoints_in_provider_wnodes():
    """
    Check that the rook ceph mon endpoint ips are found in the provider worker node ips

    Returns:
        bool: True, If all the rook ceph mon endpoint ips are found in the
            provider worker nodes. False, otherwise.

    """
    rook_ceph_mon_per_endpoint_ip = get_rook_ceph_mon_per_endpoint_ip()
    log.info(f"rook ceph mon per endpoint ip: {rook_ceph_mon_per_endpoint_ip}")
    provider_wnode_ips = get_provider_internal_node_ips()

    for mon_name, endpoint_ip in rook_ceph_mon_per_endpoint_ip.items():
        if endpoint_ip not in provider_wnode_ips:
            log.warning(
                f"The endpoint ip {endpoint_ip} of mon {mon_name} is not found "
                f"in the provider worker node ips"
            )
            return False

    log.info("All the mon endpoint ips are found in the provider worker node ips")
    return True


def wait_for_consumer_rook_ceph_mon_endpoints_in_provider_wnodes(timeout=180, sleep=10):
    """
    Wait for the rook ceph mon endpoint ips to be found in the provider worker node ips

    Args:
        timeout (int): The time to wait for the rook ceph mon endpoint ips to be found
            in the provider worker node ips
        sleep (int): Time in seconds to sleep between attempts

    Returns:
        bool: True, If all the rook ceph mon endpoint ips are found in the
            provider worker nodes. False, otherwise.

    """
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=check_consumer_rook_ceph_mon_endpoints_in_provider_wnodes,
    )
    return sample.wait_for_func_status(result=True)


def get_consumer_storage_provider_endpoint():
    """
    Get the consumer "storageProviderEndpoint" from the ocs storage cluster

    Returns:
        str: The consumer "storageProviderEndpoint"

    """
    sc_obj = ocp.OCP(
        kind=constants.STORAGECLUSTER,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.DEFAULT_CLUSTERNAME,
    )
    return sc_obj.get()["spec"]["externalStorage"]["storageProviderEndpoint"]


@switch_to_orig_index_at_last
def check_consumer_storage_provider_endpoint_in_provider_wnodes():
    """
    Check that the consumer "storageProviderEndpoint" ip is found in the provider worker node ips

    Returns:
        bool: True, if the consumer "storageProviderEndpoint" ip is found in the
            provider worker node ips. False, otherwise.

    """
    storage_provider_endpoint = get_consumer_storage_provider_endpoint()
    storage_provider_endpoint_ip = storage_provider_endpoint.split(":")[0]
    log.info(
        f"The consumer 'storageProviderEndpoint' ip is: {storage_provider_endpoint_ip}"
    )
    provider_wnode_ips = get_provider_internal_node_ips()

    if storage_provider_endpoint_ip in provider_wnode_ips:
        log.info(
            "The consumer 'storageProviderEndpoint' ip found in the provider worker node ips"
        )
        return True
    else:
        log.warning(
            "The consumer 'storageProviderEndpoint' ip was not found in the provider worker node ips"
        )
        return False


def wait_for_consumer_storage_provider_endpoint_in_provider_wnodes(
    timeout=180, sleep=10
):
    """
    Wait for the consumer "storageProviderEndpoint" ip to be found in the provider worker node ips

    Args:
        timeout (int): timeout in seconds to wait for the consumer "storageProviderEndpoint" ip
            to be found in the provider worker node ips
        sleep (int): Time in seconds to sleep between attempts

    Returns:
        True, if the consumer "storageProviderEndpoint" ip is found in the
            provider worker node ips. False, otherwise.

    """
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=check_consumer_storage_provider_endpoint_in_provider_wnodes,
    )
    return sample.wait_for_func_status(result=True)


def get_storageclass_names_from_storagecluster_spec():
    """
    Retrieve storage class names from the storage cluster's spec.

    This function queries the storage cluster's specification and returns a dictionary containing
    the storage class names for various resources, such as cephFilesystems, cephObjectStores,
    cephBlockPools, cephNonResilientPools, nfs, and encryption.

    Returns:
        dict: A dictionary containing the storage class names for various resources.
            The keys are the names of the resources, and the values are the respective storage
            class names. If a resource does not have a storage class name, it will be set to None.
    """
    sc_obj = ocp.OCP(
        kind=constants.STORAGECLUSTER,
        namespace=config.ENV_DATA["cluster_namespace"],
    )

    keys_to_search = [
        constants.OCS_COMPONENTS_MAP["cephfs"],
        constants.OCS_COMPONENTS_MAP["rgw"],
        constants.OCS_COMPONENTS_MAP["blockpools"],
    ]

    spec_data = sc_obj.get()["items"][0]["spec"]  # Get the "spec" data once

    data = {
        key: value.get("storageClassName")
        for key, value in spec_data.get("managedResources", {}).items()
        if key in keys_to_search and value.get("storageClassName")
    }

    # get custom storageclass name for nonresilientPools
    nonresilientpool_key = constants.OCS_COMPONENTS_MAP["cephnonresilentpools"]
    nonresilientpool_data = spec_data["managedResources"].get(nonresilientpool_key, {})
    storage_class_name = nonresilientpool_data.get("storageClassName")

    if nonresilientpool_data.get("enable") and storage_class_name:
        data[nonresilientpool_key] = storage_class_name

    # Get custom storageclass name for 'nfs' service
    if spec_data.get("nfs", {}).get("enable"):
        nfs_storage_class_name = spec_data["nfs"].get("storageClassName")
        if nfs_storage_class_name:
            data["nfs"] = nfs_storage_class_name

    # Get custom storageclass name for 'encryption' service
    if spec_data.get("encryption", {}).get("enable"):
        encryption_storage_class_name = spec_data["encryption"].get("storageClassName")
        if encryption_storage_class_name:
            data["encryption"] = encryption_storage_class_name

    return data


def check_custom_storageclass_presence(interface=None):
    """
    Verify if the custom-defined storage class names are present in the `oc get sc` output.

    Returns:
        bool: Returns True if all custom-defined storage class names are present \
            in the `oc get sc` output , otherwise False.
    """

    sc_from_spec = get_storageclass_names_from_storagecluster_spec()
    if interface:
        sc_from_spec = {interface: sc_from_spec[interface]}

    if not sc_from_spec:
        raise ValueError("No Custom Storageclass are defined in StorageCluster spec.")

    from ocs_ci.helpers.helpers import get_all_storageclass_names

    sc_list = get_all_storageclass_names()

    missing_sc = [value for value in sc_from_spec.values() if value not in sc_list]

    if missing_sc:
        missing_sc_str = ",".join(missing_sc)
        log.error(
            f"StorageClasses {missing_sc_str}' mentioned in the spec is not exist in the `oc get sc` output"
        )
        return False

    log.info("Custom-defined storage classes are correctly present.")
    return True


def patch_storage_cluster_for_custom_storage_class(
    storage_class_type, storage_class_name=None, action="add"
):
    """
    Patch the storage cluster for a custom storage class.

    This function updates the storage cluster's storage class settings based on the provided storage class type.

    Args:
        storage_class_type (str): The type of storage class ("nfs", "encryption", etc.).
        storage_class_name (str, optional): The name of the custom storage class to be set.
                                            If None, a default name will be generated.
        action (str, optional): The action to perform ("add" or "remove").

    Returns:
        bool: Result of the patch operation.
    """
    if storage_class_name is None:
        storage_class_name = f"custom-{storage_class_type}"

    resource_name = (
        constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE
        if config.DEPLOYMENT["external_mode"]
        else constants.DEFAULT_CLUSTERNAME
    )

    if storage_class_type in ["nfs", "encryption"]:
        path = f"/spec/{storage_class_type}/storageClassName"
    else:
        path = f"/spec/managedResources/{storage_class_type}/storageClassName"

    patch_data = []

    if action == "add":
        patch_data.append(
            {
                "op": "add",
                "path": path,
                "value": storage_class_name,
            }
        )

        log_message = f"Added storage class '{storage_class_name}' of type '{storage_class_type}'."

    elif action == "remove":
        patch_data.append(
            {
                "op": "remove",
                "path": path,
            }
        )
        log_message = f"Removed storage class of type '{storage_class_type}'."
    else:
        log.error(f"Not supported action '{action}' to patch StorageCluster spec.")
        return False

    try:
        sc_obj = get_storage_cluster()
        sc_obj.patch(
            resource_name=resource_name,
            params=patch_data,
            format_type="json",
        )
        log.info(log_message)
    except CommandFailed as err:
        log.error(f"Command Failed with an error :{err}")
        return False

    # Sleeping for 4 seconds to allow the recent patch command to take effect.
    from time import sleep

    sleep(4)

    # Verify the patch operation has created/deleted the storageClass from the cluster.
    from ocs_ci.helpers.helpers import get_all_storageclass_names

    storageclass_list = get_all_storageclass_names()
    log.info(f"StorageClasses On the cluster : {','.join(storageclass_list)}")

    if action == "remove":
        if storage_class_name in storageclass_list:
            log.error(
                f" StorageClass '{storage_class_name}' not removed from the cluster."
            )
            return False
        else:
            log.info(f"StorageClass {storage_class_name} removed from the cluster.")
    elif action == "add":
        if storage_class_name not in storageclass_list:
            log.error(
                f" StorageClass '{storage_class_name}' not created on the cluster."
            )
            return False
        else:
            log.info(f"StorageClass '{storage_class_name}' created on the cluster.")
            return True
    else:
        log.error(f"Invalid action: '{action}'")
        return False


@retry(AssertionError, 50, 20, 5)
def validate_serviceexport():
    """
    validate the serviceexport resource
    Number of osds and mons should match

    """
    serviceexport = OCP(
        kind="ServiceExport", namespace=config.ENV_DATA["cluster_namespace"]
    )
    osd_count = 0
    mon_count = 0
    for ent in serviceexport.get().get("items"):
        if "osd" in ent["metadata"]["name"]:
            osd_count += 1
        elif "mon" in ent["metadata"]["name"]:
            mon_count += 1
    assert (
        osd_count == get_osd_count()
    ), f"osd serviceexport count mismatch {osd_count} != {get_osd_count()} "

    assert mon_count == len(
        get_mon_pods()
    ), f"Mon serviceexport count mismatch {mon_count} != {len(get_mon_pods())}"


def get_storage_size():
    """
    Get the storagecluster storage size

    Returns:
        str: The storagecluster storage size

    """
    sc = get_storage_cluster()
    storage = (
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
    if storage.isdigit() or config.DEPLOYMENT.get("local_storage"):
        # In the case of UI deployment of LSO cluster, the value in StorageCluster CR
        # is set to 1, so we can not take OSD size from there. For LSO we will return
        # the size from PVC.
        pvc = get_deviceset_pvcs()[0]
        return pvc.get()["status"]["capacity"]["storage"]
    else:
        return storage


def resize_osd(new_osd_size, check_size=True):
    """
    Resize the OSD(e.g., from 512 to 1024, 1024 to 2048, etc.)

    Args:
        new_osd_size (str): The new osd size(e.g, 512Gi, 1024Gi, 1Ti, 2Ti, etc.)
        check_size (bool): Check that the given osd size is valid

    Returns:
        bool: True in case if changes are applied. False otherwise

    Raises:
        ValueError: In case the osd size is not valid(start with digits and follow by string)
            or the new osd size is less than the current osd size

    """
    if check_size:
        pattern = r"^\d+[a-zA-Z]+$"
        if not re.match(pattern, new_osd_size):
            raise ValueError(f"The osd size '{new_osd_size}' is not valid")
        new_osd_size_in_gb = convert_device_size(new_osd_size, "GB")
        current_osd_size = get_storage_size()
        current_osd_size_in_gb = convert_device_size(current_osd_size, "GB")
        if new_osd_size_in_gb < current_osd_size_in_gb:
            raise ValueError(
                f"The new osd size {new_osd_size} is less than the "
                f"current osd size {current_osd_size}"
            )

    sc = get_storage_cluster()
    # Patch the OSD storage size
    path = "/spec/storageDeviceSets/0/dataPVCTemplate/spec/resources/requests/storage"
    params = f"""[{{ "op": "replace", "path": "{path}", "value": {new_osd_size}}}]"""
    res = sc.patch(
        resource_name=sc.get()["items"][0]["metadata"]["name"],
        params=params.strip("\n"),
        format_type="json",
    )
    return res


def get_client_storage_provider_endpoint():
    """
    Get the client "storageProviderEndpoint" from the storage-client

    Returns:
        str: The client "storageProviderEndpoint"

    """
    sc_obj = ocp.OCP(
        kind=constants.STORAGECLIENT,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=config.cluster_ctx.ENV_DATA.get("storage_client_name"),
    )
    return sc_obj.get()["spec"]["storageProviderEndpoint"]


def wait_for_storage_client_connected(timeout=180, sleep=10):
    """
    Wait for the storage-client to be in a connected phase

    Args:
        timeout (int): Time to wait for the storage-client to be in a connected phase
        sleep (int): Time in seconds to sleep between attempts

    Raises:
        ResourceWrongStatusException: In case the storage-client didn't reach the desired connected phase

    """
    sc_obj = OCP(
        kind=constants.STORAGECLIENT, namespace=config.ENV_DATA["cluster_namespace"]
    )
    resource_name = config.ENV_DATA.get(
        "storage_client_name", constants.STORAGE_CLIENT_NAME
    )
    sc_obj.wait_for_resource(
        resource_name=resource_name,
        column="PHASE",
        condition="Connected",
        timeout=timeout,
        sleep=sleep,
    )
