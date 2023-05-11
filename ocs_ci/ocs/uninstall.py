import logging
from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.machine import get_labeled_nodes
from ocs_ci.ocs.node import label_nodes, taint_nodes, get_all_nodes, get_node_objs
from ocs_ci.ocs.ocp import switch_to_project
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.resources.pvc import get_all_pvcs_in_storageclass, get_all_pvcs
from ocs_ci.ocs.resources.storage_cluster import get_all_storageclass
from ocs_ci.utility import rosa
from ocs_ci.utility.localstorage import check_local_volume_local_volume_set
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


def remove_monitoring_stack_from_ocs():
    """
    Function removes monitoring stack from OCS

    """
    monitoring_obj = ocp.OCP(
        namespace=constants.MONITORING_NAMESPACE,
        kind="ConfigMap",
    )
    param_cmd = '[{"op": "replace", "path": "/data/config.yaml", "value": ""}]'
    monitoring_obj.patch(
        resource_name="cluster-monitoring-config", params=param_cmd, format_type="json"
    )


def remove_ocp_registry_from_ocs(platform):
    """
    Function removes OCS registry from OCP cluster

    Args:
        platform (str): the platform the cluster deployed on

    """
    image_registry_obj = ocp.OCP(
        kind=constants.IMAGE_REGISTRY_CONFIG,
        namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
    )
    params_list = list()
    if (
        platform.lower() == constants.AWS_PLATFORM
        or platform.lower() == constants.RHV_PLATFORM
    ):
        params_list.append('[{"op": "remove", "path": "/spec/storage"}]')
        params_list.append('[{"op": "remove", "path": "/status/storage"}]')

    elif platform.lower() == constants.VSPHERE_PLATFORM:
        params_list.append(
            '[{"op": "replace", "path": "/spec/storage", "value": {"emptyDir": "{}"}}]'
        )
        params_list.append(
            '[{"op": "replace", "path": "/status/storage", "value": {"emptyDir": "{}"}}]'
        )

    if params_list:
        for params in params_list:
            image_registry_obj.patch(params=params, format_type="json")
    else:
        log.info("platform registry not supported")


def remove_cluster_logging_operator_from_ocs():
    """
    Function removes cluster logging operator from OCS

    """
    # Deleting the clusterlogging instance
    clusterlogging_obj = ocp.OCP(
        kind=constants.CLUSTER_LOGGING, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    assert clusterlogging_obj.delete(resource_name="instance")

    # Deleting the PVCs
    pvc_obj = ocp.OCP(
        kind=constants.PVC, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    pvc_list = get_all_pvcs(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)
    for pvc in range(len(pvc_list) - 1):
        pvc_obj.delete(resource_name=pvc_list["items"][pvc]["metadata"]["name"])


def uninstall_lso(lso_sc):
    """
    Function uninstalls local-volume objects from OCS cluster

    """
    ocp_obj = ocp.OCP()
    sc_obj = ocp.OCP(
        kind=constants.STORAGECLASS,
        resource_name=lso_sc,
        namespace=config.ENV_DATA["local_storage_namespace"],
    )

    log.info("Deleting local volume set")
    lvs_obj = ocp.OCP(
        kind=constants.LOCAL_VOLUME_SET,
        namespace=config.ENV_DATA["local_storage_namespace"],
    )
    lvs_obj.delete(constants.LOCAL_VOLUME_SET_YAML)

    pv_obj_list = ocp.OCP(
        kind=constants.PV,
        namespace=config.ENV_DATA["local_storage_namespace"],
    )

    log.info("Deleting local volume PVs")
    for pv in pv_obj_list.get().get("items"):
        log.info(f"deleting pv {pv.get('metadata').get('name')}")
        pv_obj_list.delete(resource_name=pv.get("metadata").get("name"))

    log.info(f"Deleting storage class {lso_sc}")
    sc_obj.delete(resource_name=lso_sc)

    log.info("deleting local volume discovery")
    lvd_obj = ocp.OCP(
        kind=constants.LOCAL_VOLUME_DISCOVERY,
        namespace=config.ENV_DATA["local_storage_namespace"],
    )
    lvd_obj.delete(yaml_file=constants.LOCAL_VOLUME_DISCOVERY_YAML)

    log.info("Removing local volume from storage nodes")
    storage_node_list = get_labeled_nodes(constants.OPERATOR_NODE_LABEL)
    for node in storage_node_list:
        log.info(f"Removing from node {node}")
        ocp_obj.exec_oc_debug_cmd(
            node=node, cmd_list=[f"rm -rfv /mnt/local-storage/{lso_sc}"]
        )


def uninstall_ocs():
    """
    The function uninstalls the OCS operator from a openshift
    cluster and removes all its settings and dependencies

    """
    ocp_obj = ocp.OCP()

    log.info("deleting volume snapshots")
    vs_ocp_obj = ocp.OCP(kind=constants.VOLUMESNAPSHOT)
    vs_list = vs_ocp_obj.get(all_namespaces=True)["items"]
    for vs in vs_list:
        vs_obj = ocp.OCP(
            kind=constants.VOLUMESNAPSHOT, namespace=vs.get("metadata").get("namespace")
        )
        vs_obj.delete(resource_name=vs.get("metadata").get("name"))

    log.info("queering for OCS PVCs")
    provisioners = constants.OCS_PROVISIONERS
    sc_list = [
        sc for sc in get_all_storageclass() if sc.get("provisioner") in provisioners
    ]

    pvc_to_delete = []
    for sc in sc_list:
        pvc_to_delete.extend(
            pvc
            for pvc in get_all_pvcs_in_storageclass(sc.get("metadata").get("name"))
            if "noobaa" not in pvc.name
        )

    if config.ENV_DATA["platform"].lower() == constants.ROSA_PLATFORM:
        log.info("Deleting OCS PVCs")
        for pvc in pvc_to_delete:
            log.info(f"Deleting PVC: {pvc.name}")
            pvc.delete()
        rosa.delete_odf_addon(config.ENV_DATA["cluster_name"])
        return None
    log.info("Removing monitoring stack from OpenShift Container Storage")
    remove_monitoring_stack_from_ocs()

    log.info(
        "Removing OpenShift Container Platform registry from OpenShift Container Storage"
    )
    remove_ocp_registry_from_ocs(config.ENV_DATA["platform"])

    log.info("Removing the cluster logging operator from OpenShift Container Storage")
    try:
        remove_cluster_logging_operator_from_ocs()
    except CommandFailed:
        log.info("No cluster logging found")

    log.info("Deleting OCS PVCs")
    for pvc in pvc_to_delete:
        log.info(f"Deleting PVC: {pvc.name}")
        pvc.delete()

    ns_name = config.ENV_DATA["cluster_namespace"]
    storage_cluster = ocp.OCP(
        kind=constants.STORAGECLUSTER,
        resource_name=constants.DEFAULT_CLUSTERNAME,
        namespace=ns_name,
    )

    log.info("Checking for local storage")
    lso_sc = None
    if check_local_volume_local_volume_set():
        "Local volume was found. Will be removed later"
        lso_sc = (
            storage_cluster.get()
            .get("spec")
            .get("storageDeviceSets")[0]
            .get("dataPVCTemplate")
            .get("spec")
            .get("storageClassName")
        )

    cleanup_policy = (
        storage_cluster.get()
        .get("metadata")
        .get("annotations")
        .get("uninstall.ocs.openshift.io/cleanup-policy")
    )

    log.info("Deleting storageCluster object")
    storage_cluster.delete(resource_name=constants.DEFAULT_CLUSTERNAME)

    if cleanup_policy == "delete":
        log.info("Cleanup policy set to delete. checking cleanup pods")
        cleanup_pods = [
            pod for pod in get_all_pods() if "cluster-cleanup-job" in pod.name
        ]
        for pod in cleanup_pods:
            while pod.get().get("status").get("phase") != "Succeeded":
                log.info(f"waiting for cleanup pod {pod.name} to complete")
                TimeoutSampler(timeout=10, sleep=30)
            log.info(f"Cleanup pod {pod.name} completed successfully ")
        # no need to confirm var/vib/rook was deleted from nodes if all cleanup pods are completed.
    else:
        log.info("Cleanup policy set to retain. skipping nodes cleanup")

    log.info("Deleting cluster namespace {ns_name}")
    ocp_obj.delete_project(ns_name)
    ocp_obj.wait_for_delete(ns_name)
    switch_to_project(constants.DEFAULT_NAMESPACE)

    # step 10: TODO remove crypto from nodes.
    """for node in storage_node_list:
        log.info(f"removing encryption from {node}")
        ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=[])"""

    if lso_sc is not None:
        log.info("Removing LSO")
        try:
            uninstall_lso(lso_sc)
        except Exception as e:
            log.info(f"LSO removal failed.{e}")

    log.info("deleting noobaa storage class")
    noobaa_sc = ocp.OCP(kind=constants.STORAGECLASS)
    noobaa_sc.delete(resource_name=constants.NOOBAA_SC)

    nodes = get_all_nodes()
    node_objs = get_node_objs(nodes)

    log.info("Unlabeling storage nodes")
    label_nodes(nodes=node_objs, label=constants.OPERATOR_NODE_LABEL[:-3] + "-")
    label_nodes(nodes=node_objs, label=constants.TOPOLOGY_ROOK_LABEL + "-")

    log.info("Removing taints from storage nodes")
    taint_nodes(nodes=nodes, taint_label=constants.OPERATOR_NODE_TAINT + "-")

    log.info("Deleting remaining OCS PVs (if there are any)")
    try:
        rbd_pv = ocp.OCP(kind=constants.PV, resource_name="ocs-storagecluster-ceph-rbd")
        fs_pv = ocp.OCP(kind=constants.PV, resource_name="ocs-storagecluster-cephfs")
        rbd_pv.delete()
        fs_pv.delete()
        log.info("OCS PVs deleted")
    except Exception as e:
        log.info(f"OCS PV(s) not found. {e}")

    log.info("Removing CRDs")
    crd_list = [
        "backingstores.noobaa.io",
        "bucketclasses.noobaa.io",
        "cephblockpools.ceph.rook.io",
        "cephclusters.ceph.rook.io",
        "cephfilesystems.ceph.rook.io",
        "cephnfses.ceph.rook.io",
        "cephobjectstores.ceph.rook.io",
        "cephobjectstoreusers.ceph.rook.io",
        "noobaas.noobaa.io",
        "ocsinitializations.ocs.openshift.io",
        "storageclusters.ocs.openshift.io",
        "cephclients.ceph.rook.io",
        "cephobjectrealms.ceph.rook.io",
        "cephobjectzonegroups.ceph.rook.io",
        "cephobjectzones.ceph.rook.io",
        "cephrbdmirrors.ceph.rook.io",
    ]

    for crd in crd_list:
        try:
            ocp_obj.exec_oc_cmd(f"delete crd {crd} --timeout=300m")
        except Exception:
            log.info(f"crd {crd} was not found")
