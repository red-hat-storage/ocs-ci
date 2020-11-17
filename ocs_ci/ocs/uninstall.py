import logging
from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.machine import get_labeled_nodes
from ocs_ci.ocs.node import label_nodes, taint_nodes
from ocs_ci.ocs.ocp import switch_to_project
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.resources.pvc import get_all_pvcs_in_storageclass, get_all_pvcs
from ocs_ci.ocs.resources.storage_cluster import get_all_storageclass
from ocs_ci.utility.localstorage import check_local_volume

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
    if platform.lower() == constants.AWS_PLATFORM:
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

    lv_name = (
        sc_obj.get()
        .get("metadata")
        .get("labels")
        .get("local.storage.openshift.io/owner-name")
    )
    lv_obj = ocp.OCP(
        kind=constants.LOCAL_VOLUME,
        resource_name=lv_name,
        namespace=config.ENV_DATA["local_storage_namespace"],
    )

    log.info(
        f"Local storage was found. using storage class: {lso_sc},  local volume:{lv_name}"
    )

    device_list = (
        lv_obj.get().get("spec").get("storageClassDevices")[0].get("devicePaths")
    )
    storage_node_list = get_labeled_nodes(constants.OPERATOR_NODE_LABEL)

    pv_obj_list = ocp.OCP(
        kind=constants.PV,
        selector=f"storage.openshift.com/local-volume-owner-name={lv_name}",
        namespace=config.ENV_DATA["local_storage_namespace"],
    )

    log.info("Deleting local volume PVs")
    for pv in pv_obj_list.get().get("items"):
        log.info(f"deleting pv {pv.get('metadata').get('name')}")
        pv_obj_list.delete(resource_name=pv.get("metadata").get("name"))

    log.info("Removing local volume from storage nodes")
    for node in storage_node_list:
        log.info(f"Removing from node {node}")
        ocp_obj.exec_oc_debug_cmd(
            node=node, cmd_list=[f"rm -rfv /mnt/local-storage/{lso_sc}"]
        )

    disk_list_str = ""
    for device in device_list:
        disk_list_str = disk_list_str + f" {device}"
    disk_list_str = f'DISKS="{disk_list_str}"'
    log.info(f"The disk list is {disk_list_str}")

    sgd_command = "for disk in $DISKS; do sgdisk --zap-all $disk;done"
    log.info("Wiping disks on storage nodes ")
    for node in storage_node_list:
        log.info(f"Wiping on node {node}")
        cmd_list = [disk_list_str, sgd_command]
        ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=cmd_list)

    log.info(f"Deleting storage class {lso_sc}")
    sc_obj.delete(resource_name=lso_sc)

    log.info(f"Deleting local volume {lv_name}")
    lv_obj.delete(resource_name=lv_name)


def uninstall_ocs():
    """
    The function uninstalls the OCS operator from a openshift
    cluster and removes all its settings and dependencies

    """
    ocp_obj = ocp.OCP()
    provisioners = constants.OCS_PROVISIONERS

    # step1:  List the storage classes using OCS provisioners
    sc_list = [
        sc for sc in get_all_storageclass() if sc.get("provisioner") in provisioners
    ]

    # step 2: list and delete all existing volume snapshots
    vs_ocp_obj = ocp.OCP(kind=constants.VOLUMESNAPSHOT)
    vs_list = vs_ocp_obj.get(all_namespaces=True)["items"]
    for vs in vs_list:
        vs_obj = ocp.OCP(
            kind=constants.VOLUMESNAPSHOT, namespace=vs.get("metadata").get("namespace")
        )
        vs_obj.delete(resource_name=vs.get("metadata").get("name"))

    # step 3: # Query for PVCs and OBCs that are using the storage class provisioners listed in the previous step.
    pvc_to_delete = []
    for sc in sc_list:
        pvc_to_delete.extend(
            pvc
            for pvc in get_all_pvcs_in_storageclass(sc.get("metadata").get("name"))
            if "noobaa" not in pvc.name
        )

    # step 4:
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

    log.info("Deleting pvcs")
    for pvc in pvc_to_delete:
        log.info(f"Deleting pvc: {pvc.name}")
        pvc.delete()

    # TODO: delete all noobaa objects (bucketclass, obs, backingstore)

    # step 5: check for local storage
    storage_cluster = ocp.OCP(
        kind=constants.STORAGECLUSTER,
        resource_name=constants.DEFAULT_CLUSTERNAME,
        namespace="openshift-storage",
    )

    log.info("Checking for local storage")
    lso_sc = None
    if check_local_volume():
        "Local volume was found. Will be removed later"
        lso_sc = (
            storage_cluster.get()
            .get("spec")
            .get("storageDeviceSets")[0]
            .get("dataPVCTemplate")
            .get("spec")
            .get("storageClassName")
        )

    # step 6: delete storagecluster
    cleanup_policy = (
        storage_cluster.get()
        .get("metadata")
        .get("annotations")
        .get("uninstall.ocs.openshift.io/cleanup-policy")
    )

    log.info("Deleting storageCluster object")
    storage_cluster.delete(resource_name=constants.DEFAULT_CLUSTERNAME)

    # step 7+9: check cleanup pods are compleated
    storage_node_list = get_labeled_nodes(constants.OPERATOR_NODE_LABEL)
    if cleanup_policy == "delete":
        # check clean up pods
        cleanup_pods = [
            pod for pod in get_all_pods() if "cluster-cleanup-job" in pod.name
        ]
        for pod in cleanup_pods:
            if pod.get().get("status").get("phase") == "Succeeded":
                log.info(f"Cleanup pod {pod.name} completed successfully ")
            else:
                log.error(f"Cleanup pod {pod.name} did not complete")
        # confirm var/lib/rook is deleted
        for node in storage_node_list:
            try:
                assert not ocp_obj.exec_oc_debug_cmd(
                    node=node, cmd_list=["ls -l var/lib/rook"]
                ), "OCS artificats were not deleted from nodes "
            except CommandFailed:
                pass

        pass

    # step 8: delete namespace
    log.info("Deleting openshift-storage namespace")
    ocp_obj.delete_project("openshift-storage")
    ocp_obj.wait_for_delete("openshift-storage")
    switch_to_project("default")

    # step 10: TODO remove crypto from nodes
    for node in storage_node_list:
        log.info(f"removing encryption from {node}")
        ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=[])

    # step 11: remove LSO
    if lso_sc is not None:
        uninstall_lso(lso_sc)  # TODO check lso func

    # step 12: delete noobaa sc
    log.info("deleting noobaa storage class")
    noobaa_sc = ocp.OCP(kind=constants.STORAGECLASS)
    noobaa_sc.delete(resource_name=constants.NOOBAA_SC)

    # step 13: unlable storage nodes
    node_objs = ocp.OCP(kind=constants.NODE).get().get("items")
    label_nodes(nodes=node_objs, label=constants.OPERATOR_NODE_LABEL[:-3] + "-")
    label_nodes(nodes=node_objs, label=constants.TOPOLOGY_ROOK_LABEL + "-")

    # remove taint from nodes
    taint_nodes(nodes=node_objs, taint_label=constants.OCS_TAINT + "-")

    # step 14: delete remaining PVs
    try:
        rbd_pv = ocp.OCP(kind=constants.PV, resource_name="ocs-storagecluster-ceph-rbd")
        fs_pv = ocp.OCP(kind=constants.PV, resource_name="ocs-storagecluster-cephfs")
        rbd_pv.delete()
        fs_pv.delete()
        log.info("OCS PVs deleted")
    except Exception as e:
        log.info(f"OCS PV(s) not found. {e}")

    # step 15: remove CRSD
    log.info("Removing CRDs")
    crd_list = [
        "crd backingstores.noobaa.io",
        "bucketclasses.noobaa.io",
        "cephblockpools.ceph.rook.io",
        "cephclusters.ceph.rook.io",
        "cephfilesystems.ceph.rook.io",
        "cephnfses.ceph.rook.io",
        "cephobjectstores.ceph.rook.io",
        "cephobjectstoreusers.ceph.rook.io",
        "noobaas.noobaa.io",
        "ocsinitializations.ocs.openshift.io",
        "storageclusterinitializations.ocs.openshift.io",
        "storageclusters.ocs.openshift.io",
        "cephclients.ceph.rook.io",
        "cephobjectrealms.ceph.rook.io",
        "cephobjectzonegroups.ceph.rook.io",
        "cephobjectzones.ceph.rook.io",
        "cephrbdmirrors.ceph.rook.io",
    ]
    for crd in crd_list:
        ocp_obj.exec_oc_cmd(f"delete crd {crd} --timeout=300m")
