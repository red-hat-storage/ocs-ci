import logging
from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.exceptions import CommandFailed, ResourceNotFoundError
from ocs_ci.ocs.machine import get_labeled_nodes
from ocs_ci.ocs.node import get_all_nodes, get_node_objs
from ocs_ci.ocs.ocp import switch_to_project, get_ocs_version
from ocs_ci.ocs.resources.pvc import get_all_pvcs_in_storageclass, get_all_pvcs
from ocs_ci.ocs.resources.storage_cluster import get_all_storageclass
from ocs_ci.utility.localstorage import check_local_volume
from tests.helpers import get_all_pvs, default_volumesnapshotclass

log = logging.getLogger(__name__)


def remove_monitoring_stack_from_ocs():
    """
    Function removes monitoring stack from OCS

    """
    monitoring_obj = ocp.OCP(
        namespace=constants.MONITORING_NAMESPACE, kind='ConfigMap',
    )
    param_cmd = '[{"op": "replace", "path": "/data/config.yaml", "value": ""}]'
    monitoring_obj.patch(
        resource_name='cluster-monitoring-config',
        params=param_cmd,
        format_type='json'
    )


def remove_ocp_registry_from_ocs(platform):
    """
    Function removes OCS registry from OCP cluster

    Args:
        platform (str): the platform the cluster deployed on

    """
    image_registry_obj = ocp.OCP(
        kind=constants.IMAGE_REGISTRY_CONFIG, namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
    )
    params_list = list()
    if platform.lower() == constants.AWS_PLATFORM:
        params_list.append('[{"op": "remove", "path": "/spec/storage"}]')
        params_list.append('[{"op": "remove", "path": "/status/storage"}]')

    elif platform.lower() == constants.VSPHERE_PLATFORM:
        params_list.append('[{"op": "replace", "path": "/spec/storage", "value": {"emptyDir": "{}"}}]')
        params_list.append('[{"op": "replace", "path": "/status/storage", "value": {"emptyDir": "{}"}}]')

    if params_list:
        for params in params_list:
            image_registry_obj.patch(params=params, format_type='json')
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
    assert clusterlogging_obj.delete(resource_name='instance')

    # Deleting the PVCs
    pvc_obj = ocp.OCP(
        kind=constants.PVC, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    pvc_list = get_all_pvcs(namespace=constants.OPENSHIFT_LOGGING_NAMESPACE)
    for pvc in range(len(pvc_list) - 1):
        pvc_obj.delete(resource_name=pvc_list['items'][pvc]['metadata']['name'])


def uninstall_lso(lso_sc):
    """
    Function uninstalls local-volume objects from OCS cluster

    """
    ocp_obj = ocp.OCP()

    sc_obj = (
        ocp.OCP(
            kind=constants.STORAGECLASS,
            resource_name=lso_sc,
            namespace=config.ENV_DATA['local_storage_namespace']
        )
    )

    lv_name = sc_obj.get().get('metadata').get('labels').get('local.storage.openshift.io/owner-name')
    lv_obj = (
        ocp.OCP(
            kind=constants.LOCAL_VOLUME,
            resource_name=lv_name,
            namespace=config.ENV_DATA['local_storage_namespace']
        )
    )

    log.info(f"Local storage was found. using storage class: {lso_sc},  local volume:{lv_name}")

    device_list = lv_obj.get().get('spec').get('storageClassDevices')[0].get('devicePaths')
    storage_node_list = get_labeled_nodes(constants.OPERATOR_NODE_LABEL)

    pv_obj_list = (
        ocp.OCP(
            kind=constants.PV,
            selector=f'storage.openshift.com/local-volume-owner-name={lv_name}',
            namespace=config.ENV_DATA['local_storage_namespace']
        )
    )

    log.info("Deleting local volume PVs")
    for pv in pv_obj_list.get().get('items'):
        log.info(f"deleting pv {pv.get('metadata').get('name')}")
        pv_obj_list.delete(resource_name=pv.get('metadata').get('name'))

    log.info("Removing local volume from storage nodes")
    for node in storage_node_list:
        log.info(f"Removing from node {node}")
        ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=[f"rm -rfv /mnt/local-storage/{lso_sc}"])

    disk_list_str = ""
    for device in device_list:
        disk_list_str = disk_list_str + f" {device}"
    disk_list_str = f"DISKS=\"{disk_list_str}\""
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
    ocs_version = get_ocs_version()

    # List the storage classes
    sc_list = [sc for sc in get_all_storageclass() if sc.get('provisioner') in provisioners]

    # Query for PVCs and OBCs that are using the storage class provisioners listed in the previous step.
    pvc_to_delete = []
    for sc in sc_list:
        pvc_to_delete.extend(pvc for pvc in get_all_pvcs_in_storageclass(
            sc.get('metadata').get('name')) if 'noobaa' not in pvc.name
        )

    log.info("Removing monitoring stack from OpenShift Container Storage")
    remove_monitoring_stack_from_ocs()

    log.info("Removing OpenShift Container Platform registry from OpenShift Container Storage")
    remove_ocp_registry_from_ocs(config.ENV_DATA['platform'])

    log.info("Removing the cluster logging operator from OpenShift Container Storage")
    try:
        remove_cluster_logging_operator_from_ocs()
    except CommandFailed:
        log.info("No cluster logging found")

    log.info("Deleting pvcs")
    for pvc in pvc_to_delete:
        log.info(f"Deleting pvc: {pvc.name}")
        pvc.delete()

    storage_cluster = ocp.OCP(
        kind=constants.STORAGECLUSTER,
        resource_name=constants.DEFAULT_CLUSTERNAME,
        namespace='openshift-storage'
    )

    log.info("Checking for local storage")
    lso_sc = None
    if check_local_volume():
        "Local volume was found. Will be removed later"
        lso_sc = storage_cluster.get().get('spec').get('storageDeviceSets')[0].get(
            'dataPVCTemplate').get('spec').get('storageClassName')

    log.info("Deleting storageCluster object")
    storage_cluster.delete(resource_name=constants.DEFAULT_CLUSTERNAME)

    log.info("Removing CRDs")
    crd_list = ['backingstores.noobaa.io', 'bucketclasses.noobaa.io', 'cephblockpools.ceph.rook.io',
                'cephfilesystems.ceph.rook.io', 'cephnfses.ceph.rook.io',
                'cephobjectstores.ceph.rook.io', 'cephobjectstoreusers.ceph.rook.io', 'noobaas.noobaa.io',
                'ocsinitializations.ocs.openshift.io', 'storageclusterinitializations.ocs.openshift.io',
                'storageclusters.ocs.openshift.io', 'cephclusters.ceph.rook.io']
    for crd in crd_list:
        ocp_obj.exec_oc_cmd(f"delete crd {crd} --timeout=300m")

    log.info("Deleting openshift-storage namespace")
    ocp_obj.delete_project('openshift-storage')
    ocp_obj.wait_for_delete('openshift-storage')
    switch_to_project("default")

    log.info("Removing rook directory from nodes")
    nodes_list = get_labeled_nodes(constants.OPERATOR_NODE_LABEL)
    for node in nodes_list:
        log.info(f"Removing rook from {node}")
        ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=["rm -rf /var/lib/rook"])

    log.info("Removing LSO ")
    if lso_sc is not None:
        uninstall_lso(lso_sc)

    log.info("Delete the storage classes with an openshift-storage provisioner list")
    if ocs_version < '4.5':
        for storage_class in sc_list:
            log.info(f"Deleting storage class {storage_class.get('metadata').get('name')}")
            sc_obj = ocp.OCP(kind=constants.STORAGECLASS)
            sc_obj.delete(resource_name=storage_class.get('metadata').get('name'))
    else:
        log.info("deleting noobaa storage class")
        noobaa_sc = ocp.OCP(kind=constants.STORAGECLASS)
        noobaa_sc.delete(resource_name=constants.NOOBAA_SC)

    log.info("Unlabeling storage nodes")
    nodes_list = get_all_nodes()
    for node in nodes_list:
        node_obj = ocp.OCP(kind=constants.NODE, resource_name=node)
        node_obj.add_label(resource_name=node, label=constants.OPERATOR_NODE_LABEL[:-3] + '-')
        node_obj.add_label(resource_name=node, label=constants.TOPOLOGY_ROOK_LABEL + '-')

    log.info("OCS was removed successfully from cluster ")


def validate_uninstall():
    """
    Test to validate all OCS resources were removed from cluster

    """
    # uninstall OCS if still exists -V
    csv_obj = ocp.OCP(namespace='openshift-storage', kind='', resource_name='csv')
    if csv_obj.is_exist():
        log.info("OCS will be uninstalled")
        uninstall_ocs()

    # checking for OCS storage classes - V
    ocs_sc_list = ['ocs-storagecluster-ceph-rbd',
                   'ocs-storagecluster-cephfs',
                   'ocs-storagecluster-ceph-rgw',
                   'openshift-storage.noobaa.io'
                   ]
    sc_obj = ocp.OCP(kind=constants.STORAGECLASS)
    for sc in ocs_sc_list:
        assert not sc_obj.is_exist(resource_name=sc), f"storage class {sc} was not deleted"

    # checking for OCS PVCs -V
    pvc_list = []
    for sc in ocs_sc_list:
        pvc_list.extend(get_all_pvcs_in_storageclass(sc))
    assert len(pvc_list) == 0, f"OCS PVCs were not deleted {[pvc.name for pvc in pvc_list]}"

    # checking for monitoring map -V
    monitoring_obj = ocp.OCP(
        namespace=constants.MONITORING_NAMESPACE,
        kind='ConfigMap',
        resource_name='cluster-monitoring-config',
    )
    assert monitoring_obj.get().get('data').get('config.yaml') is not None, \
        "OCS was not removed from monitoring stack"

    # checking for registry map - V
    image_registry_obj = ocp.OCP(
        kind=constants.IMAGE_REGISTRY_CONFIG, namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
    )
    assert 'pvc' not in image_registry_obj.get().get('spec').get('storage'), "OCS was not removed from regitry map"

    # checking for cluster logging object
    clusterlogging_obj = ocp.OCP(
        kind=constants.CLUSTER_LOGGING, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )

    assert not clusterlogging_obj.is_exist(), "cluster logging object was not deleted"  # checking for bucket classes

    # checking for backing store

    # checking for local volume - V
    assert not check_local_volume(), "local volume was not deleted"

    # checking for storage cluster - V
    storage_cluster = ocp.OCP(
        kind=constants.STORAGECLUSTER,
        resource_name=constants.DEFAULT_CLUSTERNAME,
        namespace='openshift-storage'
    )

    assert not storage_cluster.is_exist(resource_name=constants.DEFAULT_CLUSTERNAME), "storage cluster was not deleted"

    # checking for openshift-storage namespace -V
    openshift_storage_namespace = ocp.OCP(
        kind=constants.NAMESPACE,
        namespace='openshift-storage'
    )

    assert not openshift_storage_namespace.is_exist(resource_name='openshift-storage'), \
        "openshift-storage namespace was not deleted "

    # checking for PVs
    pvc_list = get_all_pvs()
    for pv in pvc_list:
        assert 'ocs-storagecluster-ceph-rbd|' or 'ocs-storagecluster-cephfs' in pv.get(
            'name'), f"OCS pv {pv.get('name')} was not deleted "

    # checking for LSO artifacts on nodes -wait for ideas
    ocp_obj = ocp.OCP()
    nodes_list = get_node_objs()
    for node in nodes_list:
        try:
            assert not ocp_obj.exec_oc_debug_cmd(
                node=node, cmd_list=["ls -l /var/lib/rook"]), "OCS artificats were not deleted from nodes "
        except CommandFailed:
            pass

    # checking for rook artificats on nodes
    for node in nodes_list:
        try:
            assert not ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=["ls -l /mnt/local-storage/local-block"]), \
                "LSO artificats were not deleted from nodes "
        except CommandFailed:
            pass

    # checking for labels on nodes - v
    for node in nodes_list:
        labels = node.get().get('metadata').get('labels')
        print(labels)
        assert constants.OPERATOR_NODE_LABEL in labels, f"OCS labels was not removed from {node} node"

    # check volumesnapshotclass,snapshot volume, snapshot content
    try:
        volume_snapshot_classrbd = default_volumesnapshotclass('CephBlockPool')
        volume_snapshot_classfs = default_volumesnapshotclass('CephFileSystem')
        volume_snapshot_classrbd.get()
        volume_snapshot_classfs.get()
    except Exception:
        pass

    # checking for CRDs - NOT CRITICAL
    crds = ['backingstores.noobaa.io', 'bucketclasses.noobaa.io', 'cephblockpools.ceph.rook.io',
            'cephfilesystems.ceph.rook.io', 'cephnfses.ceph.rook.io',
            'cephobjectstores.ceph.rook.io', 'cephobjectstoreusers.ceph.rook.io', 'noobaas.noobaa.io',
            'ocsinitializations.ocs.openshift.io', 'storageclusterinitializations.ocs.openshift.io',
            'storageclusters.ocs.openshift.io', 'cephclusters.ceph.rook.io']
    for crd in crds:
        try:
            ocp_obj.exec_oc_cmd(f'get crd {crd}')
        except ResourceNotFoundError:
            pass
