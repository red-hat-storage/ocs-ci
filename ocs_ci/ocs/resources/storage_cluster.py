"""
StorageCluster related functionalities
"""
from ocs_ci.ocs.exceptions import ResourceNotFoundError
from ocs_ci.ocs.ocp import OCP, get_images
from jsonschema import validate
from ocs_ci.framework import config
import logging
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.packagemanifest import get_selector_for_ocs_operator, PackageManifest
from ocs_ci.ocs.node import get_compute_node_names
from ocs_ci.utility import utils, localstorage


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
            resource_name=resource_name, kind='StorageCluster', *args, **kwargs
        )


def ocs_install_verification(
    timeout=600, skip_osd_distribution_check=False, ocs_registry_image=None,
    post_upgrade_verification=False,
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

    """
    from ocs_ci.ocs.node import get_typed_nodes
    from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs
    from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_all_pods
    from ocs_ci.ocs.cluster import validate_cluster_on_pvc
    number_of_worker_nodes = len(get_typed_nodes())
    namespace = config.ENV_DATA['cluster_namespace']
    log.info("Verifying OCS installation")

    # Verify OCS CSV is in Succeeded phase
    log.info("verifying ocs csv")
    operator_selector = get_selector_for_ocs_operator()
    ocs_package_manifest = PackageManifest(
        resource_name=defaults.OCS_OPERATOR_NAME, selector=operator_selector,
    )
    channel = config.DEPLOYMENT.get('ocs_csv_channel')
    ocs_csv_name = ocs_package_manifest.get_current_csv(channel=channel)
    ocs_csv = CSV(
        resource_name=ocs_csv_name, namespace=namespace
    )
    log.info(f"Check if OCS operator: {ocs_csv_name} is in Succeeded phase.")
    ocs_csv.wait_for_phase(phase="Succeeded", timeout=timeout)
    # Verify if OCS CSV has proper version.
    csv_version = ocs_csv.data['spec']['version']
    ocs_version = config.ENV_DATA['ocs_version']
    log.info(
        f"Check if OCS version: {ocs_version} matches with CSV: {csv_version}"
    )
    assert ocs_version in csv_version, (
        f"OCS version: {ocs_version} mismatch with CSV version {csv_version}"
    )
    # Verify if OCS CSV has the same version in provided CI build.
    ocs_registry_image = ocs_registry_image or config.DEPLOYMENT.get(
        'ocs_registry_image'
    )
    if ocs_registry_image and ocs_registry_image.endswith(".ci"):
        ocs_registry_image = ocs_registry_image.split(":")[1]
        log.info(
            f"Check if OCS registry image: {ocs_registry_image} matches with "
            f"CSV: {csv_version}"
        )
        ignore_csv_mismatch = config.DEPLOYMENT.get('ignore_csv_mismatch')
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
    storage_cluster_name = config.ENV_DATA['storage_cluster_name']
    log.info("Verifying status of storage cluster: %s", storage_cluster_name)
    storage_cluster = StorageCluster(
        resource_name=storage_cluster_name,
        namespace=namespace,
    )
    log.info(
        f"Check if StorageCluster: {storage_cluster_name} is in"
        f"Succeeded phase"
    )
    storage_cluster.wait_for_phase(phase='Ready', timeout=timeout)

    # Verify pods in running state and proper counts
    log.info("Verifying pod states and counts")
    pod = OCP(
        kind=constants.POD, namespace=namespace
    )
    # ocs-operator
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OCS_OPERATOR_LABEL,
        timeout=timeout
    )
    # rook-ceph-operator
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OPERATOR_LABEL,
        timeout=timeout
    )
    # noobaa
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.NOOBAA_APP_LABEL,
        resource_count=2,
        timeout=timeout
    )
    # mons
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.MON_APP_LABEL,
        resource_count=3,
        timeout=timeout
    )
    # csi-cephfsplugin
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_CEPHFSPLUGIN_LABEL,
        resource_count=number_of_worker_nodes,
        timeout=timeout
    )
    # csi-cephfsplugin-provisioner
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
        resource_count=2,
        timeout=timeout
    )
    # csi-rbdplugin
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_RBDPLUGIN_LABEL,
        resource_count=number_of_worker_nodes,
        timeout=timeout
    )
    # csi-rbdplugin-provisioner
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
        resource_count=2,
        timeout=timeout
    )
    # osds
    osd_count = (
        int(storage_cluster.data['spec']['storageDeviceSets'][0]['count'])
        * int(storage_cluster.data['spec']['storageDeviceSets'][0]['replica'])
    )
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OSD_APP_LABEL,
        resource_count=osd_count,
        timeout=timeout
    )
    # mgr
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.MGR_APP_LABEL,
        timeout=timeout
    )
    # mds
    assert pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.MDS_APP_LABEL,
        resource_count=2,
        timeout=timeout
    )

    # rgw check only for VmWare and BM
    if config.ENV_DATA.get('platform') in constants.ON_PREM_PLATFORMS:
        rgw_count = 2 if float(config.ENV_DATA['ocs_version']) >= 4.5 else 1
        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.RGW_APP_LABEL,
            resource_count=rgw_count,
            timeout=timeout
        )

    # Verify StorageClasses (1 ceph-fs, 1 ceph-rbd)
    log.info("Verifying storage classes")
    storage_class = OCP(
        kind=constants.STORAGECLASS, namespace=namespace
    )
    storage_cluster_name = config.ENV_DATA['storage_cluster_name']
    required_storage_classes = {
        f'{storage_cluster_name}-cephfs',
        f'{storage_cluster_name}-ceph-rbd'
    }
    storage_classes = storage_class.get()
    storage_class_names = {
        item['metadata']['name'] for item in storage_classes['items']
    }
    assert required_storage_classes.issubset(storage_class_names)

    # Verify OSDs are distributed
    if not skip_osd_distribution_check:
        log.info("Verifying OSDs are distributed evenly across worker nodes")
        ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
        osds = ocp_pod_obj.get(selector=constants.OSD_APP_LABEL)['items']
        deviceset_count = get_deviceset_count()
        node_names = [osd['spec']['nodeName'] for osd in osds]
        for node in node_names:
            assert not node_names.count(node) > deviceset_count, (
                "OSD's are not distributed evenly across worker nodes"
            )

    # Verify that CSI driver object contains provisioner names
    log.info("Verifying CSI driver object contains provisioner names.")
    csi_driver = OCP(kind="CSIDriver")
    assert {defaults.CEPHFS_PROVISIONER, defaults.RBD_PROVISIONER} == (
        {item['metadata']['name'] for item in csi_driver.get()['items']}
    )

    # Verify node and provisioner secret names in storage class
    log.info("Verifying node and provisioner secret names in storage class.")
    sc_rbd = storage_class.get(
        resource_name=constants.DEFAULT_STORAGECLASS_RBD
    )
    sc_cephfs = storage_class.get(
        resource_name=constants.DEFAULT_STORAGECLASS_CEPHFS
    )
    assert sc_rbd['parameters']['csi.storage.k8s.io/node-stage-secret-name'] == constants.RBD_NODE_SECRET
    assert sc_rbd['parameters']['csi.storage.k8s.io/provisioner-secret-name'] == constants.RBD_PROVISIONER_SECRET
    assert sc_cephfs['parameters']['csi.storage.k8s.io/node-stage-secret-name'] == constants.CEPHFS_NODE_SECRET
    assert sc_cephfs['parameters']['csi.storage.k8s.io/provisioner-secret-name'] == constants.CEPHFS_PROVISIONER_SECRET
    log.info("Verified node and provisioner secret names in storage class.")

    # Verify ceph osd tree output
    log.info(
        "Verifying ceph osd tree output and checking for device set PVC names "
        "in the output."
    )

    if (
        config.DEPLOYMENT.get('local_storage')
        and config.ENV_DATA['platform'] != constants.BAREMETALPSI_PLATFORM
    ):
        deviceset_pvcs = get_compute_node_names()
    else:
        deviceset_pvcs = [pvc.name for pvc in get_deviceset_pvcs()]

    ct_pod = get_ceph_tools_pod()
    osd_tree = ct_pod.exec_ceph_cmd(ceph_cmd='ceph osd tree', format='json')
    schemas = {
        'root': constants.OSD_TREE_ROOT,
        'rack': constants.OSD_TREE_RACK,
        'host': constants.OSD_TREE_HOST,
        'osd': constants.OSD_TREE_OSD,
        'region': constants.OSD_TREE_REGION,
        'zone': constants.OSD_TREE_ZONE
    }
    schemas['host']['properties']['name'] = {'enum': deviceset_pvcs}
    for item in osd_tree['nodes']:
        validate(instance=item, schema=schemas[item['type']])
        if item['type'] == 'host':
            deviceset_pvcs.remove(item['name'])
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
    log.info("Verifying CSI snapshotter is not present.")
    provisioner_pods = get_all_pods(
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        selector=[
            constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
            constants.CSI_RBDPLUGIN_PROVISIONER_LABEL
        ]
    )
    for pod_obj in provisioner_pods:
        pod_info = pod_obj.get()
        for container, image in get_images(data=pod_info).items():
            assert ('snapshot' not in container) and ('snapshot' not in image), (
                f"Snapshot container is present in {pod_obj.name} pod. "
                f"Container {container}. Image {image}"
            )
    deployments = ocs_csv.get()['spec']['install']['spec']['deployments']
    rook_ceph_operator_deployment = [
        deployment_val for deployment_val in deployments if deployment_val['name'] == 'rook-ceph-operator'
    ]
    assert {'name': 'CSI_ENABLE_SNAPSHOTTER', 'value': 'false'} in (
        rook_ceph_operator_deployment[0]['spec']['template']['spec']['containers'][0]['env']
    ), "CSI_ENABLE_SNAPSHOTTER value is not set to 'false'."
    log.info("Verified: CSI snapshotter is not present.")

    # Verify pool crush rule is with "type": "zone"
    if utils.get_az_count() == 3:
        log.info("Verifying pool crush rule is with type: zone")
        crush_dump = ct_pod.exec_ceph_cmd(
            ceph_cmd='ceph osd crush dump', format=''
        )
        pool_names = [
            constants.METADATA_POOL, constants.DEFAULT_BLOCKPOOL,
            constants.DATA_POOL
        ]
        crush_rules = [rule for rule in crush_dump['rules'] if rule['rule_name'] in pool_names]
        for crush_rule in crush_rules:
            assert [
                item for item in crush_rule['steps'] if item.get('type') == 'zone'
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
    assert utils.ceph_health_check(
        namespace, health_check_tries, health_check_delay
    )


def add_capacity(osd_size_capacity_requested):
    """
    Add storage capacity to the cluster

    Args:
        osd_size_capacity_requested(int): Requested osd size capacity

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
    osd_size_existing = get_osd_size()
    device_sets_required = int(osd_size_capacity_requested / osd_size_existing)
    old_storage_devices_sets_count = get_deviceset_count()
    new_storage_devices_sets_count = int(device_sets_required + old_storage_devices_sets_count)
    lvpresent = localstorage.check_local_volume()
    if lvpresent:
        ocp_obj = OCP(kind='localvolume', namespace=constants.LOCAL_STORAGE_NAMESPACE)
        localvolume_data = ocp_obj.get(resource_name='local-block')
        device_list = localvolume_data['spec']['storageClassDevices'][0]['devicePaths']
        final_device_list = localstorage.get_new_device_paths(device_sets_required, osd_size_capacity_requested)
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
            resource_name=lvcr.get()['items'][0]['metadata']['name'],
            params=param.strip('\n'),
            format_type='json'
        )
        localstorage.check_pvs_created(int(len(final_device_list) / new_storage_devices_sets_count))
    sc = get_storage_cluster()
    # adding the storage capacity to the cluster
    params = f"""[{{ "op": "replace", "path": "/spec/storageDeviceSets/0/count",
                "value": {new_storage_devices_sets_count}}}]"""
    sc.patch(
        resource_name=sc.get()['items'][0]['metadata']['name'],
        params=params.strip('\n'),
        format_type='json'
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
    Get osd count from Storage cluster

    Returns:
        int: osd count

    """
    sc = get_storage_cluster()
    return (
        int(sc.get().get('items')[0]['spec']['storageDeviceSets'][0]['count'])
        * int(sc.get().get('items')[0]['spec']['storageDeviceSets'][0]['replica'])
    )


def get_osd_size():
    """
    Get osd size from Storage cluster

    Returns:
        int: osd size

    """
    sc = get_storage_cluster()
    return int(
        sc.get().get('items')[0].get('spec').get('storageDeviceSets')[0].get(
            'dataPVCTemplate'
        ).get('spec').get('resources').get('requests').get('storage')[:-2]
    )


def get_deviceset_count():
    """
    Get storageDeviceSets count  from storagecluster

    Returns:
        int: storageDeviceSets count

    """
    sc = get_storage_cluster()
    return int(sc.get().get('items')[0].get('spec').get(
        'storageDeviceSets')[0].get('count')
    )


def get_all_storageclass():
    """
    Function for getting all storageclass excluding 'gp2' and 'flex'

    Returns:
         list: list of storageclass

    """
    sc_obj = ocp.OCP(
        kind=constants.STORAGECLASS,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    result = sc_obj.get()
    sample = result['items']

    storageclass = [
        item for item in sample if (
            item.get('metadata').get('name') not in (constants.IGNORE_SC_GP2, constants.IGNORE_SC_FLEX)
        )
    ]
    return storageclass


def change_noobaa_endpoints_count(nb_eps):
    """
    Scale up or down the number of maximum NooBaa emdpoints

    Args:
        nb_eps (int): The number of required Noobaa endpoints

    """
    log.info(f"Scaling up Noobaa endpoints to a maximum of {nb_eps}")
    params = f'{{"spec":{{"endpoints":{{"maxCount":{nb_eps},"minCount":1}}}}}}'
    noobaa = OCP(kind='noobaa', namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    noobaa.patch(resource_name='noobaa', params=params, format_type='merge')
