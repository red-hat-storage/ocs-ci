"""
StorageCluster related functionalities
"""
import logging
import re

from jsonschema import validate
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.exceptions import ResourceNotFoundError
from ocs_ci.ocs.node import get_compute_node_names
from ocs_ci.ocs.ocp import get_images, OCP
from ocs_ci.ocs.resources.ocs import get_ocs_csv
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.utility import localstorage, utils
from ocs_ci.ocs.node import get_osd_running_nodes
from ocs_ci.ocs.exceptions import UnsupportedFeatureError
from ocs_ci.utility.utils import run_cmd
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
    post_upgrade_verification=False, version_before_upgrade=None
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
    from ocs_ci.ocs.node import get_typed_nodes
    from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs
    from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_all_pods
    from ocs_ci.ocs.cluster import validate_cluster_on_pvc
    from ocs_ci.ocs.resources.fips import check_fips_enabled
    number_of_worker_nodes = len(get_typed_nodes())
    namespace = config.ENV_DATA['cluster_namespace']
    log.info("Verifying OCS installation")

    # Verify OCS CSV is in Succeeded phase
    log.info("verifying ocs csv")
    ocs_csv = get_ocs_csv()
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
    if not config.DEPLOYMENT['external_mode']:
        osd_count = (
            int(storage_cluster.data['spec']['storageDeviceSets'][0]['count'])
            * int(storage_cluster.data['spec']['storageDeviceSets'][0]['replica'])
        )
    rgw_count = None
    if config.ENV_DATA.get('platform') in constants.ON_PREM_PLATFORMS:
        #  RGW count is 1 if OCS version < 4.5 or the cluster was upgraded from version <= 4.4
        if float(config.ENV_DATA['ocs_version']) < 4.5 or float(
            config.ENV_DATA['ocs_version']
        ) == 4.5 and (post_upgrade_verification and float(version_before_upgrade) < 4.5):
            rgw_count = 1
        else:
            rgw_count = 2

    # # With 4.4 OCS cluster deployed over Azure, RGW is the default backingstore
    if config.ENV_DATA.get('platform') == constants.AZURE_PLATFORM:
        if float(config.ENV_DATA['ocs_version']) == 4.4 or (
            float(config.ENV_DATA['ocs_version']) == 4.5 and (
                post_upgrade_verification and float(version_before_upgrade) < 4.5
            )
        ):
            rgw_count = 1

    min_eps = constants.MIN_NB_ENDPOINT_COUNT_POST_DEPLOYMENT
    max_eps = constants.MAX_NB_ENDPOINT_COUNT if float(config.ENV_DATA['ocs_version']) >= 4.6 else 1

    if config.ENV_DATA.get('platform') == constants.IBM_POWER_PLATFORM:
        min_eps = 1
        max_eps = 1

    resources_dict = {
        constants.OCS_OPERATOR_LABEL: 1,
        constants.OPERATOR_LABEL: 1,
        constants.NOOBAA_DB_LABEL: 1,
        constants.NOOBAA_OPERATOR_POD_LABEL: 1,
        constants.NOOBAA_CORE_POD_LABEL: 1,
        constants.NOOBAA_ENDPOINT_POD_LABEL: min_eps
    }
    if not config.DEPLOYMENT['external_mode']:
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
                constants.RGW_APP_LABEL: rgw_count
            }
        )

    for label, count in resources_dict.items():
        if label == constants.RGW_APP_LABEL:
            if not config.ENV_DATA.get('platform') in constants.ON_PREM_PLATFORMS:
                continue
        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=label,
            resource_count=count,
            timeout=timeout
        )

    nb_ep_pods = get_pods_having_label(
        label=constants.NOOBAA_ENDPOINT_POD_LABEL, namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    assert len(nb_ep_pods) <= max_eps, (
        f"The number of running NooBaa endpoint pods ({len(nb_ep_pods)}) "
        f"is greater than the maximum defined in the NooBaa CR ({max_eps})"
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
    if config.DEPLOYMENT['external_mode']:
        required_storage_classes.update(
            {
                f'{storage_cluster_name}-ceph-rgw',
                f'{config.ENV_DATA["cluster_namespace"]}.noobaa.io'
            }
        )
    storage_classes = storage_class.get()
    storage_class_names = {
        item['metadata']['name'] for item in storage_classes['items']
    }
    assert required_storage_classes.issubset(storage_class_names)

    # Verify OSDs are distributed
    if not config.DEPLOYMENT['external_mode']:
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
    csi_drivers = (
        {item['metadata']['name'] for item in csi_driver.get()['items']}
    )
    assert defaults.CSI_PROVISIONERS.issubset(csi_drivers)

    # Verify node and provisioner secret names in storage class
    log.info("Verifying node and provisioner secret names in storage class.")
    if config.DEPLOYMENT['external_mode']:
        sc_rbd = storage_class.get(
            resource_name=constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
        )
        sc_cephfs = storage_class.get(
            resource_name=(
                constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
            )
        )
    else:
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
    if not config.DEPLOYMENT['external_mode']:
        log.info(
            "Verifying ceph osd tree output and checking for device set PVC names "
            "in the output."
        )

        if (config.DEPLOYMENT.get('local_storage')):
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
    # if the OCS version is < 4.6
    if float(config.ENV_DATA['ocs_version']) < 4.6:
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
    if config.ENV_DATA.get('fips'):
        # In case that fips is enabled when deploying,
        # a verification of the installation of it will run
        # on all running state pods
        check_fips_enabled()
    if config.ENV_DATA.get("encryption_at_rest"):
        osd_encryption_verification()


def osd_encryption_verification():
    """
    Verify if OSD encryption at rest if successfully deployed on OCS

    Raises:
        UnsupportedFeatureError: OCS version is smaller than 4.6
        EnvironmentError: The OSD is not encrypted
    """
    ocs_version = float(config.ENV_DATA['ocs_version'])
    if ocs_version < 4.6:
        error_message = "Encryption at REST can be enabled only on OCS >= 4.6!"
        raise UnsupportedFeatureError(error_message)

    osd_node_names = get_osd_running_nodes()
    osd_size = get_osd_size()
    lsblk_output_list = []
    for worker_node in osd_node_names:
        lsblk_cmd = 'oc debug node/' + worker_node + ' -- chroot /host lsblk'
        out = run_cmd(lsblk_cmd)
        log.info(f"the output from lsblk command is {out}")
        lsblk_output_list.append(out)

    for node_output_lsblk in lsblk_output_list:
        node_lsb = node_output_lsblk.split()
        # Search 'crypt' in node_lsb list
        if 'crypt' not in node_lsb:
            raise EnvironmentError('OSD is not encrypted')
        index_crypt = node_lsb.index('crypt')
        encrypted_component_size = int(
            (re.findall(r'\d+', node_lsb[index_crypt - 2]))[0]
        )
        # Verify that OSD is encrypted, and not another component like sda
        if encrypted_component_size != osd_size:
            raise EnvironmentError(
                'The OSD is not encrypted, another mount encrypted.'
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
        ocp_obj = OCP(kind='localvolume', namespace=config.ENV_DATA['local_storage_namespace'])
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


def change_noobaa_endpoints_count(min_nb_eps=None, max_nb_eps=None):
    """
    Scale up or down the number of maximum NooBaa emdpoints

    Args:
        min_nb_eps (int): The number of required minimum Noobaa endpoints
        max_nb_eps (int): The number of required maximum Noobaa endpoints

    """
    noobaa = OCP(kind='noobaa', namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    if min_nb_eps:
        log.info(f"Changing minimum Noobaa endpoints to {min_nb_eps}")
        params = f'{{"spec":{{"endpoints":{{"minCount":{min_nb_eps}}}}}}}'
        noobaa.patch(resource_name='noobaa', params=params, format_type='merge')
    if max_nb_eps:
        log.info(f"Changing maximum Noobaa endpoints to {max_nb_eps}")
        params = f'{{"spec":{{"endpoints":{{"maxCount":{max_nb_eps}}}}}}}'
        noobaa.patch(resource_name='noobaa', params=params, format_type='merge')
    noobaa.patch(resource_name='noobaa', params=params, format_type='merge')


def check_rebalance_occur_after_expand(old_osds, exp_time=600):

    """
    The function check data balance is initiating after add capacity to cluster. The function need 2 out 3 rounds
    the old osd that had data will be decreased and new osd data will be increased.
    Args:
        old_osds: list of old [osd.0, osd.1...]
        exp_time (int): Time to check osds utilization

    """
    from time import sleep, time
    from ocs_ci.ocs.exceptions import ClusterUtilizationNotBalanced
    from ocs_ci.ocs.cluster import get_osd_utilization

    general_num_of_success = 0
    general_num_of_fails = 0
    attempt = 0
    begin_time = time()
    while general_num_of_success < 2:
        num_of_fails_old_osd_increase = 0
        num_of_fails_new_osd_decrease = 0
        attempt += 1
        log.info(f"Try number {attempt} out of to check utilization")
        first_utilization = get_osd_utilization()
        log.info(f"The first utilization is {first_utilization}")
        log.info("Waiting for 30 seconds to get second utilization")
        sleep(30)
        second_utilization = get_osd_utilization()
        log.info(f"The second utilization is {second_utilization}")
        sum_of_percentage_old_osd_first_utilization = 0
        sum_of_percentage_old_osd_second_utilization = 0
        sum_of_percentage_new_osd_first_utilization = 0
        sum_of_percentage_new_osd_second_utilization = 0
        for osd_name, osd_util in second_utilization.items():
            old_osd_flag = 0
            for old_osd in old_osds:
                if old_osd == osd_name:
                    old_osd_flag = 1
                    sum_of_percentage_old_osd_first_utilization += first_utilization[osd_name]
                    sum_of_percentage_old_osd_second_utilization += second_utilization[osd_name]
            if old_osd_flag == 0:
                sum_of_percentage_new_osd_first_utilization += first_utilization[osd_name]
                sum_of_percentage_new_osd_second_utilization += second_utilization[osd_name]
        if sum_of_percentage_old_osd_first_utilization < sum_of_percentage_old_osd_second_utilization:
            log.info(f"sum of old osd utilization is not decreasing"
                     f" first util sum is: {sum_of_percentage_old_osd_first_utilization}"
                     f" and second util sum is "
                     f"{sum_of_percentage_old_osd_second_utilization}")
            num_of_fails_old_osd_increase += 1
        else:
            log.info(f"sum of old osd is decreasing from {sum_of_percentage_old_osd_first_utilization} "
                     f"to {sum_of_percentage_old_osd_second_utilization}")
        if sum_of_percentage_new_osd_first_utilization > sum_of_percentage_new_osd_second_utilization:
            log.info(f"sum of new osd utilization is not increasing"
                     f" first util sum is: {sum_of_percentage_new_osd_first_utilization}"
                     f" and second util sum is "
                     f"{sum_of_percentage_new_osd_second_utilization}")
            num_of_fails_new_osd_decrease += 1
        else:
            log.info(f"sum of new osd is increasing from {sum_of_percentage_new_osd_first_utilization} "
                     f"to {sum_of_percentage_new_osd_second_utilization}")
        if num_of_fails_old_osd_increase == 1 or num_of_fails_new_osd_decrease == 1:
            log.error("Cluster failed to balance this attempt")
            general_num_of_fails += 1
        if general_num_of_fails == 2:
            log.error("Cluster failed to balance 2 times, resetting success counters")
            general_num_of_fails = 0
            general_num_of_success = 0
        if num_of_fails_new_osd_decrease + num_of_fails_old_osd_increase == 0:
            general_num_of_success += 1
        curr_time = time()
        if curr_time - begin_time > exp_time and general_num_of_success < 2:
            log.error(f"Osds failed to reach desired state in {exp_time} - failing")
            raise ClusterUtilizationNotBalanced(f"Osds failed to reach desired state in {exp_time} - failing")
        if general_num_of_success == 2:
            log.info(f"OSDs utilization has been balancing for 2 time  within {exp_time} - continuing")


def check_until_osd_ratio_start_decrease_or_equal(osds, digit_point=None, ratio_stable=None, exp_time=300):
    """
    The function gets a list of OSDs and check if the sum of utilization percentage is decreasing or stay equal
    depends on what was requested. It checks for 3 times in a row and tolerates one fail out of 3.
    Args:
        osds (list): List of osds ie [osd.1, osd.2...]
        digit_point (int): How many digit after decimal point to check. It was created because monitoring data
        is always added so to cancel the increase of monitoring data you can choose like 2
        ratio_stable(anything): If not none it will check that in time of 3 rounds the osd is not increasing and
        can tolerate 1 fail out of 3
        exp_time (int): How much time to check the utilization change before giving up

    """
    from time import sleep, time
    from ocs_ci.ocs.cluster import get_osd_utilization
    from ocs_ci.ocs.exceptions import OsdIsIncreasing
    num_of_fails = 0
    num_of_success = 0
    begin_time = time()
    while num_of_success < 2:
        osd_first_utilization = get_osd_utilization()
        log.info(f"First utilization is {osd_first_utilization}, waiting for 60 seconds to check next utilizaztion"
                 f" for comparision")
        sleep(60)
        osd_second_utilization = get_osd_utilization()
        log.info(f"Second utilization is {osd_second_utilization}")
        sum_of_osd_first_util = 0
        sum_of_osd_second_util = 0
        for osd in osds:
            if digit_point is not None:
                osd_first_utilization[osd] = float(format(osd_first_utilization[osd], f".{digit_point}f"))
                osd_second_utilization[osd] = float(format(osd_second_utilization[osd], f".{digit_point}f"))
            sum_of_osd_first_util += osd_first_utilization[osd]
            sum_of_osd_second_util += osd_second_utilization[osd]

        if ratio_stable is None:
            expression_action = 'decreased'
            if sum_of_osd_first_util < sum_of_osd_second_util:
                log.info("Sum of osd ration is still increasing from "
                         f"{sum_of_osd_first_util} to {sum_of_osd_second_util}")
                num_of_fails += 1
            else:
                log.info(f"sum of utilization is decreasing from {sum_of_osd_first_util} to {sum_of_osd_second_util}")
                num_of_success += 1
        else:
            expression_action = 'equal'
            if sum_of_osd_first_util != sum_of_osd_second_util:
                log.info(f"Sum of osd ration is still not equal: sum of first util "
                         f"{sum_of_osd_first_util} not equal to {sum_of_osd_second_util}")
                num_of_fails += 1
            else:
                log.info(f"Sum of utilization is equal: first util is {sum_of_osd_first_util} and second "
                         f"util is {sum_of_osd_second_util}")
                num_of_success += 1
        if num_of_fails == 1:
            num_of_success = 0
            num_of_fails = 0
        curr_time = time()
        time_diff = curr_time - begin_time
        if time_diff > exp_time and num_of_success < 2:
            log.error(f"Sum of osds has not {expression_action} 2 times within"
                      f" {exp_time} seconds, back to test")
            raise OsdIsIncreasing(f"Sum of osds has not {expression_action} 2 times within"
                                  f" {exp_time} seconds, failing")
