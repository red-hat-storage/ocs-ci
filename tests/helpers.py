"""
Helper functions file for OCS QE
"""
import logging
import re
import datetime

from ocs_ci.ocs.ocp import OCP

from uuid import uuid4
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.utility import templating
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def create_unique_resource_name(resource_description, resource_type):
    """
    Creates a unique object name by using the object_description,
    object_type and a random uuid(in hex) as suffix

    Args:
        resource_description (str): The user provided object description
        resource_type (str): The type of object for which the unique name
            will be created. For example: project, pvc, etc

    Returns:
        str: A unique name
    """
    return f"{resource_type}-{resource_description[:23]}-{uuid4().hex}"


def create_resource(do_reload=True, **kwargs):
    """
    Create a resource

    Args:
        do_reload (bool): True for reloading the resource following its creation,
            False otherwise
        kwargs (dict): Dictionary of the OCS resource

    Returns:
        OCS: An OCS instance

    Raises:
        AssertionError: In case of any failure
    """
    ocs_obj = OCS(**kwargs)
    resource_name = kwargs.get('metadata').get('name')
    created_resource = ocs_obj.create(do_reload=do_reload)
    assert created_resource, (
        f"Failed to create resource {resource_name}"
    )
    return ocs_obj


def wait_for_resource_state(resource, state, timeout=60):
    """
    Wait for a resource to get to a given status

    Args:
        resource (OCS obj): The resource object
        state (str): The status to wait for
        timeout (int): Time in seconds to wait

    Raises:
        ResourceWrongStatusException: In case the resource hasn't
            reached the desired state

    """
    try:
        resource.ocp.wait_for_resource(
            condition=state, resource_name=resource.name, timeout=timeout
        )
    except TimeoutExpiredError:
        logger.error(f"{resource.kind} {resource.name} failed to reach {state}")
        resource.reload()
        raise ResourceWrongStatusException(resource.name, resource.describe())
    logger.info(f"{resource.kind} {resource.name} reached state {state}")


def create_pod(
    interface_type=None, pvc_name=None,
    do_reload=True, namespace=defaults.ROOK_CLUSTER_NAMESPACE,
    node_name=None, pod_dict_path=None, sa_name=None, dc_deployment=False
):
    """
    Create a pod

    Args:
        interface_type (str): The interface type (CephFS, RBD, etc.)
        pvc_name (str): The PVC that should be attached to the newly created pod
        do_reload (bool): True for reloading the object after creation, False otherwise
        namespace (str): The namespace for the new resource creation
        node_name (str): The name of specific node to schedule the pod
        pod_dict_path (str): YAML path for the pod

    Returns:
        Pod: A Pod instance

    Raises:
        AssertionError: In case of any failure
    """
    if interface_type == constants.CEPHBLOCKPOOL:
        pod_dict = pod_dict_path if pod_dict_path else constants.CSI_RBD_POD_YAML
        interface = constants.RBD_INTERFACE
    else:
        pod_dict = pod_dict_path if pod_dict_path else constants.CSI_CEPHFS_POD_YAML
        interface = constants.CEPHFS_INTERFACE
    if dc_deployment:
        pod_dict = pod_dict_path if pod_dict_path else constants.FEDORA_DC_YAML
    pod_data = templating.load_yaml_to_dict(pod_dict)
    pod_name = create_unique_resource_name(
        f'test-{interface}', 'pod'
    )
    pod_data['metadata']['name'] = pod_name
    pod_data['metadata']['namespace'] = namespace
    if dc_deployment:
        pod_data['metadata']['labels']['app'] = pod_name
        pod_data['spec']['template']['metadata']['labels']['name'] = pod_name

    if pvc_name:
        if dc_deployment:
            pod_data['spec']['template']['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] = pvc_name
        else:
            pod_data['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] = pvc_name

    if node_name:
        pod_data['spec']['nodeName'] = node_name
    else:
        if 'nodeName' in pod_data.get('spec'):
            del pod_data['spec']['nodeName']
    if sa_name and dc_deployment:
        pod_data['spec']['template']['spec']['serviceAccountName'] = sa_name
    if dc_deployment:
        ocs_obj = create_resource(wait=False, **pod_data)
        logger.info(ocs_obj.name)
        assert (ocp.OCP(kind='pod', namespace=namespace)).wait_for_resource(
            condition=constants.STATUS_SUCCEEDED,
            resource_name=pod_name + '-1-deploy ',
            resource_count=0, timeout=180, sleep=3
        )
        dpod_list = pod.get_all_pods(namespace=namespace)
        for dpod in dpod_list:
            if '-1-deploy' not in dpod.name:
                if pod_name in dpod.name:
                    return dpod
    else:
        pod_obj = pod.Pod(**pod_data)
        pod_name = pod_data.get('metadata').get('name')
        created_resource = pod_obj.create(do_reload=do_reload)
        assert created_resource, (
            f"Failed to create resource {pod_name}"
        )


        return pod_obj


def create_project():
    """
    Create a project

    Returns:
        OCP: Project object

    """
    namespace = create_unique_resource_name('test', 'namespace')
    project_obj = ocp.OCP(kind='Project', namespace=namespace)
    assert project_obj.new_project(namespace), f"Failed to create namespace {namespace}"
    return project_obj


def create_secret(interface_type):
    """
    Create a secret

    Args:
        interface_type (str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)

    Returns:
        OCS: An OCS instance for the secret
    """
    secret_data = dict()
    if interface_type == constants.CEPHBLOCKPOOL:
        secret_data = templating.load_yaml_to_dict(
            constants.CSI_RBD_SECRET_YAML
        )
        secret_data['stringData']['userID'] = constants.ADMIN_USER
        secret_data['stringData']['userKey'] = get_admin_key()
        interface = constants.RBD_INTERFACE
    elif interface_type == constants.CEPHFILESYSTEM:
        secret_data = templating.load_yaml_to_dict(
            constants.CSI_CEPHFS_SECRET_YAML
        )
        del secret_data['stringData']['userID']
        del secret_data['stringData']['userKey']
        secret_data['stringData']['adminID'] = constants.ADMIN_USER
        secret_data['stringData']['adminKey'] = get_admin_key()
        interface = constants.CEPHFS_INTERFACE
    secret_data['metadata']['name'] = create_unique_resource_name(
        f'test-{interface}', 'secret'
    )
    secret_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE

    return create_resource(**secret_data)


def create_ceph_block_pool(pool_name=None):
    """
    Create a Ceph block pool

    Args:
        pool_name (str): The pool name to create

    Returns:
        OCS: An OCS instance for the Ceph block pool
    """
    cbp_data = templating.load_yaml_to_dict(constants.CEPHBLOCKPOOL_YAML)
    cbp_data['metadata']['name'] = (
        pool_name if pool_name else create_unique_resource_name(
            'test', 'cbp'
        )
    )
    cbp_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    cbp_obj = create_resource(**cbp_data)
    cbp_obj.reload()

    assert verify_block_pool_exists(cbp_obj.name), (
        f"Block pool {cbp_obj.name} does not exist"
    )
    return cbp_obj


def create_ceph_file_system(pool_name=None):
    """
    Create a Ceph file system

    Args:
        pool_name (str): The pool name to create

    Returns:
        OCS: An OCS instance for the Ceph file system
    """
    cfs_data = templating.load_yaml_to_dict(constants.CEPHFILESYSTEM_YAML)
    cfs_data['metadata']['name'] = (
        pool_name if pool_name else create_unique_resource_name(
            'test', 'cfs'
        )
    )
    cfs_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    cfs_data = create_resource(**cfs_data)
    cfs_data.reload()

    assert validate_cephfilesystem(cfs_data.name), (
        f"File system {cfs_data.name} does not exist"
    )
    return cfs_data


def create_storage_class(
    interface_type, interface_name, secret_name,
    reclaim_policy=constants.RECLAIM_POLICY_DELETE, sc_name=None,
    provisioner=None
):
    """
    Create a storage class

    Args:
        interface_type (str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)
        interface_name (str): The name of the interface
        secret_name (str): The name of the secret
        sc_name (str): The name of storage class to create
        reclaim_policy (str): Type of reclaim policy. Defaults to 'Delete'
            (eg., 'Delete', 'Retain')

    Returns:
        OCS: An OCS instance for the storage class
    """
    sc_data = dict()
    if interface_type == constants.CEPHBLOCKPOOL:
        sc_data = templating.load_yaml_to_dict(
            constants.CSI_RBD_STORAGECLASS_YAML
        )
        sc_data['parameters'][
            'csi.storage.k8s.io/node-stage-secret-name'
        ] = secret_name
        sc_data['parameters'][
            'csi.storage.k8s.io/node-stage-secret-namespace'
        ] = defaults.ROOK_CLUSTER_NAMESPACE
        interface = constants.RBD_INTERFACE
        sc_data['provisioner'] = (
            provisioner if provisioner else defaults.RBD_PROVISIONER
        )
    elif interface_type == constants.CEPHFILESYSTEM:
        sc_data = templating.load_yaml_to_dict(
            constants.CSI_CEPHFS_STORAGECLASS_YAML
        )
        sc_data['parameters'][
            'csi.storage.k8s.io/node-stage-secret-name'
        ] = secret_name
        sc_data['parameters'][
            'csi.storage.k8s.io/node-stage-secret-namespace'
        ] = defaults.ROOK_CLUSTER_NAMESPACE
        interface = constants.CEPHFS_INTERFACE
        sc_data['parameters']['fsName'] = get_cephfs_name()
        sc_data['provisioner'] = (
            provisioner if provisioner else defaults.CEPHFS_PROVISIONER
        )
    sc_data['parameters']['pool'] = interface_name

    sc_data['metadata']['name'] = (
        sc_name if sc_name else create_unique_resource_name(
            f'test-{interface}', 'storageclass'
        )
    )
    sc_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    sc_data['parameters'][
        'csi.storage.k8s.io/provisioner-secret-name'
    ] = secret_name
    sc_data['parameters'][
        'csi.storage.k8s.io/provisioner-secret-namespace'
    ] = defaults.ROOK_CLUSTER_NAMESPACE

    sc_data['parameters']['clusterID'] = defaults.ROOK_CLUSTER_NAMESPACE
    sc_data['reclaimPolicy'] = reclaim_policy

    try:
        del sc_data['parameters']['userid']
    except KeyError:
        pass
    return create_resource(**sc_data)


def create_pvc(
    sc_name, pvc_name=None, namespace=defaults.ROOK_CLUSTER_NAMESPACE,
    size=None, do_reload=True, access_mode=constants.ACCESS_MODE_RWO
):
    """
    Create a PVC

    Args:
        sc_name (str): The name of the storage class for the PVC to be
            associated with
        pvc_name (str): The name of the PVC to create
        namespace (str): The namespace for the PVC creation
        size(str): Size of pvc to create
        do_reload (bool): True for wait for reloading PVC after its creation, False otherwise
        access_mode (str): The access mode to be used for the PVC

    Returns:
        PVC: PVC instance
    """
    pvc_data = templating.load_yaml_to_dict(constants.CSI_PVC_YAML)
    pvc_data['metadata']['name'] = (
        pvc_name if pvc_name else create_unique_resource_name(
            'test', 'pvc'
        )
    )
    pvc_data['metadata']['namespace'] = namespace
    pvc_data['spec']['accessModes'] = [access_mode]
    pvc_data['spec']['storageClassName'] = sc_name
    if size:
        pvc_data['spec']['resources']['requests']['storage'] = size
    ocs_obj = pvc.PVC(**pvc_data)
    created_pvc = ocs_obj.create(do_reload=do_reload)
    assert created_pvc, f"Failed to create resource {pvc_name}"
    return ocs_obj


def create_multiple_pvcs(sc_name, namespace, number_of_pvc=1, size=None):
    """
    Create one or more PVC

    Args:
        sc_name (str): The name of the storage class to provision the PVCs from
        number_of_pvc (int): Number of PVCs to be created
        size (str): The size of the PVCs to create
        namespace (str): The namespace for the PVCs creation

    Returns:
         list: List of PVC objects
    """
    return [
        create_pvc(
            sc_name=sc_name, size=size, namespace=namespace
        ) for _ in range(number_of_pvc)
    ]


def verify_block_pool_exists(pool_name):
    """
    Verify if a Ceph block pool exist

    Args:
        pool_name (str): The name of the Ceph block pool

    Returns:
        bool: True if the Ceph block pool exists, False otherwise
    """
    logger.info(f"Verifying that block pool {pool_name} exists")
    ct_pod = pod.get_ceph_tools_pod()
    try:
        for pools in TimeoutSampler(
            60, 3, ct_pod.exec_ceph_cmd, 'ceph osd lspools'
        ):
            logger.info(f'POOLS are {pools}')
            for pool in pools:
                if pool_name in pool.get('poolname'):
                    return True
    except TimeoutExpiredError:
        return False


def get_admin_key():
    """
    Fetches admin key secret from Ceph

    Returns:
        str: The admin key
    """
    ct_pod = pod.get_ceph_tools_pod()
    out = ct_pod.exec_ceph_cmd('ceph auth get-key client.admin')
    return out['key']


def get_cephfs_data_pool_name():
    """
    Fetches ceph fs datapool name from Ceph

    Returns:
        str: fs datapool name
    """
    ct_pod = pod.get_ceph_tools_pod()
    out = ct_pod.exec_ceph_cmd('ceph fs ls')
    return out[0]['data_pools'][0]


def validate_cephfilesystem(fs_name):
    """
     Verify CephFileSystem exists at Ceph and OCP

     Args:
        fs_name (str): The name of the Ceph FileSystem

     Returns:
         bool: True if CephFileSystem is created at Ceph and OCP side else
            will return False with valid msg i.e Failure cause
    """
    cfs = ocp.OCP(
        kind=constants.CEPHFILESYSTEM,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    ct_pod = pod.get_ceph_tools_pod()
    ceph_validate = False
    ocp_validate = False

    result = cfs.get(resource_name=fs_name)
    if result.get('metadata').get('name'):
        logger.info("Filesystem %s got created from Openshift Side", fs_name)
        ocp_validate = True
    else:
        logger.info(
            "Filesystem %s was not create at Openshift Side", fs_name
        )
        return False

    try:
        for pools in TimeoutSampler(
            60, 3, ct_pod.exec_ceph_cmd, 'ceph fs ls'
        ):
            for out in pools:
                result = out.get('name')
                if result == fs_name:
                    logger.info("FileSystem %s got created from Ceph Side", fs_name)
                    ceph_validate = True
                    break
                else:
                    logger.error("FileSystem %s was not present at Ceph Side", fs_name)
                    ceph_validate = False
            if ceph_validate:
                break
    except TimeoutExpiredError:
        pass

    return True if (ceph_validate and ocp_validate) else False


def get_all_storageclass_names():
    """
    Function for getting all storageclass

    Returns:
         list: list of storageclass name
    """
    sc_obj = ocp.OCP(
        kind=constants.STORAGECLASS,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    result = sc_obj.get()
    sample = result['items']

    storageclass = [
        item.get('metadata').get('name') for item in sample if (
            (item.get('metadata').get('name') not in constants.IGNORE_SC_GP2)
            and (item.get('metadata').get('name') not in constants.IGNORE_SC_FLEX)
        )
    ]
    return storageclass


def delete_storageclasses(sc_objs):
    """"
    Function for Deleting storageclasses

    Args:
        sc_objs (list): List of SC objects for deletion

    Returns:
        bool: True if deletion is successful
    """

    for sc in sc_objs:
        logger.info("Deleting StorageClass with name %s", sc.name)
        sc.delete()
    return True


def get_cephblockpool_names():
    """
    Function for getting all CephBlockPool

    Returns:
         list: list of cephblockpool name
    """
    pool_obj = ocp.OCP(
        kind=constants.CEPHBLOCKPOOL,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    result = pool_obj.get()
    sample = result['items']
    pool_list = [
        item.get('metadata').get('name') for item in sample
    ]
    return pool_list


def delete_cephblockpools(cbp_objs):
    """
    Function for deleting CephBlockPool

    Args:
        cbp_objs (list): List of CBP objects for deletion

    Returns:
        bool: True if deletion of CephBlockPool is successful
    """
    for cbp in cbp_objs:
        logger.info("Deleting CephBlockPool with name %s", cbp.name)
        cbp.delete()
    return True


def get_cephfs_name():
    """
    Function to retrive CephFS name
    Returns:
        str: Name of CFS
    """
    cfs_obj = ocp.OCP(
        kind=constants.CEPHFILESYSTEM,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    result = cfs_obj.get()
    return result['items'][0].get('metadata').get('name')


def run_io_with_rados_bench(**kw):
    """ A task for radosbench

        Runs radosbench command on specified pod . If parameters are
        not provided task assumes few default parameters.This task
        runs command in synchronous fashion.


        Args:
            **kw: Needs a dictionary of various radosbench parameters.
                ex: pool_name:pool
                    pg_num:number of pgs for pool
                    op: type of operation {read, write}
                    cleanup: True OR False


        Returns:
            ret: return value of radosbench command
    """

    logger.info("Running radosbench task")
    ceph_pods = kw.get('ceph_pods')  # list of pod objects of ceph cluster
    config = kw.get('config')

    role = config.get('role', 'client')
    clients = [cpod for cpod in ceph_pods if role in cpod.roles]

    idx = config.get('idx', 0)
    client = clients[idx]
    op = config.get('op', 'write')
    cleanup = ['--no-cleanup', '--cleanup'][config.get('cleanup', True)]
    pool = config.get('pool')

    block = str(config.get('size', 4 << 20))
    time = config.get('time', 120)
    time = str(time)

    rados_bench = (
        f"rados --no-log-to-stderr "
        f"-b {block} "
        f"-p {pool} "
        f"bench "
        f"{time} "
        f"{op} "
        f"{cleanup} "
    )
    try:
        ret = client.exec_ceph_cmd(ceph_cmd=rados_bench)
    except CommandFailed as ex:
        logger.error(f"Rados bench failed\n Error is: {ex}")
        return False

    logger.info(ret)
    logger.info("Finished radosbench")
    return ret


def get_all_pvs():
    """
    Gets all pv in openshift-storage namespace

    Returns:
         dict: Dict of all pv in openshift-storage namespace
    """
    ocp_pv_obj = ocp.OCP(
        kind=constants.PV, namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    return ocp_pv_obj.get()


@retry(AssertionError, tries=10, delay=5, backoff=1)
def validate_pv_delete(pv_name):
    """
    validates if pv is deleted after pvc deletion

    Args:
        pv_name (str): pv from pvc to validates
    Returns:
        bool: True if deletion is successful

    Raises:
        AssertionError: If pv is not deleted
    """
    ocp_pv_obj = ocp.OCP(
        kind=constants.PV, namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )

    try:
        if ocp_pv_obj.get(resource_name=pv_name):
            raise AssertionError

    except CommandFailed:
        return True


def create_pods(pvc_objs_list, interface_type=None, namespace=None):
    """
    Create Pods.
    A pod will be created for each PVC in 'pvc_objs_list'.

    Args:
        pvc_objs_list (list): List of PVC objects
        interface_type (str): The interface type (CephFS, Cephblockpool, etc.)
        namespace(str): Name of the namespace

    Returns:
        list: List of Pod objects

    """
    pod_objs = [
        create_pod(
            interface_type=interface_type, pvc_name=pvc_obj.name,
            do_reload=False, namespace=namespace
        ) for pvc_obj in pvc_objs_list
    ]

    return pod_objs


def get_worker_nodes():
    """
    Fetches all worker nodes.

    Returns:
        list: List of names of worker nodes
    """
    label = 'node-role.kubernetes.io/worker'
    ocp_node_obj = ocp.OCP(kind=constants.NODE)
    nodes = ocp_node_obj.get(selector=label).get('items')
    worker_nodes_list = [node.get('metadata').get('name') for node in nodes]
    return worker_nodes_list


def get_start_creation_time(interface, pvc_name):
    """
    Get the starting creation time of a PVC based on provisioner logs

    Args:
        interface (str): The interface backed the PVC
        pvc_name (str): Name of the PVC for creation time measurement

    Returns:
        datetime object: Start time of PVC creation

    """
    format = '%H:%M:%S.%f'
    # Get the correct provisioner pod based on the interface
    if interface == constants.CEPHBLOCKPOOL:
        pod_name = pod.get_rbd_provisioner_pod().name
    else:
        pod_name = pod.get_cephfs_provisioner_pod().name

    # get the logs from the csi-provisioner container
    logs = pod.get_pod_logs(pod_name, 'csi-provisioner')
    logs = logs.split("\n")
    # Extract the starting time for the PVC provisioning
    start = [
        i for i in logs if re.search(f"provision.*{pvc_name}.*started", i)
    ][0].split(' ')[1]
    return datetime.datetime.strptime(start, format)


def get_end_creation_time(interface, pvc_name):
    """
    Get the ending creation time of a PVC based on provisioner logs

    Args:
        interface (str): The interface backed the PVC
        pvc_name (str): Name of the PVC for creation time measurement

    Returns:
        datetime object: End time of PVC creation

    """
    format = '%H:%M:%S.%f'
    # Get the correct provisioner pod based on the interface
    if interface == constants.CEPHBLOCKPOOL:
        pod_name = pod.get_rbd_provisioner_pod().name
    else:
        pod_name = pod.get_cephfs_provisioner_pod().name

    # get the logs from the csi-provisioner container
    logs = pod.get_pod_logs(pod_name, 'csi-provisioner')
    logs = logs.split("\n")
    # Extract the starting time for the PVC provisioning
    end = [
        i for i in logs if re.search(f"provision.*{pvc_name}.*succeeded", i)
    ][0].split(' ')[1]
    return datetime.datetime.strptime(end, format)


def measure_pvc_creation_time(interface, pvc_name):
    """
    Measure PVC creation time based on logs

    Args:
        interface (str): The interface backed the PVC
        pvc_name (str): Name of the PVC for creation time measurement

    Returns:
        float: Creation time for the PVC

    """
    start = get_start_creation_time(interface=interface, pvc_name=pvc_name)
    end = get_end_creation_time(interface=interface, pvc_name=pvc_name)
    total = end - start
    return total.total_seconds()


def verify_volume_deleted_in_backend(interface, image_uuid, pool_name=None):
    """
    Verify that Image/Subvolume is not present in the backend.

    Args:
        interface (str): The interface backed the PVC
        image_uuid (str): Part of VolID which represents
            corresponding image/subvolume in backend
            eg: oc get pv/<volumeName> -o jsonpath='{.spec.csi.volumeHandle}'
                Output is the CSI generated VolID and looks like:
                '0001-000c-rook-cluster-0000000000000001-
                f301898c-a192-11e9-852a-1eeeb6975c91' where
                image_uuid is 'f301898c-a192-11e9-852a-1eeeb6975c91'
        pool_name (str): Name of the rbd-pool if interface is CephBlockPool

    Returns:
        bool: True if volume is not present. False if volume is present
    """
    ct_pod = pod.get_ceph_tools_pod()
    if interface == constants.CEPHBLOCKPOOL:
        valid_error = f"error opening image csi-vol-{image_uuid}"
        cmd = f"rbd info -p {pool_name} csi-vol-{image_uuid}"
    if interface == constants.CEPHFILESYSTEM:
        valid_error = f"Subvolume 'csi-vol-{image_uuid}' not found"
        cmd = (
            f"ceph fs subvolume getpath {defaults.CEPHFILESYSTEM_NAME}"
            f" csi-vol-{image_uuid} csi"
        )

    try:
        ct_pod.exec_ceph_cmd(ceph_cmd=cmd, format='json')
        return False
    except CommandFailed as ecf:
        assert valid_error in str(ecf), (
            f"Error occurred while verifying volume is deleted in backend: "
            f"{str(ecf)} ImageUUID: {image_uuid}. Interface type: {interface}"
        )
    logger.info(
        f"Verified: Volume corresponding to uuid {image_uuid} is deleted "
        f"in backend"
    )
    return True


def create_serviceaccount(namespace):
    """
    Create a Serviceaccount

    Args:
        namespace (str): The namespace for the serviceaccount creation

    Returns:
        OCS: An OCS instance for the service_account
    """

    service_account_data = templating.load_yaml_to_dict(
        constants.SERVICE_ACCOUNT_YAML
    )
    service_account_data['metadata']['name'] = create_unique_resource_name(
        'sa', 'serviceaccount'
    )
    service_account_data['metadata']['namespace'] = namespace

    return create_resource(**service_account_data, wait=False)


def add_scc_policy(sa_name, namespace):
    """
    Adding ServiceAccount to scc privilaged

    Args:
        sa_name (str): ServiceAccount name
        namespace (str): The namespace for the scc_policy creation

    """
    ocp = OCP()
    out = ocp.exec_oc_cmd(
        command=f"adm policy add-scc-to-user privileged system:serviceaccount:{namespace}:{sa_name}",
        out_yaml_format=False
    )

    logger.info(out)


def remove_scc_policy(sa_name, namespace):
    """
     Removing ServiceAccount from scc privilaged

    Args:
        sa_name (str): ServiceAccount name
        namespace (str): The namespace for the scc_policy deletion

    """
    ocp = OCP()
    out = ocp.exec_oc_cmd(
        command=f"adm policy remove-scc-from-user privileged system:serviceaccount:{namespace}:{sa_name}",
        out_yaml_format=False
    )

    logger.info(out)


def delete_deploymentconfig(pod_obj):
    """
    Delete deploymentconfig

    Args:
         pod_obj (object): Pod object
    """
    dc_ocp_obj = ocp.OCP(kind=constants.DEPLOYMENTCONFIG)
    dc_ocp_obj.delete(resource_name=pod_obj.get_labels().get('name'))
<<<<<<< HEAD
=======


def measure_pvc_creation_time(interface, pvc_name):
    """
    Measure PVC creation time based on logs

    Args:
        interface (str): The interface backed the PVC
        pvc_name (str): Name of the PVC for creation time measurement

    Returns:
        float: Creation time for the PVC

    """
    format = '%H:%M:%S.%f'
    # Get the correct provisioner pod based on the interface
    if interface == constants.CEPHBLOCKPOOL:
        pod_name = pod.get_rbd_provisioner_pod().name
    else:
        pod_name = pod.get_cephfs_provisioner_pod().name

    # get the logs from the csi-provisioner container
    logs = pod.get_pod_logs(pod_name, 'csi-provisioner')
    logs = logs.split("\n")
    # Extract the starting time for the PVC provisioning
    start = [
        i for i in logs if re.search(f"provision.*{pvc_name}.*started", i)
    ][0].split(' ')[1]
    # Extract the end time for the PVC provisioning
    end = [
        i for i in logs if re.search(f"provision.*{pvc_name}.*succeeded", i)
    ][0].split(' ')[1]
    total = (
        datetime.datetime.strptime(end, format) - datetime.datetime.strptime(
            start, format
        )
    )
    return total.total_seconds()
>>>>>>> fixed travis failure
