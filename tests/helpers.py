"""
Helper functions file for OCS QE
"""
import datetime
import logging

from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.utility import templating
from ocs_ci.framework import config
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.exceptions import CommandFailed

from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


def create_unique_resource_name(resource_description, resource_type):
    """
    Creates a unique object name by using the object_description
    and object_type, as well as the current date/time string.

    Args:
        resource_description (str): The user provided object description
        resource_type (str): The type of object for which the unique name
            will be created. For example: project, pvc, etc

    Returns:
        str: A unique name
    """
    current_date_time = (
        datetime.datetime.now().strftime("%d%H%M%S%f")
    )
    return f"{resource_type}-{resource_description[:23]}-{current_date_time[:10]}"


def create_resource(
    desired_status=constants.STATUS_AVAILABLE, wait=True, **kwargs
):
    """
    Create a resource

    Args:
        desired_status (str): The status of the resource to wait for
        wait (bool): True for waiting for the resource to reach the desired
            status, False otherwise
        kwargs (dict): Dictionary of the OCS resource

    Returns:
        OCS: An OCS instance

    Raises:
        AssertionError: In case of any failure
    """
    ocs_obj = OCS(**kwargs)
    resource_name = kwargs.get('metadata').get('name')
    created_resource = ocs_obj.create(do_reload=wait)
    assert created_resource, (
        f"Failed to create resource {resource_name}"
    )
    if wait:
        assert wait_for_resource_state(
            resource=resource_name, state=desired_status)
    return ocs_obj


def wait_for_resource_state(resource, state):
    """
    Wait for a resource to get to a given status

    Args:
        resource (OCS obj): The resource object
        state (str): The status to wait for

    Returns:
        bool: True if resource reached the desired state, False otherwise
    """
    try:
        resource.ocp.wait_for_resource(
            condition=state, resource_name=resource.name
        )
    except TimeoutExpiredError:
        logger.info(f"{resource.kind} {resource.name} failed to reach {state}")
        return False
    logger.info(f"{resource.kind} {resource.name} reached state {state}")
    return True


def create_pod(
    interface_type=None, pvc_name=None, desired_status=constants.STATUS_RUNNING, wait=True,
    namespace=defaults.ROOK_CLUSTER_NAMESPACE
):
    """
    Create a pod

    Args:
        interface_type (str): The interface type (CephFS, RBD, etc.)
        pvc_name (str): The PVC that should be attached to the newly created pod
        desired_status (str): The status of the pod to wait for
        wait (bool): True for waiting for the pod to reach the desired
            status, False otherwise
        namespace (str): The namespace for the new resource creation

    Returns:
        Pod: A Pod instance

    Raises:
        AssertionError: In case of any failure
    """
    if interface_type == constants.CEPHBLOCKPOOL:
        pod_dict = constants.CSI_RBD_POD_YAML
        interface = constants.RBD_INTERFACE
    else:
        pod_dict = constants.CSI_CEPHFS_POD_YAML
        interface = constants.CEPHFS_INTERFACE

    pod_data = templating.load_yaml_to_dict(pod_dict)
    pod_data['metadata']['name'] = create_unique_resource_name(
        f'test-{interface}', 'pod'
    )
    pod_data['metadata']['namespace'] = namespace
    if pvc_name:
        pod_data['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] = pvc_name
    pod_obj = pod.Pod(**pod_data)
    pod_name = pod_data.get('metadata').get('name')
    created_resource = pod_obj.create(do_reload=wait)
    assert created_resource, (
        f"Failed to create resource {pod_name}"
    )
    if wait:
        assert wait_for_resource_state(pod_obj, desired_status)

    return pod_obj


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

    return create_resource(**secret_data, wait=False)


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
    cbp_obj = create_resource(**cbp_data, wait=False)

    assert verify_block_pool_exists(cbp_obj.name), (
        f"Block pool {cbp_obj.name} does not exist"
    )
    return cbp_obj


def create_storage_class(
    interface_type, interface_name, secret_name,
    reclaim_policy=constants.RECLAIM_POLICY_DELETE, sc_name=None
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
            'csi.storage.k8s.io/node-publish-secret-name'
        ] = secret_name
        sc_data['parameters'][
            'csi.storage.k8s.io/node-publish-secret-namespace'
        ] = defaults.ROOK_CLUSTER_NAMESPACE
        interface = constants.RBD_INTERFACE
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
    return create_resource(**sc_data, wait=False)


def create_pvc(
    sc_name, pvc_name=None, namespace=defaults.ROOK_CLUSTER_NAMESPACE,
    size=None, wait=True
):
    """
    Create a PVC

    Args:
        sc_name (str): The name of the storage class for the PVC to be
            associated with
        pvc_name (str): The name of the PVC to create
        namespace (str): The namespace for the PVC creation
        size(str): Size of pvc to create
        wait (bool): True for wait for the VPC operation to complete, False otherwise

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
    pvc_data['spec']['storageClassName'] = sc_name
    if size:
        pvc_data['spec']['resources']['requests']['storage'] = size
    ocs_obj = pvc.PVC(**pvc_data)
    created_pvc = ocs_obj.create(do_reload=wait)
    assert created_pvc, f"Failed to create resource {pvc_name}"
    if wait:
        assert wait_for_resource_state(ocs_obj, constants.STATUS_BOUND)
        ocs_obj.reload()

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

    pvc_objs = [
        create_pvc(
            sc_name=sc_name, size=size, namespace=namespace, wait=False
        ) for _ in range(number_of_pvc)
    ]
    for pvc in pvc_objs:
        assert wait_for_resource_state(pvc, constants.STATUS_BOUND), (
            f"PVC {pvc.name} failed to reach {constants.STATUS_BOUND} status"
        )
    return pvc_objs


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
    pools = ct_pod.exec_ceph_cmd('ceph osd lspools')
    logger.info(f'POOLS are {pools}')
    for pool in pools:
        if pool_name in pool.get('poolname'):
            return True
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
     Verify CephFileSystem exists at ceph and k8s

     Args:
        fs_name (str): The name of the Ceph FileSystem

     Returns:
         bool: True if CephFileSystem is created at ceph and k8s side else
            will return False with valid msg i.e Failure cause
    """
    CFS = ocp.OCP(
        kind=constants.CEPHFILESYSTEM,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    ct_pod = pod.get_ceph_tools_pod()
    ceph_validate = False
    k8s_validate = False
    cmd = "ceph fs ls"
    logger.info(fs_name)
    out = ct_pod.exec_ceph_cmd(ceph_cmd=cmd)
    if out:
        out = out[0]['name']
        logger.info(out)
        if out == fs_name:
            logger.info("FileSystem got created from Ceph Side")
            ceph_validate = True
        else:
            logger.error("FileSystem was not present at Ceph Side")
            return False
    result = CFS.get(resource_name=fs_name)
    if result['metadata']['name']:
        logger.info(f"Filesystem got created from kubernetes Side")
        k8s_validate = True
    else:
        logger.error("Filesystem was not create at Kubernetes Side")
        return False
    return True if (ceph_validate and k8s_validate) else False


def get_all_storageclass_name():
    """
    Function for getting all storageclass

    Returns:
         list: list of storageclass name
    """
    SC = ocp.OCP(
        kind=constants.STORAGECLASS,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    sc_obj = SC.get()
    sample = sc_obj['items']

    storageclass = [
        item.get('metadata').get('name') for item in sample if (
            item.get('metadata').get('name') not in constants.IGNORE_SC
        )
    ]
    return storageclass


def delete_all_storageclass():
    """"
    Function for Deleting all storageclass

    Returns:
        bool: True if deletion is successful
    """

    SC = ocp.OCP(
        kind=constants.STORAGECLASS,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    storageclass_list = get_all_storageclass_name()
    for item in storageclass_list:
        logger.info(f"Deleting StorageClass with name {item}")
        assert SC.delete(resource_name=item)
    return True


def get_cephblockpool_name():
    """
    Function for getting all CephBlockPool

    Returns:
         list: list of cephblockpool name
    """
    POOL = ocp.OCP(
        kind=constants.CEPHBLOCKPOOL,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    sc_obj = POOL.get()
    sample = sc_obj['items']
    pool_list = [
        item.get('metadata').get('name') for item in sample
    ]
    return pool_list


def delete_cephblockpool(pool_name=None):
    """
    Function for deleting CephBlockPool

    Returns:
        bool: True if deletion of CephBlockPool is successful
    """
    POOL = ocp.OCP(
        kind=constants.CEPHBLOCKPOOL,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    pool_list = get_cephblockpool_name()
    if pool_name:
        assert POOL.delete(resource_name=pool_name)
    else:
        for item in pool_list:
            logger.info(f"Deleting CephBlockPool with name {item}")
            assert POOL.delete(resource_name=item)
    return True


def create_cephfilesystem():
    """
    Function for deploying CephFileSystem (MDS)

    Returns:
        bool: True if CephFileSystem creates successful
    """
    fs_data = templating.load_yaml_to_dict(constants.CEPHFILESYSTEM_YAML)
    fs_data['metadata']['name'] = create_unique_resource_name(
        'test', 'cephfs'
    )
    fs_data['metadata']['namespace'] = config.ENV_DATA['cluster_namespace']
    global CEPHFS_OBJ
    CEPHFS_OBJ = OCS(**fs_data)
    CEPHFS_OBJ.create()
    POD = pod.get_all_pods(
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    assert POD[0].ocp.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.MDS_APP_LABEL,
        timeout=120
    )

    assert validate_cephfilesystem(fs_name=fs_data['metadata']['name'])
    return True


def delete_all_cephfilesystem():
    """
    Function to Delete CephFileSysem

    Returns:
        bool: True if deletion of CephFileSystem is successful
    """
    CFS = ocp.OCP(
        kind=constants.CEPHFILESYSTEM,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    result = CFS.get()
    cephfs_dict = result['items']
    for item in cephfs_dict:
        assert CFS.delete(resource_name=item.get('metadata').get('name'))
    return True


def get_cephfs_name():
    """
    Function to retrive CephFS name
    Returns:
        str: Name of CFS
    """
    CFS = ocp.OCP(
        kind=constants.CEPHFILESYSTEM,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    result = CFS.get()
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

    clients = []
    role = config.get('role', 'client')
    clients = [cpod for cpod in ceph_pods if role in cpod.roles]

    idx = config.get('idx', 0)
    client = clients[idx]
    op = config.get('op', 'write')
    cleanup = ['--no-cleanup', '--cleanup'][config.get('cleanup', True)]
    pool = config.get('pool')

    pool = create_ceph_block_pool(pool)
    block = str(config.get('size', 4 << 20))
    time = config.get('time', 120)
    time = str(time)

    rados_bench = (
        f"rados --no-log-to-stderr "
        f"-b {block} "
        f"-p {pool.name} "
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
