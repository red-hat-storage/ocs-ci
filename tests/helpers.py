"""
Helper functions file for OCS QE
"""
import datetime
import logging
import time

from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.utility import templating
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler

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


def wait_for_resource_state(resource, state, timeout=60):
    """
    Wait for a resource to get to a given status

    Args:
        resource (OCS obj): The resource object
        state (str): The status to wait for
        timeout (int): Time in seconds to wait

    Returns:
        bool: True if resource reached the desired state, False otherwise
    """
    try:
        resource.ocp.wait_for_resource(
            condition=state, resource_name=resource.name, timeout=timeout
        )
    except TimeoutExpiredError:
        logger.error(f"{resource.kind} {resource.name} failed to reach {state}")
        resource.reload()
        logging.error(f"\n{resource.describe()}")
        return False
    logger.info(f"{resource.kind} {resource.name} reached state {state}")
    return True


def create_pod(
    interface_type=None, pvc_name=None, desired_status=constants.STATUS_RUNNING,
    wait=True, namespace=defaults.ROOK_CLUSTER_NAMESPACE, node_name=None,
    pod_dict_path=None
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

    pod_data = templating.load_yaml_to_dict(pod_dict)
    pod_data['metadata']['name'] = create_unique_resource_name(
        f'test-{interface}', 'pod'
    )
    pod_data['metadata']['namespace'] = namespace
    if pvc_name:
        pod_data['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] = pvc_name

    if node_name:
        pod_data['spec']['nodeName'] = node_name
    else:
        if 'nodeName' in pod_data.get('spec'):
            del pod_data['spec']['nodeName']

    pod_obj = pod.Pod(**pod_data)
    pod_name = pod_data.get('metadata').get('name')
    created_resource = pod_obj.create(do_reload=wait)
    assert created_resource, (
        f"Failed to create resource {pod_name}"
    )
    if wait:
        assert wait_for_resource_state(
            resource=pod_obj, state=desired_status, timeout=120
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
    cbp_obj.reload()

    assert verify_block_pool_exists(cbp_obj.name), (
        f"Block pool {cbp_obj.name} does not exist"
    )
    return cbp_obj


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
    return create_resource(**sc_data, wait=False)


def create_pvc(
    sc_name, pvc_name=None, namespace=defaults.ROOK_CLUSTER_NAMESPACE,
    size=None, wait=True, access_mode=constants.ACCESS_MODE_RWO,
    measure_time=False
):
    """
    Create a PVC

    Args:
        sc_name (str): The name of the storage class for the PVC to be
            associated with
        pvc_name (str): The name of the PVC to create
        namespace (str): The namespace for the PVC creation
        size(str): Size of pvc to create
        wait (bool): True for wait for the PVC operation to complete, False otherwise
        access_mode (str): The access mode to be used for the PVC
        measure_time (bool): Measure the creation time for a PVC.
            The requirement is 1 second for a PVC nad hence the assertion

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
    if measure_time:
        time_before = time.time()
    created_pvc = ocs_obj.create(do_reload=wait)
    if measure_time:
        time_after = time.time()
        t_time = time_after - time_before
        # 1 second for PVC creation is a requirement:
        # https://jira.coreos.com/browse/KNIP-627
        assert t_time < 1, (
            f"Creation time for PVC took longer than 1 second:\n"
            f"Creation time: {t_time}"
        )
    assert created_pvc, f"Failed to create resource {pvc_name}"
    if wait:
        assert wait_for_resource_state(
            ocs_obj, constants.STATUS_BOUND
        )
        ocs_obj.reload()

    return ocs_obj


def create_multiple_pvcs(
    sc_name, namespace, number_of_pvc=1, size=None,
    desired_status=constants.STATUS_BOUND, wait=True, wait_each=False
):
    """
    Create one or more PVC

    Args:
        sc_name (str): The name of the storage class to provision the PVCs from
        number_of_pvc (int): Number of PVCs to be created
        size (str): The size of the PVCs to create
        namespace (str): The namespace for the PVCs creation
        desired_status (str): The status of the PVC to wait for
        wait (bool): True for waiting for PVC to reach the desired status,
            False otherwise. Status of each PVC will be checked after creating
            all PVCs
        wait_each (bool): True for waiting for each PVC to reach the desired
            status before creating next PVC, False otherwise. This will take
            precedence over 'wait'

    Returns:
         list: List of PVC objects
    """

    pvc_objs = [
        create_pvc(
            sc_name=sc_name, size=size, namespace=namespace, wait=wait_each
        ) for _ in range(number_of_pvc)
    ]
    if wait and not wait_each:
        for pvc_obj in pvc_objs:
            assert wait_for_resource_state(pvc_obj, desired_status), (
                f"PVC {pvc_obj.name} failed to reach {desired_status} status"
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


def create_pods(
    pvc_objs_list, interface_type=None,
    desired_status=constants.STATUS_RUNNING, wait=True, wait_each=False,
    namespace=None
):
    """
    Create Pods.
    A pod will be created for each PVC in 'pvc_objs_list'.
    Args:
        pvc_objs_list (list): List of PVC objects
        interface_type (str): The interface type (CephFS, Cephblockpool, etc.)
        desired_status (str): The status of the pod to wait for
        wait (bool): True for waiting for pod to reach the desired
            status, False otherwise
        wait_each (bool): True for waiting for each pod to reach the desired
            status before creating next pod, False otherwise
        namespace(str): Name of the namespace
    Returns:
        list: List of Pod objects
    """
    pod_objs = []
    for pvc_obj in pvc_objs_list:
        pod_obj = create_pod(
            interface_type=interface_type, pvc_name=pvc_obj.name,
            desired_status=desired_status, wait=wait_each, namespace=namespace
        )
        pod_objs.append(pod_obj)

    if wait and not wait_each:
        for pod_obj in pod_objs:
            assert wait_for_resource_state(pod_obj, desired_status)
        logging.info(f"Verified: All pods are in '{desired_status}' state.")
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
