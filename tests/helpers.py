"""
Helper functions file for OCS QE
"""
import logging
import re
import datetime
import statistics
import os
from subprocess import TimeoutExpired
import tempfile
import time
import yaml
import threading

from ocs_ci.ocs.ocp import OCP

from uuid import uuid4
from ocs_ci.ocs.exceptions import TimeoutExpiredError, UnexpectedBehaviour
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.utility import templating
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from ocs_ci.framework import config

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
    node_name=None, pod_dict_path=None, sa_name=None, dc_deployment=False,
    raw_block_pv=False, raw_block_device=constants.RAW_BLOCK_DEVICE, replica_count=1,
    pod_name=None
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
        sa_name (str): Serviceaccount name
        dc_deployment (bool): True if creating pod as deploymentconfig
        raw_block_pv (bool): True for creating raw block pv based pod, False otherwise
        raw_block_device (str): raw block device for the pod
        replica_count (int): Replica count for deployment config
        pod_name (str): Name of the pod to create

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
    pod_data = templating.load_yaml(pod_dict)
    if not pod_name:
        pod_name = create_unique_resource_name(
            f'test-{interface}', 'pod'
        )
    pod_data['metadata']['name'] = pod_name
    pod_data['metadata']['namespace'] = namespace
    if dc_deployment:
        pod_data['metadata']['labels']['app'] = pod_name
        pod_data['spec']['template']['metadata']['labels']['name'] = pod_name
        pod_data['spec']['replicas'] = replica_count

    if pvc_name:
        if dc_deployment:
            pod_data['spec']['template']['spec']['volumes'][0][
                'persistentVolumeClaim'
            ]['claimName'] = pvc_name
        else:
            pod_data['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] = pvc_name

    if interface_type == constants.CEPHBLOCKPOOL and raw_block_pv:
        pod_data['spec']['containers'][0]['volumeDevices'][0]['devicePath'] = raw_block_device
        pod_data['spec']['containers'][0]['volumeDevices'][0]['name'] = pod_data.get('spec').get('volumes')[
            0].get('name')

    if node_name:
        pod_data['spec']['nodeName'] = node_name
    else:
        if 'nodeName' in pod_data.get('spec'):
            del pod_data['spec']['nodeName']
    if sa_name and dc_deployment:
        pod_data['spec']['template']['spec']['serviceAccountName'] = sa_name
    if dc_deployment:
        ocs_obj = create_resource(**pod_data)
        logger.info(ocs_obj.name)
        assert (ocp.OCP(kind='pod', namespace=namespace)).wait_for_resource(
            condition=constants.STATUS_COMPLETED,
            resource_name=pod_name + '-1-deploy',
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
        logger.info(f'Creating new Pod {pod_name} for test')
        created_resource = pod_obj.create(do_reload=do_reload)
        assert created_resource, (
            f"Failed to create Pod {pod_name}"
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


def create_multilpe_projects(number_of_project):
    """
    Create one or more projects

    Args:
        number_of_project (int): Number of projects to be created

    Returns:
         list: List of project objects

    """
    project_objs = [create_project() for _ in range(number_of_project)]
    return project_objs


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
        secret_data = templating.load_yaml(
            constants.CSI_RBD_SECRET_YAML
        )
        secret_data['stringData']['userID'] = constants.ADMIN_USER
        secret_data['stringData']['userKey'] = get_admin_key()
        interface = constants.RBD_INTERFACE
    elif interface_type == constants.CEPHFILESYSTEM:
        secret_data = templating.load_yaml(
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
    cbp_data = templating.load_yaml(constants.CEPHBLOCKPOOL_YAML)
    cbp_data['metadata']['name'] = (
        pool_name if pool_name else create_unique_resource_name(
            'test', 'cbp'
        )
    )
    cbp_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    cbp_data['spec']['failureDomain'] = get_failure_domin()
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
    cfs_data = templating.load_yaml(constants.CEPHFILESYSTEM_YAML)
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
        sc_data = templating.load_yaml(
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
        sc_data = templating.load_yaml(
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
    size=None, do_reload=True, access_mode=constants.ACCESS_MODE_RWO,
    volume_mode=None
):
    """
    Create a PVC

    Args:
        sc_name (str): The name of the storage class for the PVC to be
            associated with
        pvc_name (str): The name of the PVC to create
        namespace (str): The namespace for the PVC creation
        size (str): Size of pvc to create
        do_reload (bool): True for wait for reloading PVC after its creation, False otherwise
        access_mode (str): The access mode to be used for the PVC
        volume_mode (str): Volume mode for rbd RWX pvc i.e. 'Block'

    Returns:
        PVC: PVC instance
    """
    pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
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
    if volume_mode:
        pvc_data['spec']['volumeMode'] = volume_mode
    ocs_obj = pvc.PVC(**pvc_data)
    created_pvc = ocs_obj.create(do_reload=do_reload)
    assert created_pvc, f"Failed to create resource {pvc_name}"
    return ocs_obj


def create_multiple_pvcs(
    sc_name, namespace, number_of_pvc=1, size=None, do_reload=False,
    access_mode=constants.ACCESS_MODE_RWO
):
    """
    Create one or more PVC

    Args:
        sc_name (str): The name of the storage class to provision the PVCs from
        namespace (str): The namespace for the PVCs creation
        number_of_pvc (int): Number of PVCs to be created
        size (str): The size of the PVCs to create
        do_reload (bool): True for wait for reloading PVC after its creation,
            False otherwise
        access_mode (str): The kind of access mode for PVC

    Returns:
         list: List of PVC objects
    """
    if access_mode == 'ReadWriteMany' and 'rbd' in sc_name:
        volume_mode = 'Block'
    else:
        volume_mode = None
    return [
        create_pvc(
            sc_name=sc_name, size=size, namespace=namespace,
            do_reload=do_reload, access_mode=access_mode, volume_mode=volume_mode
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


# TODO: revert counts of tries and delay,BZ 1726266

@retry(AssertionError, tries=20, delay=10, backoff=1)
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
            msg = f"{constants.PV} {pv_name} is not deleted after PVC deletion"
            raise AssertionError(msg)

    except CommandFailed:
        return True


def create_pods(pvc_objs, pod_factory, interface, pods_for_rwx=1, status=""):
    """
    Create pods

    Args:
        pvc_objs (list): List of ocs_ci.ocs.resources.pvc.PVC instances
        pod_factory (function): pod_factory function
        interface (int): Interface type
        pods_for_rwx (int): Number of pods to be created if access mode of
            PVC is RWX
        status (str): If provided, wait for desired state of each pod before
            creating next one

    Returns:
        list: list of Pod objects
    """
    pod_objs = []

    for pvc_obj in pvc_objs:
        volume_mode = getattr(
            pvc_obj, 'volume_mode', pvc_obj.get()['spec']['volumeMode']
        )
        access_mode = getattr(
            pvc_obj, 'access_mode', pvc_obj.get_pvc_access_mode
        )
        if volume_mode == 'Block':
            pod_dict = constants.CSI_RBD_RAW_BLOCK_POD_YAML
            raw_block_pv = True
        else:
            raw_block_pv = False
            pod_dict = ''
        if access_mode == constants.ACCESS_MODE_RWX:
            pod_obj_rwx = [pod_factory(
                interface=interface, pvc=pvc_obj, status=status,
                pod_dict_path=pod_dict, raw_block_pv=raw_block_pv
            ) for _ in range(1, pods_for_rwx)]
            pod_objs.extend(pod_obj_rwx)
        pod_obj = pod_factory(
            interface=interface, pvc=pvc_obj, status=status,
            pod_dict_path=pod_dict, raw_block_pv=raw_block_pv
        )
        pod_objs.append(pod_obj)

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


def get_master_nodes():
    """
    Fetches all master nodes.

    Returns:
        list: List of names of master nodes

    """
    label = 'node-role.kubernetes.io/master'
    ocp_node_obj = ocp.OCP(kind=constants.NODE)
    nodes = ocp_node_obj.get(selector=label).get('items')
    master_nodes_list = [node.get('metadata').get('name') for node in nodes]
    return master_nodes_list


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
    pod_name = pod.get_csi_provisioner_pod(interface)
    # get the logs from the csi-provisioner containers
    logs = pod.get_pod_logs(pod_name[0], 'csi-provisioner')
    logs += pod.get_pod_logs(pod_name[1], 'csi-provisioner')

    logs = logs.split("\n")
    # Extract the starting time for the PVC provisioning
    start = [
        i for i in logs if re.search(f"provision.*{pvc_name}.*started", i)
    ]
    start = start[0].split(' ')[1]
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
    pod_name = pod.get_csi_provisioner_pod(interface)
    # get the logs from the csi-provisioner containers
    logs = pod.get_pod_logs(pod_name[0], 'csi-provisioner')
    logs += pod.get_pod_logs(pod_name[1], 'csi-provisioner')

    logs = logs.split("\n")
    # Extract the starting time for the PVC provisioning
    end = [
        i for i in logs if re.search(f"provision.*{pvc_name}.*succeeded", i)
    ]
    end = end[0].split(' ')[1]
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


def get_default_storage_class():
    """
    Get the default StorageClass(es)

    Returns:
        list: default StorageClass(es) list

    """
    default_sc_obj = ocp.OCP(kind='StorageClass')
    storage_classes = default_sc_obj.get().get('items')
    storage_classes = [
        sc for sc in storage_classes if 'annotations' in sc.get('metadata')
    ]
    return [
        sc.get('metadata').get('name') for sc in storage_classes if sc.get(
            'metadata'
        ).get('annotations').get(
            'storageclass.kubernetes.io/is-default-class'
        ) == 'true'
    ]


def change_default_storageclass(scname):
    """
    Change the default StorageClass to the given SC name

    Args:
        scname (str): StorageClass name

    Returns:
        bool: True on success

    """
    default_sc = get_default_storage_class()
    ocp_obj = ocp.OCP(kind='StorageClass')
    if default_sc:
        # Change the existing default Storageclass annotation to false
        patch = " '{\"metadata\": {\"annotations\":" \
                "{\"storageclass.kubernetes.io/is-default-class\"" \
                ":\"false\"}}}' "
        patch_cmd = f"patch storageclass {default_sc} -p" + patch
        ocp_obj.exec_oc_cmd(command=patch_cmd)

    # Change the new storageclass to default
    patch = " '{\"metadata\": {\"annotations\":" \
            "{\"storageclass.kubernetes.io/is-default-class\"" \
            ":\"true\"}}}' "
    patch_cmd = f"patch storageclass {scname} -p" + patch
    ocp_obj.exec_oc_cmd(command=patch_cmd)
    return True


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

    service_account_data = templating.load_yaml(
        constants.SERVICE_ACCOUNT_YAML
    )
    service_account_data['metadata']['name'] = create_unique_resource_name(
        'sa', 'serviceaccount'
    )
    service_account_data['metadata']['namespace'] = namespace

    return create_resource(**service_account_data)


def get_serviceaccount_obj(sa_name, namespace):
    """
    Get serviceaccount obj

    Args:
        sa_name (str): Service Account name
        namespace (str): The namespace for the serviceaccount creation

    Returns:
        OCS: An OCS instance for the service_account
    """
    ocp_sa_obj = ocp.OCP(kind=constants.SERVICE_ACCOUNT, namespace=namespace)
    try:
        sa_dict = ocp_sa_obj.get(resource_name=sa_name)
        return OCS(**sa_dict)

    except CommandFailed:
        logger.error("ServiceAccount not found in specified namespace")


def validate_scc_policy(sa_name, namespace):
    """
    Validate serviceaccount is added to scc of privileged

    Args:
        sa_name (str): Service Account name
        namespace (str): The namespace for the serviceaccount creation

    Returns:
        bool: True if sc_name is present in scc of privileged else False
    """
    sa_name = f"system:serviceaccount:{namespace}:{sa_name}"
    logger.info(sa_name)
    ocp_scc_obj = ocp.OCP(kind=constants.SCC, namespace=namespace)
    scc_dict = ocp_scc_obj.get(resource_name=constants.PRIVILEGED)
    scc_users_list = scc_dict.get('users')
    for scc_user in scc_users_list:
        if scc_user == sa_name:
            return True
    return False


def add_scc_policy(sa_name, namespace):
    """
    Adding ServiceAccount to scc privileged

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
    Removing ServiceAccount from scc privileged

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
    dc_ocp_obj.wait_for_delete(resource_name=pod_obj.get_labels().get('name'))


def craft_s3_command(mcg_obj, cmd):
    """
    Crafts the AWS CLI S3 command including the
    login credentials and command to be ran

    Args:
        mcg_obj: An MCG object containing the MCG S3 connection credentials
        cmd: The AWSCLI command to run

    Returns:
        str: The crafted command, ready to be executed on the pod

    """
    if mcg_obj:
        base_command = (
            f"sh -c \"AWS_ACCESS_KEY_ID={mcg_obj.access_key_id} "
            f"AWS_SECRET_ACCESS_KEY={mcg_obj.access_key} "
            f"AWS_DEFAULT_REGION={mcg_obj.region} "
            f"aws s3 "
            f"--endpoint={mcg_obj.s3_endpoint} "
            f"--no-verify-ssl "
        )
        string_wrapper = "\""
    else:
        base_command = (
            f"aws s3 --no-verify-ssl --no-sign-request "
        )
        string_wrapper = ''

    return f"{base_command}{cmd}{string_wrapper}"


def wait_for_resource_count_change(
    func_to_use, previous_num, namespace, change_type='increase',
    min_difference=1, timeout=20, interval=2, **func_kwargs
):
    """
    Wait for a change in total count of PVC or pod

    Args:
        func_to_use (function): Function to be used to fetch resource info
            Supported functions: pod.get_all_pvcs(), pod.get_all_pods()
        previous_num (int): Previous number of pods/PVCs for comparison
        namespace (str): Name of the namespace
        change_type (str): Type of change to check. Accepted values are
            'increase' and 'decrease'. Default is 'increase'.
        min_difference (int): Minimum required difference in PVC/pod count
        timeout (int): Maximum wait time in seconds
        interval (int): Time in seconds to wait between consecutive checks

    Returns:
        True if difference in count is greater than or equal to
            'min_difference'. False in case of timeout.
    """
    try:
        for sample in TimeoutSampler(
            timeout, interval, func_to_use, namespace, **func_kwargs
        ):
            if func_to_use == pod.get_all_pods:
                current_num = len(sample)
            else:
                current_num = len(sample['items'])

            if change_type == 'increase':
                count_diff = current_num - previous_num
            else:
                count_diff = previous_num - current_num
            if count_diff >= min_difference:
                return True
    except TimeoutExpiredError:
        return False


def verify_pv_mounted_on_node(node_pv_dict):
    """
    Check if mount point of a PV exists on a node

    Args:
        node_pv_dict (dict): Node to PV list mapping
            eg: {'node1': ['pv1', 'pv2', 'pv3'], 'node2': ['pv4', 'pv5']}

    Returns:
        dict: Node to existing PV list mapping
            eg: {'node1': ['pv1', 'pv3'], 'node2': ['pv5']}
    """
    existing_pvs = {}
    for node, pvs in node_pv_dict.items():
        cmd = f'oc debug nodes/{node} -- df'
        df_on_node = run_cmd(cmd)
        existing_pvs[node] = []
        for pv_name in pvs:
            if f"/pv/{pv_name}/" in df_on_node:
                existing_pvs[node].append(pv_name)
    return existing_pvs


def converge_lists(list_to_converge):
    """
    Function to flatten and remove the sublist created during future obj

    Args:
       list_to_converge (list): arg list of lists, eg: [[1,2],[3,4]]

    Returns:
        list (list): return converged list eg: [1,2,3,4]
    """
    return [item for sublist in list_to_converge for item in sublist]


def create_multiple_pvc_parallel(
    sc_obj, namespace, number_of_pvc, size, access_modes
):
    """
    Funtion to create multiple PVC in parallel using threads
    Function will create PVCs based on the available access modes

    Args:
        sc_obj (str): Storage Class object
        namespace (str): The namespace for creating pvc
        number_of_pvc (int): NUmber of pvc to be created
        size (str): size of the pvc eg: '10Gi'
        access_modes (list): List of access modes for PVC creation

    Returns:
        pvc_objs_list (list): List of pvc objs created in function
    """
    obj_status_list, result_lists = ([] for i in range(2))
    with ThreadPoolExecutor() as executor:
        for mode in access_modes:
            result_lists.append(
                executor.submit(
                    create_multiple_pvcs, sc_name=sc_obj.name,
                    namespace=namespace, number_of_pvc=number_of_pvc,
                    access_mode=mode, size=size)
            )
    result_list = [result.result() for result in result_lists]
    pvc_objs_list = converge_lists(result_list)
    # Check for all the pvcs in Bound state
    with ThreadPoolExecutor() as executor:
        for objs in pvc_objs_list:
            obj_status_list.append(
                executor.submit(wait_for_resource_state, objs, 'Bound')
            )
    if False in [obj.result() for obj in obj_status_list]:
        raise TimeoutExpiredError
    return pvc_objs_list


def create_pods_parallel(pvc_list, namespace, interface, raw_block_pv=False):
    """
    Function to create pods in parallel

    Args:
        pvc_list (list): List of pvcs to be attached in pods
        namespace (str): The namespace for creating pod
        interface (str): The interface backed the PVC
        raw_block_pv (bool): Either RAW block or not

    Returns:
        pod_objs (list): Returns list of pods created
    """
    future_pod_objs = []
    # Added 300 sec wait time since in scale test once the setup has more
    # PODs time taken for the pod to be up will be based on resource available
    wait_time = 300
    if raw_block_pv:
        pod_dict_path = constants.CSI_RBD_RAW_BLOCK_POD_YAML
    else:
        pod_dict_path = None
    with ThreadPoolExecutor() as executor:
        for pvc_obj in pvc_list:
            future_pod_objs.append(executor.submit(
                create_pod, interface_type=interface,
                pvc_name=pvc_obj.name, do_reload=False, namespace=namespace,
                raw_block_pv=raw_block_pv, pod_dict_path=pod_dict_path)
            )
    pod_objs = [pvc_obj.result() for pvc_obj in future_pod_objs]
    # Check for all the pods are in Running state
    # In above pod creation not waiting for the pod to be created because of threads usage
    with ThreadPoolExecutor() as executor:
        for obj in pod_objs:
            future_pod_objs.append(
                executor.submit(wait_for_resource_state, obj, 'Running', timeout=wait_time)
            )
    # If pods not up raise exception/failure
    if False in [obj.result() for obj in future_pod_objs]:
        raise TimeoutExpiredError
    return pod_objs


def delete_objs_parallel(obj_list):
    """
    Function to delete objs specified in list
    Args:
        obj_list(list): List can be obj of pod, pvc, etc

    Returns:
        bool: True if obj deleted else False

    """
    threads = list()
    for obj in obj_list:
        process = threading.Thread(target=obj.delete)
        process.start()
        threads.append(process)
    for process in threads:
        process.join()
    return True


def memory_leak_analysis(median_dict):
    """
    Function to analyse Memory leak after execution of test case
    Memory leak is analyzed based on top output "RES" value of ceph-osd daemon,
    i.e. list[7] in code

    Args:
         median_dict (dict): dict of worker nodes and respective median value
         eg: median_dict = {'worker_node_1':102400, 'worker_node_2':204800, ...}

    More Detail on Median value:
        For calculating memory leak require a constant value, which should not be
        start or end of test, so calculating it by getting memory for 180 sec
        before TC execution and take a median out of it.
        Memory value could be different for each nodes, so identify constant value
        for each node and update in median_dict

    Usage:
        test_case(.., memory_leak_function):
            .....
            median_dict = helpers.get_memory_leak_median_value()
            .....
            TC execution part, memory_leak_fun will capture data
            ....
            helpers.memory_leak_analysis(median_dict)
            ....
    """
    # dict to store memory leak difference for each worker
    diff = {}
    for worker in get_worker_nodes():
        memory_leak_data = []
        if os.path.exists(f"/tmp/{worker}-top-output.txt"):
            with open(f"/tmp/{worker}-top-output.txt", "r") as f:
                data = f.readline()
                list = data.split(" ")
                list = [i for i in list if i]
                memory_leak_data.append(list[7])
        else:
            logging.info(f"worker {worker} memory leak file not found")
            raise UnexpectedBehaviour
        number_of_lines = len(memory_leak_data) - 1
        # Get the start value form median_dict arg for respective worker
        start_value = median_dict[f"{worker}"]
        end_value = memory_leak_data[number_of_lines]
        logging.info(f"Median value {start_value}")
        logging.info(f"End value {end_value}")
        # Convert the values to kb for calculations
        if start_value.__contains__('g'):
            start_value = float(1024 ** 2 * float(start_value[:-1]))
        elif start_value.__contains__('m'):
            start_value = float(1024 * float(start_value[:-1]))
        else:
            start_value = float(start_value)
        if end_value.__contains__('g'):
            end_value = float(1024 ** 2 * float(end_value[:-1]))
        elif end_value.__contains__('m'):
            end_value = float(1024 * float(end_value[:-1]))
        else:
            end_value = float(end_value)
        # Calculate the percentage of diff between start and end value
        # Based on value decide TC pass or fail
        diff[worker] = ((end_value - start_value) / start_value) * 100
        logging.info(f"Percentage diff in start and end value {diff[worker]}")
        if diff[worker] <= 20:
            logging.info(f"No memory leak in worker {worker} passing the test")
        else:
            logging.info(f"There is a memory leak in worker {worker}")
            logging.info(f"Memory median value start of the test {start_value}")
            logging.info(f"Memory value end of the test {end_value}")
            raise UnexpectedBehaviour


def get_memory_leak_median_value():
    """
    Function to calculate memory leak Median value by collecting the data for 180 sec
    and find the median value which will be considered as starting point
    to evaluate memory leak using "RES" value of ceph-osd daemon i.e. list[7] in code

    Returns:
        median_dict (dict): dict of worker nodes and respective median value
    """
    median_dict = {}
    timeout = 180  # wait for 180 sec to evaluate  memory leak median data.
    logger.info(f"waiting for {timeout} sec to evaluate the median value")
    time.sleep(timeout)
    for worker in get_worker_nodes():
        memory_leak_data = []
        if os.path.exists(f"/tmp/{worker}-top-output.txt"):
            with open(f"/tmp/{worker}-top-output.txt", "r") as f:
                data = f.readline()
                list = data.split(" ")
                list = [i for i in list if i]
                memory_leak_data.append(list[7])
        else:
            logging.info(f"worker {worker} memory leak file not found")
            raise UnexpectedBehaviour
        median_dict[f"{worker}"] = statistics.median(memory_leak_data)
    return median_dict


def refresh_oc_login_connection(user=None, password=None):
    """
    Function to refresh oc user login
    Default login using kubeadmin user and password

    Args:
        user (str): Username to login
        password (str): Password to login

    """
    user = user or config.RUN['username']
    if not password:
        filename = os.path.join(
            config.ENV_DATA['cluster_path'],
            config.RUN['password_location']
        )
        with open(filename) as f:
            password = f.read()
    ocs_obj = ocp.OCP()
    ocs_obj.login(user=user, password=password)


def rsync_kubeconf_to_node(node):
    """
    Function to copy kubeconfig to OCP node

    Args:
        node (str): OCP node to copy kubeconfig if not present

    """
    # ocp_obj = ocp.OCP()
    filename = os.path.join(
        config.ENV_DATA['cluster_path'],
        config.RUN['kubeconfig_location']
    )
    file_path = os.path.dirname(filename)
    master_list = get_master_nodes()
    ocp_obj = ocp.OCP()
    check_auth = 'auth'
    check_conf = 'kubeconfig'
    node_path = '/home/core/'
    if check_auth not in ocp_obj.exec_oc_debug_cmd(node=master_list[0], cmd_list=[f"ls {node_path}"]):
        ocp.rsync(
            src=file_path, dst=f"{node_path}", node=node, dst_node=True
        )
    elif check_conf not in ocp_obj.exec_oc_debug_cmd(node=master_list[0], cmd_list=[f"ls {node_path}auth"]):
        ocp.rsync(
            src=file_path, dst=f"{node_path}", node=node, dst_node=True
        )


def create_dummy_osd(deployment):
    """
    Replace one of OSD pods with pod that contains all data from original
    OSD but doesn't run osd daemon. This can be used e.g. for direct acccess
    to Ceph Placement Groups.

    Args:
        deployment (str): Name of deployment to use

    Returns:
        list: first item is dummy deployment object, second item is dummy pod
            object
    """
    oc = OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA.get('cluster_namespace')
    )
    osd_data = oc.get(deployment)
    dummy_deployment = create_unique_resource_name('dummy', 'osd')
    osd_data['metadata']['name'] = dummy_deployment

    osd_containers = osd_data.get('spec').get('template').get('spec').get(
        'containers'
    )
    # get osd container spec
    original_osd_args = osd_containers[0].get('args')
    osd_data['spec']['template']['spec']['containers'][0]['args'] = []
    osd_data['spec']['template']['spec']['containers'][0]['command'] = [
        '/bin/bash',
        '-c',
        'sleep infinity'
    ]
    osd_file = tempfile.NamedTemporaryFile(
        mode='w+', prefix=dummy_deployment, delete=False
    )
    with open(osd_file.name, "w") as temp:
        yaml.dump(osd_data, temp)
    oc.create(osd_file.name)

    # downscale the original deployment and start dummy deployment instead
    oc.exec_oc_cmd(f"scale --replicas=0 deployment/{deployment}")
    oc.exec_oc_cmd(f"scale --replicas=1 deployment/{dummy_deployment}")

    osd_list = pod.get_osd_pods()
    dummy_pod = [pod for pod in osd_list if dummy_deployment in pod.name][0]
    wait_for_resource_state(
        resource=dummy_pod,
        state=constants.STATUS_RUNNING,
        timeout=60
    )
    ceph_init_cmd = '/rook/tini' + ' ' + ' '.join(original_osd_args)
    try:
        logger.info('Following command should expire after 7 seconds')
        dummy_pod.exec_cmd_on_pod(ceph_init_cmd, timeout=7)
    except TimeoutExpired:
        logger.info('Killing /rook/tini process')
        try:
            dummy_pod.exec_bash_cmd_on_pod(
                "kill $(ps aux | grep '[/]rook/tini' | awk '{print $2}')"
            )
        except CommandFailed:
            pass

    return dummy_deployment, dummy_pod


def get_failure_domin():
    """
    Function is used to getting failure domain of pool

    Returns:
        str: Failure domain from cephblockpool

    """
    ct_pod = pod.get_ceph_tools_pod()
    out = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd crush rule dump", format='json')
    assert out, "Failed to get cmd output"
    for crush_rule in out:
        if constants.CEPHBLOCKPOOL.lower() in crush_rule.get("rule_name"):
            for steps in crush_rule.get("steps"):
                if "type" in steps:
                    return steps.get("type")


def wait_for_ct_pod_recovery():
    """
    In case the of node failures scenarios, in which the selected node is
    running the ceph tools pod, we'll want to wait for the pod recovery

    Returns:
        bool: True in case the ceph tools pod was recovered, False otherwise

    """
    try:
        _ = get_admin_key()
    except CommandFailed as ex:
        logger.info(str(ex))
        if "connection timed out" in str(ex):
            logger.info(
                "Ceph tools box was running on the node that had a failure. "
                "Hence, waiting for a new Ceph tools box pod to spin up"
            )
            wait_for_resource_count_change(
                func_to_use=pod.get_all_pods, previous_num=1,
                namespace=config.ENV_DATA['cluster_namespace'], timeout=120,
                selector=constants.TOOL_APP_LABEL
            )
            return True
        else:
            return False
    return True
