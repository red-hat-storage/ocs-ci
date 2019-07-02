"""
Helper functions file for OCS QE
"""
import datetime
import logging

from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
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


def create_resource(desired_status=constants.STATUS_AVAILABLE, wait=True, **kwargs):
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
    created_resource = ocs_obj.create()
    assert created_resource, (
        f"Failed to create resource {resource_name}"
    )
    if wait:
        assert ocs_obj.ocp.wait_for_resource(
            condition=desired_status, resource_name=resource_name
        ), f"{ocs_obj.kind} {resource_name} failed to reach"
        f"status {desired_status}"
    return ocs_obj


def create_pod(desired_status=constants.STATUS_RUNNING, wait=True, **kwargs):
    """
    Create a pod

    Args:
        desired_status (str): The status of the pod to wait for
        wait (bool): True for waiting for the pod to reach the desired
            status, False otherwise
        **kwargs: The pod data yaml converted to dict

    Returns:
        Pod: A Pod instance

    Raises:
        AssertionError: In case of any failure
    """
    pod_obj = pod.Pod(**kwargs)
    pod_name = kwargs.get('metadata').get('name')
    created_resource = pod_obj.create()
    assert created_resource, (
        f"Failed to create resource {pod_name}"
    )
    if wait:
        assert pod_obj.ocp.wait_for_resource(
            condition=desired_status, resource_name=pod_name
        ), f"{pod_obj.kind} {pod_name} failed to reach"
        f"status {desired_status}"
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
    elif interface_type == constants.CEPHFILESYSTEM:
        secret_data = templating.load_yaml_to_dict(
            constants.CSI_CEPHFS_SECRET_YAML
        )
        del secret_data['stringData']['userID']
        del secret_data['stringData']['userKey']
        secret_data['stringData']['adminID'] = constants.ADMIN_USER
        secret_data['stringData']['adminKey'] = get_admin_key()
    secret_data['metadata']['name'] = create_unique_resource_name(
        'test', 'secret'
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


def create_storage_class(interface_type, interface_name, secret_name, sc_name=None):
    """
    Create a storage class
    Args:
        interface_type (str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)
        interface_name (str): The name of the interface
        secret_name (str): The name of the secret
        sc_name (str): The name of storage class to create
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
        sc_data['parameters']['fsName'] = get_cephfs_name()
    sc_data['parameters']['pool'] = interface_name

    sc_data['metadata']['name'] = (
        sc_name if sc_name else create_unique_resource_name(
            'test', 'storageclass'
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

    try:
        del sc_data['parameters']['userid']
    except KeyError:
        pass
    return create_resource(**sc_data, wait=False)


def create_pvc(sc_name, pvc_name=None):
    """
    Create a PVC

    Args:
        sc_name (str): The name of the storage class for the PVC to be
            associated with
        pvc_name (str): The name of the PVC to create

    Returns:
        OCS: An OCS instance for the PVC
    """
    pvc_data = templating.load_yaml_to_dict(constants.CSI_PVC_YAML)
    pvc_data['metadata']['name'] = (
        pvc_name if pvc_name else create_unique_resource_name(
            'test', 'pvc'
        )
    )
    pvc_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    pvc_data['spec']['storageClassName'] = sc_name
    return create_resource(
        desired_status=constants.STATUS_BOUND, **pvc_data
    )


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
    Fetches ceph fs datapool name from CEPH

    Returns:
        str: fs datapool name
    """
    ct_pod = pod.get_ceph_tools_pod()
    out = ct_pod.exec_ceph_cmd('ceph fs ls')
    return out[0]['data_pools'][0]


@retry(CommandFailed, tries=5, delay=3, backoff=1)
def validate_cephfilesystem(fs_name):
    """
     Verify CephFileSystem exists at CEPH and OCP

     Args:
        fs_name (str): The name of the Ceph FileSystem

     Returns:
         bool: True if CephFileSystem is created at ceph and Ocp side else
            will return False with valid msg i.e Failure cause
    """
    CFS = ocp.OCP(
        kind=constants.CEPHFILESYSTEM,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    ct_pod = pod.get_ceph_tools_pod()
    ceph_validate = False
    ocp_validate = False
    cmd = "ceph fs ls"

    result = CFS.get(resource_name=fs_name)
    if result.get('metadata').get('name'):
        logger.info("Filesystem %s got created from Openshift Side", fs_name)
        ocp_validate = True
    else:
        logger.info(
            "Filesystem %s was not create at Openshift Side", fs_name
        )
        return False

    out = ct_pod.exec_ceph_cmd(ceph_cmd=cmd)
    if out:
        out = out[0]['name']
        if out == fs_name:
            logger.info("FileSystem %s got created from Ceph Side", fs_name)
            ceph_validate = True
        else:
            logger.error("FileSystem %s was not present at Ceph Side", fs_name)
            return False
    return ceph_validate and ocp_validate


def get_all_storageclass_name():
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
            item.get('metadata').get('name') not in constants.IGNORE_SC
        )
    ]
    return storageclass


def delete_storageclass(sc_name):
    """"
    Function for Deleting specific storageclass

    Args:
        sc_name (str): Name of sc for deletion

    Returns:
        bool: True if deletion is successful
    """

    sc_obj = ocp.OCP(
        kind=constants.STORAGECLASS,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    logger.info(f"Deleting StorageClass with name {sc_name}")
    return sc_obj.delete(resource_name=sc_name)


def get_cephblockpool_name():
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


def delete_cephblockpool(cbp_name):
    """
    Function for deleting specific CephBlockPool

    Args:
        cbp_name (str): Name of CBP for deletion

    Returns:
        bool: True if deletion of CephBlockPool is successful
    """
    pool_obj = ocp.OCP(
        kind=constants.CEPHBLOCKPOOL,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    logger.info(f"Deleting CephBlockPool with name {cbp_name}")
    return pool_obj.delete(resource_name=cbp_name)


def delete_cephfilesystem(fs_name):
    """
    Function to Delete CephFileSystem

    Args:
        fs_name (str): Name of CFS for deletion

    Returns:
        bool: True if deletion of CephFileSystem is successful
    """
    cfs_obj = ocp.OCP(
        kind=constants.CEPHFILESYSTEM,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    return cfs_obj.delete(resource_name=fs_name)


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
