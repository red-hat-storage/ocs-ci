"""
Helper functions file for OCS QE
"""
import base64
import datetime
import logging

from ocs import constants, defaults, ocp
from utility.templating import load_yaml_to_dict
from ocsci import config
from resources import pod
from resources.ocs import OCS

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


def get_crd_dict(path_to_dict):
    """

    """
    return load_yaml_to_dict(path_to_dict)


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
        secret_data = get_crd_dict(defaults.CSI_RBD_SECRET)
        del secret_data['data']['kubernetes']
        secret_data['data']['admin'] = get_admin_key()
    elif interface_type == constants.CEPHFILESYSTEM:
        secret_data = get_crd_dict(defaults.CSI_CEPHFS_SECRET)
        del secret_data['data']['userID']
        del secret_data['data']['userKey']
        secret_data['data']['adminID'] = constants.ADMIN_BASE64
        secret_data['data']['adminKey'] = get_admin_key()
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
    cbp_data = get_crd_dict(defaults.CEPHBLOCKPOOL_YAML)
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
    interface_type, interface_name, secret_name, sc_name=None
):
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
        sc_data = get_crd_dict(defaults.CSI_RBD_STORAGECLASS_DICT)
        sc_data['parameters']['csi.storage.k8s.io/node-publish-secret-name'] = secret_name
        sc_data['parameters']['csi.storage.k8s.io/node-publish-secret-namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    elif interface_type == constants.CEPHFILESYSTEM:
        sc_data = get_crd_dict(defaults.CSI_CEPHFS_STORAGECLASS_DICT)
        sc_data['parameters']['csi.storage.k8s.io/node-stage-secret-name'] = secret_name
        sc_data['parameters']['csi.storage.k8s.io/node-stage-secret-namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    sc_data['parameters']['pool'] = interface_name

    mons = (
        f'rook-ceph-mon-a.{config.ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789,'
        f'rook-ceph-mon-b.{config.ENV_DATA["cluster_namespace"]}.'
        f'svc.cluster.local:6789,'
        f'rook-ceph-mon-c.{config.ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789'
    )
    sc_data['metadata']['name'] = sc_name if sc_name else create_unique_resource_name(
        'test', 'storageclass'
    )
    sc_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    sc_data['parameters']['csi.storage.k8s.io/provisioner-secret-name'] = secret_name
    sc_data['parameters']['csi.storage.k8s.io/provisioner-secret-namespace'] = defaults.ROOK_CLUSTER_NAMESPACE

    if interface_type == constants.CEPHBLOCKPOOL:
        sc_data['parameters']['clusterID'] = defaults.ROOK_CLUSTER_NAMESPACE
    elif interface_type == constants.CEPHFILESYSTEM:
        sc_data['parameters']['monitors'] = mons

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
    pvc_data = get_crd_dict(defaults.CSI_PVC_DICT)
    pvc_data['metadata']['name'] = pvc_name if pvc_name else create_unique_resource_name(
        'test', 'pvc'
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
    base64_output = base64.b64encode(out['key'].encode()).decode()
    return base64_output


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


def delete_cephblockpool():
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
    fs_data = get_crd_dict(defaults.CEPHFILESYSTEM_YAML)
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
    for pod_names in POD:
        if 'rook-ceph-mds' in pod_names.labels.values():
            assert pod_names.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector='app=rook-ceph-mds'
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
