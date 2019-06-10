"""
Helper functions file for OCS QE
"""
import logging
import base64
import datetime
from ocs import constants, defaults
from ocsci.config import ENV_DATA
from resources.ocs import OCS
from resources import pod

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
        secret_data = defaults.CSI_RBD_SECRET.copy()
    elif interface_type == constants.CEPHFILESYSTEM:
        secret_data = defaults.CSI_CEPHFS_SECRET.copy()
    secret_data['metadata']['name'] = create_unique_resource_name(
        'test', 'secret'
    )
    secret_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    del secret_data['data']['kubernetes']
    secret_data['data']['admin'] = get_admin_key()
    return create_resource(**secret_data, wait=False)


def create_ceph_block_pool():
    """
    Create a Ceph block pool

    Returns:
        OCS: An OCS instance for the Ceph block pool
    """
    cbp_data = defaults.CEPHBLOCKPOOL_DICT.copy()
    cbp_data['metadata']['name'] = create_unique_resource_name(
        'test', 'cbp'
    )
    cbp_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    cbp_obj = create_resource(**cbp_data, wait=False)

    assert verify_block_pool_exists(cbp_obj.name), (
        f"Block pool {cbp_obj.name} does not exist"
    )
    return cbp_obj


def create_storage_class(interface_type, interface_name, secret_name):
    """
    Create a storage class

    Args:
        interface_type (str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)
        interface_name (str): The name of the interface
        secret_name (str): The name of the secret

    Returns:
        OCS: An OCS instance for the storage class
    """
    sc_data = dict()
    if interface_type == constants.CEPHBLOCKPOOL:
        sc_data = defaults.CSI_RBD_STORAGECLASS_DICT.copy()
        sc_data['parameters']['pool'] = interface_name
    elif interface_type == constants.CEPHFILESYSTEM:
        sc_data = defaults.CEPHFILESYSTEM_DICT.copy()

    mons = (
        f'rook-ceph-mon-a.{ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789,'
        f'rook-ceph-mon-b.{ENV_DATA["cluster_namespace"]}.'
        f'svc.cluster.local:6789,'
        f'rook-ceph-mon-c.{ENV_DATA["cluster_namespace"]}'
        f'.svc.cluster.local:6789'
    )
    sc_data['metadata']['name'] = create_unique_resource_name(
        'test', 'storageclass'
    )
    sc_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    sc_data['parameters']['csi.storage.k8s.io/provisioner-secret-name'] = secret_name
    sc_data['parameters']['csi.storage.k8s.io/node-publish-secret-name'] = secret_name
    sc_data['parameters']['csi.storage.k8s.io/provisioner-secret-namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
    sc_data['parameters']['csi.storage.k8s.io/node-publish-secret-namespace'] = defaults.ROOK_CLUSTER_NAMESPACE

    sc_data['parameters']['monitors'] = mons
    del sc_data['parameters']['userid']
    return create_resource(**sc_data, wait=False)


def create_pvc(sc_name):
    """
    Args:
        sc_name (str): The name of the storage class for the PVC to be
            associated with

    Returns:
        OCS: An OCS instance for the PVC
    """
    pvc_data = defaults.CSI_PVC_DICT.copy()
    pvc_data['metadata']['name'] = create_unique_resource_name(
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
