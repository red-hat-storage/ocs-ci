"""
StorageCluster related functionalities
"""
import pytest
import logging
from ocs_ci.ocs import defaults, constants
from ocs_ci.ocs.ocp import OCP, log
from ocs_ci.ocs.resources import pod
from tests import helpers


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


def add_capacity(capacity):
    """
        Add storage capacity to the cluster

        Args:
            capacity (int): Size of the storage to add

    """

    ocp = OCP(namespace=defaults.ROOK_CLUSTER_NAMESPACE, kind=constants.STORAGECLUSTER)
    osd_capacity = get_storage_cluster()['spec']['storageDeviceSets'][0]['dataPVCTemplate']['spec']['resources']['resources']['storage']
    osd_replicas = get_storage_cluster()['spec']['storageDeviceSets'][0]['replicas']
    worker_nodes = len(helpers.get_worker_nodes())
    current_osd_count = len(pod.get_osd_pods())
    available_osd_number = worker_nodes * 3 - current_osd_count

    if capacity % osd_capacity == 0:
        if capacity / osd_capacity * osd_replicas <= available_osd_number:
            sc = get_storage_cluster()
            ocp.patch(
                resource_name=sc['metadata']['name'],
                params=f'[{{"op": "replace", "path": "/spec/storageDeviceSets/0/count", '
                       f'"value":{capacity / osd_capacity * osd_replicas}}}]'
            )

            ocp.patch(
                resource_name=sc['metadata']['name'],
                params=f'[{{"op": "replace", "path": "/spec/storageDeviceSets/0/dataPVCTemplate/spec/resources'
                       f'/requests/storage", "value":{capacity}}}] '
            )
            return True
        else:
            log.info("not enough worker nodes")
            return False
    else:
        log.info("invalid storage capacity ")
        return False


def get_storage_cluster(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
        Get storage cluster name

        Args:
            namespace (str): Namespace of the resource
        Returns:
            yaml: Storage cluster yaml
    """

    sc_obj = OCP(kind=constants.STORAGECLUSTER, namespace=namespace)
    return sc_obj.get()