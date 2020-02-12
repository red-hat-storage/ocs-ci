"""
StorageCluster related functionalities
"""
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP, log
from ocs_ci.utility import utils


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


def count_cluster_osd():
    """
    The function returns the number of cluster OSDs

    Returns:
         osd_count (int): number of OSD pods in current cluster

    """
    storage_cluster_obj = StorageCluster(
        resource_name=config.ENV_DATA['storage_cluster_name'],
        namespace=config.ENV_DATA['cluster_namespace'],
    )
    storage_cluster_obj.reload_data()
    osd_count = (
        int(storage_cluster_obj.data['spec']['storageDeviceSets'][0]['count'])
        * int(storage_cluster_obj.data['spec']['storageDeviceSets'][0]['replica'])
    )
    return osd_count


def add_capacity(capacity_string):
    """
   Add storage capacity to the cluster

   Args:
       capacity_string(int): Size of the storage to add
   Returns:
       True if capacity was added, False if not

   """
    ocp = OCP(namespace=defaults.ROOK_CLUSTER_NAMESPACE, kind=constants.STORAGECLUSTER)
    old_osd_count = count_cluster_osd()
    osd_size = parse_size_to_int(get_storage_cluster()['items'][0]['spec']['storageDeviceSets'][0]['dataPVCTemplate']
                                                      ['spec']['resources']['requests']['storage'])
    capacity_to_add = parse_size_to_int(capacity_string/osd_size + old_osd_count)
    sc = get_storage_cluster()

    # adding the storage capacity to the cluster
    ocp.patch(
        resource_name=sc['metadata']['name'],
        params=f'[{{"op": "replace", "path": "/spec/storageDeviceSets/0/count", '
               f'"value":{capacity_to_add}}}]'
    )

    # cluster health check
    if not utils.ceph_health_check:
        log.info("Capacity was not added")
        return False

    # osd amount validation
    new_osd_count = count_cluster_osd()
    expected = capacity_to_add / osd_size + old_osd_count
    if not expected == new_osd_count:
        log.info("Capacity was not added")
        return False

    log.info(f"{capacity_string} was added")
    return True


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


def parse_size_to_int(num):
    """
   Change capacity string to int

   Args:
       num (String): capacity in string format
   Returns:
       capacity (int) : capacity in int format (Gi)

   """
    place = num.find('G')
    capacity = int(num[0, place])
    return capacity
