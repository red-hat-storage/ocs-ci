"""
StorageCluster related functionalities
"""
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


def add_capacity(capacity_string):
    """
   Add storage capacity to the cluster

   Args:
       capacity_string(int): Size of the storage to add

   Returns:
       True if capacity was added, False if not

   """
    ocp = OCP(namespace=defaults.ROOK_CLUSTER_NAMESPACE, kind=constants.STORAGECLUSTER)
    sc = ocp.get()
    old_device_set_count = sc.get('items')[0].get('spec').get('storageDeviceSets')[0].get('count')
    osd_size = parse_size_to_int(sc.get('items')[0].get('spec').get('storageDeviceSets')[0].get('dataPVCTemplate').get
                                 ('spec').get('resources').get('requests').get('storage'))
    capacity_to_add = str(parse_size_to_int(capacity_string) / osd_size + old_device_set_count)
    capacity_to_add = capacity_to_add.split(".")[0]
    # adding the storage capacity to the cluster
    ocp.patch(
        resource_name=sc['items'][0]['metadata']['name'],
        params='{"items":[{"spec": {"storageDeviceSets":[{"count":' 
               f"{capacity_to_add}"'}]}}]}',
        format_type='fetch'

    )
    #params=f'{{"op": "replace", "path": "items/0/spec/storageDeviceSets/0/count", 'f'"value":"{capacity_to_add}"}}',

    # cluster health check
    if utils.ceph_health_check:
        log.info("Cluster is not OK")
        return True
    else:
        return False
    # TODO - add another validations



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
    return int(num.split('G')[0])
