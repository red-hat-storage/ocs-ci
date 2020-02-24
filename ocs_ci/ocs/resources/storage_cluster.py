"""
StorageCluster related functionalities
"""
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP


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
       capacity(int): Size of the storage to add as number of deviceSets

   Returns:
       New number of deviceSets

   """
    ocp = OCP(namespace=defaults.ROOK_CLUSTER_NAMESPACE, kind=constants.STORAGECLUSTER)
    sc = ocp.get()
    device_set_count = sc.get('items')[0].get('spec').get('storageDeviceSets')[0].get('count')
    print("current: " + str(device_set_count))
    capacity_to_add = device_set_count + capacity
    print("new total: " + str(capacity_to_add))
    # adding the storage capacity to the cluster
    params = f"""[{{"op": "replace", "path": "/spec/storageDeviceSets/0/count", "value":{capacity_to_add}}}]"""
    ocp.patch(
        resource_name=sc['items'][0]['metadata']['name'],
        params=params,
        format_type='json'
    )

    new_count = sc.get('items')[0].get('spec').get('storageDeviceSets')[0].get('count')
    print("after add there are: (func) " + str(new_count))
    return new_count


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
