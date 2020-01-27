"""
StorageCluster related functionalities
"""
import logging
from ocs_ci.ocs import defaults, constants
from ocs_ci.ocs.ocp import OCP, log
from ocs_ci.ocs.resources.pod import get_osd_pods, get_pod_count
from tests import helpers


logger = logging.getLogger(__name__)


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
    osd_capacity = parse_size_to_int(get_storage_cluster()['spec']['storageDeviceSets']
                                        [0]['dataPVCTemplate']['spec']
                                        ['resources']['resources']['storage'])

    osd_replicas = parse_size_to_int(get_storage_cluster()['spec']['storageDeviceSets'][0]['replicas'])
    worker_nodes = len(helpers.get_worker_nodes())
    current_osd_count = len(get_pod_count("app=rook-ceph-osd"))
    available_osd_number = (worker_nodes * 3) - current_osd_count
    capacity = parse_size_to_int(capacity_string)

    if capacity % osd_capacity == 0:
        if capacity / osd_capacity * osd_replicas <= available_osd_number:
            sc = get_storage_cluster()
            ocp.patch(
                resource_name=sc['metadata']['name'],
                params=f'[{{"op": "replace", "path": "/spec/storageDeviceSets/0/count", '
                       f'"value":{capacity / osd_capacity * osd_replicas}}}]'
            )
            osd_list = get_osd_pods()
            for pod in osd_list:
                if not ocp.wait_for_resource('Running', pod.name):
                    log.info(f" OSD pod {pod.name} faild to reach running state ")
                    return False
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


def parse_size_to_int(num):
    """
        Change capacity string to int

        Args:
            num (String):
        Returns:
            capacity (int) : capacity in int format (Gi)

    """
    place = num.find('G')
    capacity = int(num[0, place])
    return capacity
