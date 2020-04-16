import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.cluster import CephCluster


logger = logging.getLogger(__name__)


class Sanity:
    """
    Class for cluster health and functional validations
    """

    def __init__(self):
        """
        Initializer for Sanity class - Init CephCluster() in order to
        set the cluster status before starting the tests
        """
        self.pvc_objs = list()
        self.pod_objs = list()
        self.ceph_cluster = CephCluster()

    def health_check(self, cluster_check=True, tries=20):
        """
        Perform Ceph and cluster health checks
        """
        logger.info("Checking cluster and Ceph health")
        node.wait_for_nodes_status()

        ceph_health_check(namespace=config.ENV_DATA['cluster_namespace'], tries=tries)
        if cluster_check:
            self.ceph_cluster.cluster_health_check(timeout=60)

    def create_resources(self, pvc_factory, pod_factory, run_io=True):
        """
        Sanity validation - Create resources (FS and RBD) and run IO

        Args:
            pvc_factory (function): A call to pvc_factory function
            pod_factory (function): A call to pod_factory function
            run_io (bool): True for run IO, False otherwise

        """
        logger.info(f"Creating resources and running IO as a sanity functional validation")

        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            pvc_obj = pvc_factory(interface)
            self.pvc_objs.append(pvc_obj)
            self.pod_objs.append(pod_factory(pvc=pvc_obj))
        if run_io:
            for pod in self.pod_objs:
                pod.run_io('fs', '1G')
            for pod in self.pod_objs:
                get_fio_rw_iops(pod)

    def delete_resources(self):
        """
        Sanity validation - Delete resources (FS and RBD)

        """
        logger.info(f"Deleting resources as a sanity functional validation")

        for pod_obj in self.pod_objs:
            pod_obj.delete()
        for pod_obj in self.pod_objs:
            pod_obj.ocp.wait_for_delete(pod_obj.name)
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete()
        for pvc_obj in self.pvc_objs:
            pvc_obj.ocp.wait_for_delete(pvc_obj.name)
