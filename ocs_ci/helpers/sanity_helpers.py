import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import ignore_leftovers
from ocs_ci.ocs.ocp import wait_for_cluster_connectivity
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs.resources.pvc import delete_pvcs
from ocs_ci.helpers import helpers
from ocs_ci.ocs.bucket_utils import s3_delete_object, s3_get_object, s3_put_object
from ocs_ci.helpers.pvc_ops import create_pvcs
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.cluster import CephCluster, CephClusterExternal


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
        self.obc_objs = list()
        self.obj_data = ""
        self.ceph_cluster = CephCluster()

    def health_check(self, cluster_check=True, tries=20):
        """
        Perform Ceph and cluster health checks
        """
        wait_for_cluster_connectivity(tries=400)
        logger.info("Checking cluster and Ceph health")
        node.wait_for_nodes_status(timeout=300)

        if not config.ENV_DATA["mcg_only_deployment"]:
            ceph_health_check(
                namespace=config.ENV_DATA["cluster_namespace"], tries=tries
            )
            if cluster_check:
                self.ceph_cluster.cluster_health_check(timeout=60)

    def create_resources(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory, run_io=True
    ):
        """
        Sanity validation: Create resources - pods, OBCs (RGW and MCG), PVCs (FS and RBD) and run IO

        Args:
            pvc_factory (function): A call to pvc_factory function
            pod_factory (function): A call to pod_factory function
            bucket_factory (function): A call to bucket_factory function
            rgw_bucket_factory (function): A call to rgw_bucket_factory function
            run_io (bool): True for run IO, False otherwise

        """
        logger.info(
            "Creating resources and running IO as a sanity functional validation"
        )

        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            pvc_obj = pvc_factory(interface)
            self.pvc_objs.append(pvc_obj)
            self.pod_objs.append(pod_factory(pvc=pvc_obj, interface=interface))
        if run_io:
            for pod in self.pod_objs:
                pod.run_io("fs", "1G", runtime=30)
            for pod in self.pod_objs:
                get_fio_rw_iops(pod)

        if rgw_bucket_factory:
            self.obc_objs.extend(rgw_bucket_factory(1, "rgw-oc"))

        if bucket_factory:
            self.obc_objs.extend(bucket_factory(amount=1, interface="OC"))

            self.ceph_cluster.wait_for_noobaa_health_ok()

    def delete_resources(self):
        """
        Sanity validation - Delete resources (pods, PVCs and OBCs)

        """
        logger.info("Deleting resources as a sanity functional validation")

        for pod_obj in self.pod_objs:
            pod_obj.delete()
        for pod_obj in self.pod_objs:
            pod_obj.ocp.wait_for_delete(pod_obj.name)
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete()
        for pvc_obj in self.pvc_objs:
            pvc_obj.ocp.wait_for_delete(pvc_obj.name)
        for obc_obj in self.obc_objs:
            obc_obj.delete(), f"OBC {obc_obj.name} still exists"

    @ignore_leftovers
    def create_pvc_delete(self, multi_pvc_factory, project=None):
        """
        Creates and deletes all types of PVCs

        """
        # Create rbd pvcs
        pvc_objs_rbd = create_pvcs(
            multi_pvc_factory=multi_pvc_factory,
            interface="CephBlockPool",
            project=project,
            status="",
            storageclass=None,
        )

        # Create cephfs pvcs
        pvc_objs_cephfs = create_pvcs(
            multi_pvc_factory=multi_pvc_factory,
            interface="CephFileSystem",
            project=project,
            status="",
            storageclass=None,
        )

        all_pvc_to_delete = pvc_objs_rbd + pvc_objs_cephfs

        # Check pvc status
        for pvc_obj in all_pvc_to_delete:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=300
            )

        # Start deleting PVC
        delete_pvcs(all_pvc_to_delete)

        # Check PVCs are deleted
        for pvc_obj in all_pvc_to_delete:
            pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

        logger.info("All PVCs are deleted as expected")

    def obc_put_obj_create_delete(self, mcg_obj, bucket_factory, timeout=300):
        """
        Creates bucket then writes, reads and deletes objects

        """
        bucket_name = bucket_factory(
            amount=1,
            interface="OC",
            timeout=timeout,
        )[0].name
        self.obj_data = "A string data"

        for i in range(0, 30):
            key = "Object-key-" + f"{i}"
            logger.info(f"Write, read and delete object with key: {key}")
            assert s3_put_object(
                mcg_obj, bucket_name, key, self.obj_data
            ), f"Failed: Put object, {key}"
            assert s3_get_object(
                mcg_obj, bucket_name, key
            ), f"Failed: Get object, {key}"
            assert s3_delete_object(
                mcg_obj, bucket_name, key
            ), f"Failed: Delete object, {key}"


class SanityExternalCluster(Sanity):
    """
    Helpers for health check and functional validation
    in External mode
    """

    def __init__(self):
        """
        Initializer for Sanity class - Init CephCluster() in order to
        set the cluster status before starting the tests
        """
        self.pvc_objs = list()
        self.pod_objs = list()
        self.obc_objs = list()
        self.ceph_cluster = CephClusterExternal()
