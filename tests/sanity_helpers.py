import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import ignore_leftovers
from ocs_ci.ocs.ocp import wait_for_cluster_connectivity, OCP
from ocs_ci.ocs import constants, node, defaults
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs.resources.pvc import delete_pvcs
from tests import helpers
from ocs_ci.ocs.bucket_utils import s3_delete_object, s3_get_object, s3_put_object
from tests.manage.z_cluster.pvc_ops import create_pvcs
from ocs_ci.utility.utils import ceph_health_check, run_cmd
from ocs_ci.utility import templating
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
        self.obj_data = ""
        self.ceph_cluster = CephCluster()

    def health_check(self, cluster_check=True, tries=20):
        """
        Perform Ceph and cluster health checks
        """
        wait_for_cluster_connectivity(tries=400)
        logger.info("Checking cluster and Ceph health")
        node.wait_for_nodes_status(timeout=300)

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
        logger.info("Creating resources and running IO as a sanity functional validation")

        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            pvc_obj = pvc_factory(interface)
            self.pvc_objs.append(pvc_obj)
            self.pod_objs.append(pod_factory(pvc=pvc_obj, interface=interface))
        if run_io:
            for pod in self.pod_objs:
                pod.run_io('fs', '1G', runtime=30)
            for pod in self.pod_objs:
                get_fio_rw_iops(pod)
        self.create_obc()
        self.verify_obc()

    def create_obc(self):
        """
        OBC creation for RGW and Nooba

        """
        if config.ENV_DATA['platform'] in constants.ON_PREM_PLATFORMS:
            obc_rgw = templating.load_yaml(
                constants.RGW_OBC_YAML
            )
            obc_rgw_data_yaml = tempfile.NamedTemporaryFile(
                mode='w+', prefix='obc_rgw_data', delete=False
            )
            templating.dump_data_to_temp_yaml(
                obc_rgw, obc_rgw_data_yaml.name
            )
            logger.info("Creating OBC for rgw")
            run_cmd(f"oc create -f {obc_rgw_data_yaml.name}", timeout=2400)
            self.obc_rgw = obc_rgw['metadata']['name']

        obc_nooba = templating.load_yaml(
            constants.MCG_OBC_YAML
        )
        obc_mcg_data_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='obc_mcg_data', delete=False
        )
        templating.dump_data_to_temp_yaml(
            obc_nooba, obc_mcg_data_yaml.name
        )
        logger.info("create OBC for mcg")
        run_cmd(f"oc create -f {obc_mcg_data_yaml.name}", timeout=2400)
        self.obc_mcg = obc_nooba['metadata']['name']

    def delete_obc(self):
        """
        Clenaup OBC resources created above

        """
        if config.ENV_DATA['platform'] in constants.ON_PREM_PLATFORMS:
            logger.info(f"Deleting rgw obc {self.obc_rgw}")
            obcrgw = OCP(
                kind='ObjectBucketClaim',
                resource_name=f'{self.obc_rgw}'
            )
            run_cmd(f"oc delete obc/{self.obc_rgw}")
            obcrgw.wait_for_delete(
                resource_name=f'{self.obc_rgw}',
                timeout=300
            )

        logger.info(f"Deleting mcg obc {self.obc_mcg}")
        obcmcg = OCP(kind='ObjectBucketClaim', resource_name=f'{self.obc_mcg}')
        run_cmd(
            f"oc delete obc/{self.obc_mcg} -n "
            f"{defaults.ROOK_CLUSTER_NAMESPACE}"
        )
        obcmcg.wait_for_delete(resource_name=f'{self.obc_mcg}', timeout=300)

    def verify_obc(self):
        """
        OBC verification from external cluster perspective,
        we will check 2 OBCs

        """
        self.ceph_cluster.wait_for_noobaa_health_ok()

    def delete_resources(self):
        """
        Sanity validation - Delete resources (FS and RBD)

        """
        logger.info("Deleting resources as a sanity functional validation")

        self.delete_obc()

        for pod_obj in self.pod_objs:
            pod_obj.delete()
        for pod_obj in self.pod_objs:
            pod_obj.ocp.wait_for_delete(pod_obj.name)
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete()
        for pvc_obj in self.pvc_objs:
            pvc_obj.ocp.wait_for_delete(pvc_obj.name)

    @ignore_leftovers
    def create_pvc_delete(self, multi_pvc_factory, project=None):
        """
        Creates and deletes all types of PVCs

        """
        # Create rbd pvcs
        pvc_objs_rbd = create_pvcs(
            multi_pvc_factory=multi_pvc_factory, interface='CephBlockPool',
            project=project, status="", storageclass=None
        )

        # Create cephfs pvcs
        pvc_objs_cephfs = create_pvcs(
            multi_pvc_factory=multi_pvc_factory, interface='CephFileSystem',
            project=project, status="", storageclass=None
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

    def obc_put_obj_create_delete(self, mcg_obj, bucket_factory):
        """
        Creates bucket then writes, reads and deletes objects

        """
        bucket_name = bucket_factory(amount=1, interface='OC')[0].name
        self.obj_data = "A string data"

        for i in range(0, 30):
            key = 'Object-key-' + f"{i}"
            logger.info(f"Write, read and delete object with key: {key}")
            assert s3_put_object(mcg_obj, bucket_name, key, self.obj_data), f"Failed: Put object, {key}"
            assert s3_get_object(mcg_obj, bucket_name, key), f"Failed: Get object, {key}"
            assert s3_delete_object(mcg_obj, bucket_name, key), f"Failed: Delete object, {key}"


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
        self.ceph_cluster = CephClusterExternal()
