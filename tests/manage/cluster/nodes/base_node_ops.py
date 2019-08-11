import logging

from ocs_ci.framework import config
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs import constants, node
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs.cluster import CephCluster
from tests import helpers


logger = logging.getLogger(__name__)


class BaseNodes(ManageTest):
    """
    Base class for nodes restart related tests
    """
    def health_check(self):
        """
        Perform Ceph and cluster health checks
        """
        ceph_cluster = CephCluster()
        assert ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace']
        )
        ceph_cluster.cluster_health_check(timeout=60)

    def validate_cluster(self, resources, nodes, health_check=True):
        """
        Perform cluster validation - nodes readiness, Ceph cluster health
        check and functional resources tests
        """
        assert node.wait_for_nodes_status(nodes), (
            "Not all nodes reached status Ready"
        )
        if health_check:
            self.health_check()

        # Create resources and run IO for both FS and RBD
        # Unpack resources
        projects, secrets, pools, storageclasses, pvcs, pods = resources[:6]

        # Project
        projects.append(helpers.create_project())

        # Secrets
        secrets.append(helpers.create_secret(constants.CEPHBLOCKPOOL))
        secrets.append(helpers.create_secret(constants.CEPHFILESYSTEM))

        # Pools
        pools.append(helpers.create_ceph_block_pool())
        pools.append(helpers.get_cephfs_data_pool_name())

        # Storageclasses
        storageclasses.append(
            helpers.create_storage_class(
                interface_type=constants.CEPHBLOCKPOOL,
                interface_name=pools[0].name,
                secret_name=secrets[0].name
            )
        )
        storageclasses.append(
            helpers.create_storage_class(
                interface_type=constants.CEPHFILESYSTEM,
                interface_name=pools[1],
                secret_name=secrets[1].name
            )
        )

        # PVCs
        pvcs.append(helpers.create_pvc(
            sc_name=storageclasses[0].name, namespace=projects[0].namespace)
        )
        pvcs.append(helpers.create_pvc(
            sc_name=storageclasses[1].name, namespace=projects[0].namespace)
        )

        # Pods
        pods.append(
            helpers.create_pod(
                interface_type=constants.CEPHBLOCKPOOL, pvc_name=pvcs[0].name,
                namespace=projects[0].namespace
            )
        )
        pods.append(
            helpers.create_pod(
                interface_type=constants.CEPHFILESYSTEM, pvc_name=pvcs[1].name,
                namespace=projects[0].namespace
            )
        )

        # Run IO
        for pod in pods:
            pod.run_io('fs', '1G')
        for pod in pods:
            fio_result = pod.get_fio_results()
            logger.info(f"IOPs after FIO for pod {pod.name}:")
            logger.info(
                f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}"
            )
            logger.info(
                f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}"
            )
