import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.testlib import tier4, ignore_leftovers, ManageTest
from ocs_ci.utility import aws
from ocs_ci.ocs.cluster import CephCluster
from tests import helpers


logger = logging.getLogger(__name__)

AWS = aws.AWS()


@pytest.fixture()
def instances(request):
    """
    Get cluster instances

    Returns:
        dict: The ID keys and the name values of the instances
    """
    def finalizer():
        """
        Make sure all instances are running
        """
        AWS.start_ec2_instances(instances=instances, wait=True)

    request.addfinalizer(finalizer)

    nodes = ocp.get_all_nodes()
    instances = aws.get_instances_ids_and_names(nodes)
    return instances


@pytest.fixture()
def resources(request):
    """
    Delete the resources created during the test

    Returns:
        tuple: empty lists of resources

    """
    projects, secrets, pools, storageclasses, pvcs, pods = ([] for i in range(6))

    def finalizer():
        """
        Delete the resources created during the test
        """
        for resource_type in pods, pvcs, storageclasses, secrets:
            for resource in resource_type:
                resource.delete()
                resource.ocp.wait_for_delete(resource.name)
        if pools:
            # Delete only the RBD pool
            pools[0].delete()
        if projects:
            for project in projects:
                project.delete(resource_name=project.namespace)
                project.wait_for_delete(project.namespace)

    request.addfinalizer(finalizer)

    return projects, secrets, pools, storageclasses, pvcs, pods


class BaseNodesRestart(ManageTest):
    """
    Base class for nodes restart related tests
    """
    def validate_cluster(self, resources, instances):
        assert ocp.wait_for_nodes_ready(len(instances)), (
            "Not all nodes reached status Ready"
        )

        ceph_cluster = CephCluster()
        assert ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace']
        )
        ceph_cluster.cluster_health_check(timeout=60)

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


@tier4
@ignore_leftovers
class TestNodesRestart(BaseNodesRestart):
    """
    Test ungraceful cluster shutdown
    """

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'AWS',
        reason="Tests are not running on AWS deployed cluster"
    )
    @pytest.mark.parametrize(
        argnames=["force"],
        argvalues=[
            pytest.param(*[True], marks=pytest.mark.polarion_id("OCS-894")),
            pytest.param(*[False], marks=pytest.mark.polarion_id("OCS-895"))
        ]
    )
    def test_ungraceful_shutdown_aws(self, resources, instances, force):
        """
        Test ungraceful cluster shutdown - AWS
        """
        AWS.stop_ec2_instances(instances=instances, wait=True, force=force)
        AWS.start_ec2_instances(instances=instances, wait=True)
        self.validate_cluster(resources, instances)

# TODO: Add a test class for graceful shutdown
# TODO: Add fixtures and test methods for VMWare and RHHI.Next
