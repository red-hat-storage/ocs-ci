import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, node
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.cluster import CephCluster

from tests import helpers


logger = logging.getLogger(__name__)


def health_check(nodes):
    """
    Perform Ceph and cluster health checks
    """
    node.wait_for_nodes_status(nodes)
    ceph_cluster = CephCluster()
    assert ceph_health_check(
        namespace=config.ENV_DATA['cluster_namespace']
    )
    ceph_cluster.cluster_health_check(timeout=60)


def create_resources(resources, run_io=True):
    """
    Sanity validation - Create resources (FS and RBD) and run IO

    Args:
        resources (tuple): Lists of projects, secrets, pools,
            storageclasses, pvcs and pods
        run_io (bool): True for run IO, False otherwise

    """
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
    for pvc in pvcs:
        helpers.wait_for_resource_state(pvc, constants.STATUS_BOUND)
        pvc.reload()

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
    for pod in pods:
        helpers.wait_for_resource_state(pod, constants.STATUS_RUNNING)
        pod.reload()

    if run_io:
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


def delete_resources(resources):
    """
    Sanity validation - Delete resources (FS and RBD)

    Args:
        resources (tuple): Lists of projects, secrets, pools,
            storageclasses, pvcs and pods

    """
    # Delete resources and run IO for both FS and RBD
    # Unpack resources
    projects, secrets, pools, storageclasses, pvcs, pods = resources[:6]

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
