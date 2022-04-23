"""
Helper functions specific for DR
"""
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp

logger = logging.getLogger(__name__)


def failover(failover_cluster, drpc_name, namespace):
    """
    Initiates Failover action to the specified cluster

    Args:
        failover_cluster (str): Cluster name to which the workload should be failed over
        drpc_name (str): Name of the DRPC resource to apply the patch
        namespace (str): Name of the namespace to use

    """
    prev_index = config.cur_index
    config.switch_acm_ctx()
    failover_params = (
        f'{{"spec":{{"action":"Failover","failoverCluster":"{failover_cluster}"}}}}'
    )
    drpc_obj = ocp.OCP(
        kind=constants.DRPC, namespace=namespace, resource_name=drpc_name
    )
    drpc_obj._has_phase = True

    logger.info(f"Initiating failover action to {failover_cluster}")
    assert drpc_obj.patch(
        params=failover_params, format_type="merge"
    ), f"Failed to patch {constants.DRPC}: {drpc_name}"

    logger.info(
        f"Wait for {constants.DRPC}: {drpc_name} to reach {constants.STATUS_FAILEDOVER} phase"
    )
    drpc_obj.wait_for_phase(constants.STATUS_FAILEDOVER)

    config.switch_ctx(prev_index)


def relocate(preferred_cluster, drpc_name, namespace):
    """
    Initiates Relocate action to the specified cluster

    Args:
        preferred_cluster (str): Cluster name to which the workload should be relocated
        drpc_name (str): Name of the DRPC resource to apply the patch
        namespace (str): Name of the namespace to use

    """
    prev_index = config.cur_index
    config.switch_acm_ctx()
    relocate_params = (
        f'{{"spec":{{"action":"Relocate","preferredCluster":"{preferred_cluster}"}}}}'
    )
    drpc_obj = ocp.OCP(
        kind=constants.DRPC, namespace=namespace, resource_name=drpc_name
    )
    drpc_obj._has_phase = True

    logger.info(f"Initiating relocate action to {preferred_cluster}")
    assert drpc_obj.patch(
        params=relocate_params, format_type="merge"
    ), f"Failed to patch {constants.DRPC}: {drpc_name}"

    logger.info(
        f"Wait for {constants.DRPC}: {drpc_name} to reach {constants.STATUS_RELOCATED} phase"
    )
    drpc_obj.wait_for_phase(constants.STATUS_RELOCATED)

    config.switch_ctx(prev_index)


def get_drpc_name(namespace):
    """
    Get the DRPC Resource Name

    Args:
        namespace (str): Name of namespace

    Returns:
        str: DRPC resource name
    """

    drpc_obj = ocp.OCP(
        kind=constants.DRPC, namespace=namespace,
    ).get()['items'][0]
    return drpc_obj['metadata']['name']


def get_drpolicy_name(namespace):
    """
    Get DRPolicy Name

    Args:
        namespace (str) : Name of namespace

    Returns:
        str: DRPolicy name
    """

    drpolicy_obj = ocp.OCP(
        kind=constants.DRPOLICY, namespace=namespace,
    ).get()['items'][0]
    return drpolicy_obj['metadata']['name']


def get_primary_cluster_name(namespace):
    """
    Get Primary Cluster Name based on Namespace

    Args:
        namespace (str): Name of the Namespace

    Returns:
        str: Primary Cluster Name
    """
    config.switch_acm_ctx()
    drpc_resource_name = get_drpc_name(namespace=namespace)
    drpc_obj = ocp.OCP(
        kind=constants.DRPC, namespace="busybox-workloads-2", resource_name=drpc_resource_name
    ).get()

    if drpc_obj.get('spec').get('action') == constants.ACTION_FAILOVER:

        cluster_name = drpc_obj['spec']['failoverCluster']

    elif drpc_obj.get('spec').get('action') == constants.ACTION_RELOCATE:
        cluster_name = drpc_obj['spec']['preferredCluster']

    else:
        cluster_name = drpc_obj['spec']['preferredCluster']

    return cluster_name


def set_primary_cluster_context(namespace):
    """
    Set Primary Cluster Context based on Namespace

    Args:
        namespace (str): Name of the Namespace

    """
    cluster_name = get_primary_cluster_name(namespace)

    for index, cluster in enumerate(config.clusters):
        if cluster.ENV_DATA['cluster_name'] == cluster_name:
            config.switch_ctx(index)


def get_secondary_cluster_name(namespace):
    """
    Get Secondary Cluster Name based on Namespace

    Args:
        namespace (str): Name of the Namespace

    Returns:
        str: Secondary cluster name
    """
    config.switch_acm_ctx()
    drpc_resource_name = get_drpolicy_name(namespace)
    primary_cluster_name = get_primary_cluster_name(namespace)
    drpolicy_obj = ocp.OCP(
        kind=constants.DRPOLICY, namespace=namespace, resource_name=drpc_resource_name
    ).get()
    for cluster_name in drpolicy_obj['spec']['drClusterSet']:
        if not cluster_name['name'] == primary_cluster_name:
            return cluster_name['name']


def set_secondary_cluster_context(namespace):
    """
    Set Secondary Cluster Context based on Namespace

    Args:
        namespace (str): Name of the Namespace
    """
    cluster_name = get_secondary_cluster_name(namespace)
    for index, cluster in enumerate(config.clusters):
        if cluster.ENV_DATA['cluster_name'] == cluster_name:
            config.switch_ctx(index)
