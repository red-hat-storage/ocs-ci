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
