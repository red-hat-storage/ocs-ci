"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/dr_helpers.py
Insertion point: after function get_secondary_cluster_config
Description: Scale the Ramen DR cluster operator on a given cluster to the specified replica count

Review and merge this into ocs_ci/helpers/dr_helpers.py before running the test.
"""

def scale_ramen_operator(cluster_index, replica_count):
    """
    Scale the Ramen DR cluster operator deployment on the specified cluster.

    Switches to the target cluster context, scales the deployment, and
    switches back to the original context.

    Args:
        cluster_index (int): The multicluster config index of the target cluster.
        replica_count (int): The desired replica count (0 to disable, 1 to enable).
    """
    from ocs_ci.framework import config
    from ocs_ci.helpers.helpers import modify_deployment_replica_count
    from ocs_ci.ocs.constants import (
        RAMEN_DR_CLUSTER_OPERATOR_DEPLOYMENT,
        RAMEN_DR_SYSTEM_NAMESPACE,
    )

    logger = logging.getLogger(__name__)
    prev_ctx = config.cur_index
    config.switch_ctx(cluster_index)
    try:
        modify_deployment_replica_count(
            deployment_name=RAMEN_DR_CLUSTER_OPERATOR_DEPLOYMENT,
            replica_count=replica_count,
            namespace=RAMEN_DR_SYSTEM_NAMESPACE,
        )
        logger.info(
            f"Ramen operator scaled to {replica_count} replica(s) on cluster index {cluster_index}"
        )
    finally:
        config.switch_ctx(prev_ctx)
