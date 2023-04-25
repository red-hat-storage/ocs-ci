import logging

from ocs_ci.framework import config
from ocs_ci.ocs import node, exceptions, constants
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.resources.pod import wait_for_storage_pods
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


class FlowOperations:
    """
    Flow based operations class

    """

    def __init__(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def validate_cluster(
        self,
        cluster_check=False,
        node_status=False,
        pod_status=False,
        operation_name="",
    ):
        """
        Validates various ceph and ocs cluster checks

        Args:
            node_status (bool): Verifies node is Ready
            pod_status (bool): Verifies StorageCluster pods in expected state
            operation_name (str): Name of the operation, to Tag

        """
        logger.info(f"{operation_name}: Verifying cluster health")
        assert ceph_health_check(
            config.ENV_DATA["cluster_namespace"], tries=100
        ), "Entry criteria FAILED: Cluster is Unhealthy"
        if cluster_check:
            self.sanity_helpers.health_check(tries=100)
        if node_status:
            logger.info(f"{operation_name}: Verifying whether node is ready")
            wait_for_nodes_status(status=constants.NODE_READY, timeout=300)
        if pod_status:
            logger.info(
                f"{operation_name}: Verifying StorageCluster pods are in running/completed state"
            )
            wait_for_storage_pods()

    def node_operations_entry_criteria(
        self,
        node_type,
        number_of_nodes,
        operation_name="Node Operation",
        network_fail_time=None,
    ):
        """
        Entry criteria function for node related operations

        Args:
            node_type (str): Type of node
            number_of_nodes (int): Number of nodes
            operation_name (str): Name of the node operation
            network_fail_time (int): Total time to fail the network in a node

        Returns:
            tuple: containing the params used in Node operations

        """
        self.validate_cluster(node_status=True, operation_name=operation_name)

        logger.info(f"Getting parameters related to: {operation_name}")
        typed_nodes = node.get_nodes(node_type=node_type, num_of_nodes=number_of_nodes)
        if network_fail_time:
            return typed_nodes, network_fail_time
        else:
            return typed_nodes

    def add_capacity_entry_criteria(self):
        """
        Entry criteria verification function for add capacity operation

        Returns:
            tuple: containing the params used in add capacity exit operation

        """
        self.validate_cluster(operation_name="Add Capacity")

        logger.info(
            "Add capacity: Getting restart count of pods before adding capacity"
        )
        restart_count_before = pod_helpers.get_pod_restarts_count(
            config.ENV_DATA["cluster_namespace"]
        )

        logger.info("Add capacity entry: Getting OSD pod count before adding capacity")
        osd_pods_before = pod_helpers.get_osd_pods()

        return osd_pods_before, restart_count_before

    def add_capacity_exit_criteria(self, restart_count_before, osd_pods_before):
        """
        Exit criteria function for Add capacity operation

        Args:
            restart_count_before (dict): Restart counts of pods
            osd_pods_before (list): List of OSD pods before

        """
        self.validate_cluster(operation_name="Add Capacity")

        logger.info("Add capacity: Getting restart count of pods after adding capacity")
        restart_count_after = pod_helpers.get_pod_restarts_count(
            config.ENV_DATA["cluster_namespace"]
        )
        logger.info(
            f"Sum of restart count before = {sum(restart_count_before.values())}"
        )
        logger.info(f"Sum of restart count after = {sum(restart_count_after.values())}")
        assert sum(restart_count_before.values()) == sum(
            restart_count_after.values()
        ), "Exit criteria verification FAILED: One or more pods got restarted"

        osd_pods_after = pod_helpers.get_osd_pods()
        number_of_osds_added = len(osd_pods_after) - len(osd_pods_before)
        logger.info(
            f"Number of OSDs added = {number_of_osds_added}, "
            f"before = {len(osd_pods_before)}, after = {len(osd_pods_after)}"
        )
        assert (
            number_of_osds_added == 3
        ), "Exit criteria verification FAILED: osd count mismatch"

        logger.info("Add capacity: Exit criteria verification: Success")


class BackgroundOps:
    def __init__(self):
        self.OPERATION_COMPLETED = False

    def handler(self, func, *args, **kwargs):
        """
        Wraps the function to run specific iterations

        Returns:
            bool : True if function runs successfully

        """
        iterations = kwargs.get("iterations", 1)
        func_name = func.__name__
        del kwargs["iterations"]
        for i in range(iterations):
            if self.OPERATION_COMPLETED:
                logger.info(
                    f"{func_name}: Done with execution. Stopping the thread. In iteration {i}"
                )
                return True
            else:
                func(*args, **kwargs)
                logger.info(f"{func_name}: iteration {i}")

    def wait_for_bg_operations(self, bg_ops, timeout=1200):
        """
        Waits for threads to be completed

        Args:
            bg_ops (list): Futures
            timeout (int): Time in seconds to wait

        """
        self.OPERATION_COMPLETED = True
        for thread in bg_ops:
            sample = TimeoutSampler(timeout=timeout, sleep=10, func=thread.done)
            assert sample.wait_for_func_status(result=True)

            try:
                logger.info(f"Thread completed: {thread.result()}")
            except exceptions.CommandFailed:
                logger.exception("Thread failed to complete")
                raise
            except Exception:
                logger.exception("Found an exception")
                raise
