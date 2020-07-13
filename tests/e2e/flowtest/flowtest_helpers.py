import logging

from ocs_ci.ocs import node, platform_nodes, defaults, exceptions
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.resources.pod import wait_for_storage_pods
from ocs_ci.utility.utils import TimeoutSampler
from tests.manage.mcg.helpers import s3_delete_object, s3_put_object, s3_get_object
from tests.sanity_helpers import Sanity
from ocs_ci.framework.testlib import ignore_leftovers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import delete_pvcs
from tests import helpers
from tests.manage.z_cluster.pvc_ops import create_pvcs

logger = logging.getLogger(__name__)


class FlowOperations:
    def __init__(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def node_operations_entry_criteria(self, node_type, number_of_nodes, network_fail_time=None):
        """
        Entry criteria function for node related operations
        Args:
            node_type (str): Tyoe of node
            number_of_nodes (int): Number of nodes
            network_fail_time (int): Total time to fail the network in a node
        Returns:
            tuple: containing the params used in Node operations

        """
        logger.info("Node operations entry: Verifying cluster health")
        self.sanity_helpers.health_check(cluster_check=False)
        logger.info("Node operations entry: Verifying StorageCluster pods are in running/completed state")
        wait_for_storage_pods(timeout=300), 'Some pods were not in expected state'
        logger.info("Node operations entry: Verifying whether node is ready")
        wait_for_nodes_status(status=constants.NODE_READY, timeout=300)

        logger.info("Getting parameters related to Node operation")
        typed_nodes = node.get_typed_nodes(
            node_type=node_type, num_of_nodes=number_of_nodes
        )
        factory = platform_nodes.PlatformNodesFactory()
        nodes = factory.get_nodes_platform()

        if network_fail_time:
            return typed_nodes, nodes, network_fail_time
        else:
            return typed_nodes, nodes

    def node_operations_exit_criteria(self):
        """
        Exit criteria verification function for node related operations

        """
        logger.info("Node operations exit: Verifying whether node is ready")
        wait_for_nodes_status(status=constants.NODE_READY, timeout=300)
        logger.info("Node operations exit: Verifying cluster health")
        self.sanity_helpers.health_check(cluster_check=False, tries=80)
        logger.info("Node operations exit: Verifying StorageCluster pods are in running/completed state")
        wait_for_storage_pods(timeout=300), 'Some pods were not in expected state'

        logging.info("Node operation: Exit criteria verification: Success")

    def add_capacity_entry_criteria(self):
        """
        Entry criteria verification function for add capacity operation
        Returns:
            tuple: containing the params used in add capacity operation

        """
        logger.info("Add capacity entry: Verifying cluster health")
        self.sanity_helpers.health_check(cluster_check=False)
        logger.info("Add capacity entry: Verifying StorageCluster pods are in running/completed state")
        wait_for_storage_pods(timeout=300), 'Some pods were not in expected state'

        logger.info("Add capacity entry: Getting restart count of pods before adding capacity")
        restart_count_before = pod_helpers.get_pod_restarts_count(
            defaults.ROOK_CLUSTER_NAMESPACE)

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
        logger.info("Add capacity exit: Verifying cluster health")
        self.sanity_helpers.health_check(cluster_check=False, tries=80)
        logger.info("Add capacity exit: Verifying StorageCluster pods are in running/completed state")
        wait_for_storage_pods(timeout=300), 'Some pods were not in expected state'

        logger.info("Add capacity exit: Getting restart count of pods after adding capacity")
        restart_count_after = pod_helpers.get_pod_restarts_count(
            defaults.ROOK_CLUSTER_NAMESPACE)
        logging.info(f"sum(restart_count_before.values()) = {sum(restart_count_before.values())}")
        logging.info(f"sum(restart_count_after.values()) = {sum(restart_count_after.values())}")
        assert sum(restart_count_before.values()) == sum(restart_count_after.values(
        )), "Exit criteria verification FAILED: One or more pods got restarted"

        osd_pods_after = pod_helpers.get_osd_pods()
        number_of_osds_added = len(osd_pods_after) - len(osd_pods_before)
        logger.info(f"Number of osds added = {number_of_osds_added}, "
                    f"before = {len(osd_pods_before)}, after = {len(osd_pods_after)}")
        assert number_of_osds_added == 3, "Exit criteria verification FAILED: osd count mismatch"

        logging.info("Add capacity: Exit criteria verification: Success")


@ignore_leftovers
def create_pvc_delete(multi_pvc_factory, project=None):
    """
    Creates and deletes all types of PVCs
    Returns:
        bool : True if function runs successfully

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
    return True


def obc_put_obj_create_delete(mcg_obj, bucket_factory):
    """
    Creates bucket then writes, reads and deletes objects
    Returns:
        bool : True if function runs successfully

    """
    bucket_name = bucket_factory(amount=1, interface='OC')[0].name
    data = "A string data"

    for i in range(0, 30):
        key = 'Object-key-' + f"{i}"
        logger.info(f"Write, read and delete object with key: {key}")
        assert s3_put_object(mcg_obj, bucket_name, key, data), f"Failed: Put object, {key}"
        assert s3_get_object(mcg_obj, bucket_name, key), f"Failed: Get object, {key}"
        assert s3_delete_object(mcg_obj, bucket_name, key), f"Failed: Delete object, {key}"

    return True


class BackgroundOps:
    OPERATION_COMPLETED = False

    def handler(self, func, *args, **kwargs):
        """
        Wraps the function to run specific iterations
        Returns:
            bool : True if function runs successfully

        """
        iterations = kwargs.get('iterations', 1)
        func_name = func.__name__
        del kwargs['iterations']
        for i in range(iterations):
            if BackgroundOps.OPERATION_COMPLETED:
                logger.info(f"{func_name}: Done with execution. Stopping the thread. In iteration {i}")
                return True
            else:
                assert func(*args, **kwargs), f'{func_name} failed!'
                logger.info(f"{func_name}: iteration {i}")

    def wait_for_bg_operations(self, bg_ops, timeout=1200):
        """
        Waits for threads to be completed
        Args:
            bg_ops (list): Futures
            timeout (int): Time in seconds to wait

        """
        for thread in bg_ops:
            sample = TimeoutSampler(
                timeout=timeout, sleep=10, func=thread.done
            )
            assert sample.wait_for_func_status(result=True)

            try:
                logger.info(f"Thread completed: {thread.result()}")
            except exceptions.CommandFailed:
                logger.exception("Thread failed to complete")
                raise
            except Exception:
                logger.exception("Found an exception")
                raise
