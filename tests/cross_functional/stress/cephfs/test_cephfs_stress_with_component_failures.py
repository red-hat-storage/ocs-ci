import logging
import time
import re
import random

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, stress
from ocs_ci.helpers.cephfs_stress_helpers import (
    CephFSStressTestManager,
    verify_openshift_storage_ns_pods_in_running_state,
)
from ocs_ci.helpers.disruption_helpers import Disruptions
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.node import (
    get_node_objs,
    node_network_failure,
    wait_for_nodes_status,
)
from ocs_ci.ocs.platform_nodes import PlatformNodesFactory
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import get_active_mds_pod_objs, CephCluster

logger = logging.getLogger(__name__)


@magenta_squad
@stress
class TestCephfsStressWithFailures(E2ETest):
    """
    CephFS stress test with comprehensive component failures
    """

    def test_cephfs_stress_with_component_failures(
        self,
        project_factory,
        nodes,
    ):
        """
        CephFS stress test with inducing failures on Ceph components after
        each iteration of the stress workload.

        The test performs the following:
        1. Creates a CephFS stress job that runs multiple iterations.
            Iteration 1: Using smallfiles, double the existing base directory count on the CephFS mount by creating new
            directories and files.
            Iteration 2: If stable, increase to three times the original base directory count
            Iteration 3: If stable, increase to four times the original base directory count
            Iteration 4: If stable, increase to five times the original base directory count
            Subsequent Iterations (Gradual Increase): If stable, continue increasing the file and directory count by
            factors of 3, then 2, then 1, from the previous iteration's total

            File Operations (for each iteration):
            Perform a variety of file operations (e.g., create,append,rename,stat,chmod,ls-l) on the iter-(1) data
        2. After each iteration completes:
           - For EACH Ceph component (MDS, MGR, MON, OSD):
             a) Restart the node on which the active pod is running
             b) Restart operators and plugin pods
             c) Abruptly power off the node, then power it back on
             d) Induce network failure on the node, then recover it
        3. Waits for rebalance and health check after each failure

        Args:
            project_factory: Factory fixture to create projects
            nodes: Node fixture for node operations

        """
        CHECKS_RUNNER_INTERVAL_MINUTES = 30
        JOB_STATUS_CHECK_INTERVAL = 60
        REBALANCE_WAIT_TIME = 7200
        HEALTH_CHECK_WAIT_TIME = 180
        POWER_ON_WAIT_TIME = 600

        CEPH_COMPONENTS = ["mds", "mgr", "mon", "osd"]

        MULTIPLICATION_FACTORS = "1,2,3"

        proj_name = "cephfs-component-failure-test"
        project_factory(project_name=proj_name)
        stress_mgr = CephFSStressTestManager(namespace=proj_name)

        platform_nodes = PlatformNodesFactory()
        nodes_util = platform_nodes.get_nodes_platform()

        try:
            pvc_obj, _ = stress_mgr.setup_stress_test_environment(pvc_size="500Gi")

            stress_mgr.start_background_checks(
                interval_minutes=CHECKS_RUNNER_INTERVAL_MINUTES
            )

            cephfs_stress_job_obj = stress_mgr.create_cephfs_stress_job(
                pvc_name=pvc_obj.name,
                multiplication_factors=MULTIPLICATION_FACTORS,
                parallelism=3,
                completions=3,
                base_file_count=10000,
            )
            logger.info(
                f"CephFS stress job {cephfs_stress_job_obj.name} has been submitted"
            )

            logger.info(
                "\n====================================================\n"
                "Starting component failure tests after each iteration\n"
                "ALL components (MDS, MGR, MON, OSD) will be tested\n"
                "===================================================="
            )

            completed_iterations = 0
            last_checked_iteration = 0

            while True:
                if stress_mgr.verification_failures:
                    raise Exception(
                        f"Test failed due to validation failure: "
                        f"{stress_mgr.verification_failures[0]}"
                    )

                status = cephfs_stress_job_obj.status()

                if status == "Complete":
                    logger.info(
                        f"Job '{cephfs_stress_job_obj.name}' reached 'Complete' state"
                    )
                    break
                elif status != constants.STATUS_RUNNING:
                    raise Exception(
                        f"Job '{cephfs_stress_job_obj.name}' entered unexpected "
                        f"state '{status}'"
                    )

                job_pods = pod.get_pods_having_label(
                    label=f"job-name={cephfs_stress_job_obj.name}",
                    namespace=proj_name,
                )

                logger.info(f"Found {len(job_pods)} job pods")
                for jp in job_pods:
                    pod_name = jp.get("metadata", {}).get("name")
                    pod_status = jp.get("status", {}).get("phase")
                    logger.info(f"  Pod: {pod_name}, Status: {pod_status}")

                logger.info("Checking logs of all job pods for completed iterations")
                current_max_iteration = self._get_max_completed_iteration(
                    job_pods, proj_name
                )
                logger.info(f"Current max iteration detected: {current_max_iteration}")

                if current_max_iteration > last_checked_iteration:
                    completed_iterations = current_max_iteration
                    logger.info(
                        f"Detected completion of iteration {completed_iterations}\n"
                        f"Will now induce failures for ALL components: {CEPH_COMPONENTS}\n"
                    )

                    logger.info(
                        "Pausing background verification checks during failure injection..."
                    )
                    stress_mgr.pause_background_checks()

                    for component in CEPH_COMPONENTS:
                        logger.info(
                            f"Inducing ALL failures for component: {component.upper()}\n"
                            f"(After iteration {completed_iterations})\n"
                        )
                        self._induce_all_failures_for_component(
                            component,
                            nodes,
                            nodes_util,
                            REBALANCE_WAIT_TIME,
                            HEALTH_CHECK_WAIT_TIME,
                            POWER_ON_WAIT_TIME,
                            cephfs_stress_job_obj,
                        )
                        logger.info(f"Completed all failures for {component.upper()}\n")
                    logger.info(
                        "Resuming background verification checks after failure recovery..."
                    )
                    stress_mgr.resume_background_checks()
                    last_checked_iteration = completed_iterations

                    logger.info(
                        f"Completed ALL component failures after iteration {completed_iterations}\n"
                        f"Components tested: {', '.join(CEPH_COMPONENTS)}\n"
                    )
                    logger.info("Performing Health checks after ALL component failures")
                    verify_openshift_storage_ns_pods_in_running_state()
                    logger.info("Performing health check after ALL failures")
                    ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"])
                    logger.info("Health check passed after ALL failure")

                logger.info(
                    f"Job still running. Waiting {JOB_STATUS_CHECK_INTERVAL}s "
                    f"before next check..."
                )
                time.sleep(JOB_STATUS_CHECK_INTERVAL)

            logger.info(
                "\n====================================================\n"
                "CephFS component failure test completed successfully!\n"
                f"Total iterations completed: {completed_iterations}\n"
                f"Components tested per iteration: {', '.join(CEPH_COMPONENTS)}\n"
                f"Total failures induced: {completed_iterations * len(CEPH_COMPONENTS) * 4}\n"
                "===================================================="
            )

        finally:
            stress_mgr.teardown()

    def _get_max_completed_iteration(self, job_pods, namespace):
        """
        Get the maximum completed iteration number from all job pods.

        Args:
            job_pods (list): List of job pod objects
            namespace (str): Namespace where the pods are running

        Returns:
            int: Maximum completed iteration number (0 if none found)

        """
        max_iteration = 0

        for job_pod in job_pods:
            try:
                pod_name = job_pod.get("metadata", {}).get("name")
                logs = pod.get_pod_logs(pod_name=pod_name, namespace=namespace)

                if not logs or not isinstance(logs, str):
                    logger.info(
                        f"Pod {pod_name} has no logs yet or logs are not in string format"
                    )
                    continue

                log_lines = logs.split("\n")
                logger.info(f"Pod {pod_name} has {len(log_lines)} lines of logs")
                if len(log_lines) > 0:
                    logger.info(f"Last few lines: {log_lines[-5:]}")

                matches = re.findall(r"Completed iteration:\s*(\d+)", logs)

                if matches:
                    pod_max = max(int(match) for match in matches)
                    max_iteration = max(max_iteration, pod_max)
                    logger.info(f"Pod {pod_name} completed iteration {pod_max}")

            except Exception as e:
                logger.warning(f"Failed to get logs from pod {pod_name}: {e}")
                continue

        return max_iteration

    def _get_target_pod_for_component(self, component):
        """
        Get a target pod for the given component.
        For MDS: select from active MDS pods
        For other components: randomly select from available pods

        Args:
            component (str): Component name (mds, mgr, mon, osd)

        Returns:
            Pod object to target for failures

        """
        if component == "mds":
            active_mds_pods = get_active_mds_pod_objs()
            if not active_mds_pods:
                raise Exception("No active MDS pods found")
            target_pod = (
                random.choice(active_mds_pods)
                if len(active_mds_pods) > 1
                else active_mds_pods[0]
            )
            logger.info(
                f"Selected active {component.upper()} pod: {target_pod.name} "
                f"(from {len(active_mds_pods)} active MDS pods)"
            )
        else:
            disruption = Disruptions()
            disruption.set_resource(component)
            if not disruption.resource_obj:
                raise Exception(f"No {component} pods found")
            target_pod = random.choice(disruption.resource_obj)
            logger.info(
                f"Randomly selected {component.upper()} pod: {target_pod.name} "
                f"(from {len(disruption.resource_obj)} available pods)"
            )
        return target_pod

    def _induce_all_failures_for_component(
        self,
        component,
        nodes,
        nodes_util,
        rebalance_wait,
        health_check_wait,
        power_on_wait,
        job_obj,
    ):
        """
        Induce all types of failures for a given component.

        Args:
            component (str): Component name (mds, mgr, mon, osd)
            nodes: Node fixture
            nodes_util: Platform nodes utility for power operations
            rebalance_wait (int): Time to wait for rebalance
            health_check_wait (int): Time to wait for health check
            power_on_wait (int): Time to wait for node to power on
            job_obj: Job object to verify

        """
        logger.info(f"\n--- Failure 1/4: Restarting node for {component} pod ---")
        target_pod = self._get_target_pod_for_component(component)
        self._restart_node_with_pod(target_pod, nodes, component)
        self._wait_for_rebalance_and_health_check(
            component, rebalance_wait, health_check_wait
        )
        self._verify_job_still_running(job_obj)

        logger.info("\n--- Failure 2/4: Restarting operator and plugin pods ---")
        self._restart_operator_and_plugin_pods()
        self._wait_for_rebalance_and_health_check(
            component, rebalance_wait, health_check_wait
        )
        self._verify_job_still_running(job_obj)

        logger.info(
            f"\n--- Failure 3/4: Abruptly powering off node for {component} pod ---"
        )
        target_pod = self._get_target_pod_for_component(component)
        self._power_off_and_on_node(target_pod, nodes_util, component, power_on_wait)
        self._wait_for_rebalance_and_health_check(
            component, rebalance_wait, health_check_wait
        )
        self._verify_job_still_running(job_obj)

        logger.info(
            f"\n--- Failure 4/4: Inducing network failure on {component} pod node ---"
        )
        target_pod = self._get_target_pod_for_component(component)
        self._induce_network_failure_on_node(
            target_pod, nodes_util, component, power_on_wait
        )
        self._wait_for_rebalance_and_health_check(
            component, rebalance_wait, health_check_wait
        )
        self._verify_job_still_running(job_obj)

    def _restart_node_with_pod(self, pod_obj, nodes, component):
        """
        Restart the node on which the given pod is running.

        Args:
            pod_obj: The pod object
            nodes: Node fixture
            component (str): Component name for logging

        """
        node_name = pod_obj.get().get("spec").get("nodeName")
        logger.info(f"Restarting node {node_name} hosting {component} pod")

        node_objs = get_node_objs([node_name])
        if not node_objs:
            raise Exception(f"Could not find node object for {node_name}")

        nodes.restart_nodes(node_objs, wait=True)
        logger.info(f"Node {node_name} restarted successfully")

        logger.info("Waiting for pods to be running after node restart")
        pod.wait_for_pods_to_be_running(
            timeout=600, namespace=config.ENV_DATA["cluster_namespace"]
        )

    def _restart_operator_and_plugin_pods(self):
        """
        Restart rook-operator, ocs-operator, and plugin pods.
        """
        pods_to_restart = [
            ("operator", "Rook operator"),
            ("ocs_operator", "OCS operator"),
            ("cephfsplugin", "CephFS plugin"),
            ("rbdplugin", "RBD plugin"),
        ]

        for resource_name, display_name in pods_to_restart:
            logger.info(f"Restarting {display_name} pods")
            disruption = Disruptions()
            disruption.set_resource(resource_name)

            for i in range(len(disruption.resource_obj)):
                try:
                    disruption.delete_resource(resource_id=i)
                    logger.info(f"Deleted {display_name} pod {i}")
                except Exception as e:
                    logger.warning(f"Failed to delete {display_name} pod {i}: {e}")

            logger.info(f"All {display_name} pods restarted")

    def _power_off_and_on_node(self, pod_obj, nodes_util, component, power_on_wait):
        """
        Abruptly power off the node, then power it back on.

        Args:
            pod_obj: The pod object
            nodes_util: Platform nodes utility
            component (str): Component name
            power_on_wait (int): Time to wait for node to power on

        """
        node_name = pod_obj.get().get("spec").get("nodeName")
        logger.info(f"Abruptly powering off node {node_name} hosting {component} pod")

        node_objs = get_node_objs([node_name])
        if not node_objs:
            raise Exception(f"Could not find node object for {node_name}")

        nodes_util.stop_nodes(node_objs, force=True)
        logger.info(f"Node {node_name} powered off")

        time.sleep(30)

        logger.info(f"Powering on node {node_name}")
        nodes_util.start_nodes(node_objs)

        logger.info(f"Waiting {power_on_wait}s for node {node_name} to be ready")
        wait_for_nodes_status(
            node_names=[node_name],
            status=constants.NODE_READY,
            timeout=power_on_wait,
        )
        logger.info(f"Node {node_name} is back online and ready")

        logger.info("Waiting for pods to be running after power cycle")
        pod.wait_for_pods_to_be_running(
            timeout=600, namespace=config.ENV_DATA["cluster_namespace"]
        )

    def _induce_network_failure_on_node(
        self, pod_obj, nodes_util, component, recovery_wait
    ):
        """
        Induce network failure on the node by bringing down network interface.

        Args:
            pod_obj: The pod object
            nodes_util: Platform nodes utility
            component (str): Component name
            recovery_wait (int): Time to wait for recovery

        """
        node_name = pod_obj.get().get("spec").get("nodeName")
        logger.info(
            f"Inducing network failure on node {node_name} hosting {component} pod"
        )

        node_network_failure([node_name], wait=True)
        logger.info(f"Network failure induced on node {node_name}")

        time.sleep(30)

        logger.info(f"Recovering node {node_name} from network failure")
        node_objs = get_node_objs([node_name])
        if not node_objs:
            raise Exception(f"Could not find node object for {node_name}")

        nodes_util.restart_nodes(node_objs, wait=True)
        logger.info(f"Node {node_name} restarted to recover from network failure")

        logger.info(f"Waiting {recovery_wait}s for node {node_name} to be ready")
        wait_for_nodes_status(
            node_names=[node_name],
            status=constants.NODE_READY,
            timeout=recovery_wait,
        )
        logger.info(f"Node {node_name} recovered from network failure")

        logger.info("Waiting for pods to be running after network recovery")
        pod.wait_for_pods_to_be_running(
            timeout=600, namespace=config.ENV_DATA["cluster_namespace"]
        )

    def _wait_for_rebalance_and_health_check(
        self, component, rebalance_wait, health_check_wait
    ):
        """
        Wait for rebalance to complete and perform health check.

        Args:
            component (str): Component name
            rebalance_wait (int): Time to wait for rebalance in seconds
            health_check_wait (int): Time to wait for health check in seconds

        """
        logger.info(f"Waiting {rebalance_wait}s for rebalance to complete...")
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=rebalance_wait
        ), "Data re-balance failed to complete"

        logger.info(f"Performing health check after {component} failure")
        ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"])
        logger.info(f"Health check passed after {component} failure")

        logger.info(f"Waiting additional {health_check_wait}s for stabilization...")
        time.sleep(health_check_wait)

    def _verify_job_still_running(self, job_obj):
        """
        Verify that the stress job is still running.

        Args:
            job_obj: Job object to check

        Raises:
            Exception: If job is not in running or complete state

        """
        status = job_obj.status()
        if status not in [constants.STATUS_RUNNING, "Complete"]:
            raise Exception(
                f"Job {job_obj.name} is in unexpected state: {status}. "
                "Expected Running or Complete."
            )
        logger.info(f"Job {job_obj.name} is still in {status} state")
