import logging
import pytest

from ocs_ci.framework.testlib import ignore_leftovers, ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import get_csv_name_start_with_prefix
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.pytest_customization.marks import tier4c, blue_squad, polarion_id

logger = logging.getLogger(__name__)

# Parameterizing ocs & mcg operator probe values
OPERATOR_PROBE_PARAMS = [
    pytest.param("ocs-operator", "liveness", "/healthz"),
    pytest.param("ocs-operator", "readiness", "/readyz"),
    pytest.param("mcg-operator", "liveness", "/readyz"),
    pytest.param("mcg-operator", "readiness", "/readyz"),
]


@tier4c
@ignore_leftovers
@blue_squad
@polarion_id("OCS-7421")
@pytest.mark.parametrize("csv_prefix, probe_type, healthy_path", OPERATOR_PROBE_PARAMS)
class TestOperatorProbeResilience(ManageTest):
    """
    Test suite to verify the resilience of OCS and MCG operator probes.
    It ensures that the operators correctly handle liveness and readiness
    probe failures by restarting or changing pod status.
    """

    @pytest.fixture(autouse=True)
    def setup(self, csv_prefix):
        logger.info(f"Setting up test for CSV prefix: {csv_prefix}")

        self.namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        logger.debug(f"Using namespace: {self.namespace}")

        # Using get_csv_name_start_with_prefix logic to resolve the CSV name
        logger.debug(f"Resolving CSV name with prefix: {csv_prefix}")
        self.csv_name = get_csv_name_start_with_prefix(csv_prefix, self.namespace)

        if not self.csv_name:
            logger.error(
                f"Could not find CSV with prefix {csv_prefix} in {self.namespace}"
            )
            pytest.fail(
                f"Could not find CSV with prefix {csv_prefix} in {self.namespace}"
            )

        logger.info(f"Resolved prefix '{csv_prefix}' to actual CSV: {self.csv_name}")

        self.csv_obj = OCP(
            kind=constants.CLUSTER_SERVICE_VERSION,
            namespace=self.namespace,
            resource_name=self.csv_name,
        )
        logger.debug(f"Created CSV OCP object for {self.csv_name}")

        # Map labels for pod discovery based on the resolved name
        if "ocs-operator" in self.csv_name:
            self.label_key, self.label_value = "name", "ocs-operator"
        else:
            self.label_key, self.label_value = "noobaa-operator", "deployment"

        logger.info(f"Pod discovery labels: {self.label_key}={self.label_value}")

    @pytest.fixture(autouse=True)
    def teardown(self, request, probe_type, healthy_path):
        """
        Register the finalizer to restore the CSV state
        """

        def finalizer():
            logger.info(
                f"Teardown: Restoring {self.csv_name} {probe_type} probe to {healthy_path}"
            )

            # Resetting to healthy state
            self._patch_csv(probe_type, healthy_path)

            # Create a POD OCP object to wait for the selector
            selector = f"{self.label_key}={self.label_value}"
            logger.info(
                f"Waiting for pods to reach Running state (selector: {selector}, timeout: 300s)"
            )

            pod_obj = OCP(kind=constants.POD, namespace=self.namespace)
            pod_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=selector,
                timeout=300,
            )

            logger.info(
                f"Teardown completed: {self.csv_name} restored and pods running"
            )

        request.addfinalizer(finalizer)

    def _patch_csv(self, probe_type, path_value):
        """
        Helper function to apply JSON patch to the CSV.

        Args:
            probe_type (str): Type of probe to patch (liveness/readiness)
            path_value (str): The HTTP path value to set for the probe
        """
        base_path = f"/spec/install/spec/deployments/0/spec/template/spec/containers/0/{probe_type}Probe"
        patch_list = [
            {"op": "replace", "path": f"{base_path}/httpGet/path", "value": path_value},
            {"op": "replace", "path": f"{base_path}/initialDelaySeconds", "value": 5},
            {"op": "replace", "path": f"{base_path}/periodSeconds", "value": 5},
            {"op": "replace", "path": f"{base_path}/failureThreshold", "value": 1},
        ]
        logger.info(
            f"Patching {self.csv_name} {probe_type}Probe: "
            f"path={path_value}, initialDelay=5s, period=5s, failureThreshold=1"
        )
        logger.debug(f"JSON patch list: {patch_list}")
        self.csv_obj.patch(params=patch_list, format_type="json")
        logger.debug(f"Patch applied successfully to {self.csv_name}")

    def test_probe_resilience(self, probe_type, healthy_path):
        """
        Test case to verify operator pod behavior when probes fail.
        1. Patches the operator CSV with an invalid probe path.
        2. Verifies that liveness failure causes a restart.
        3. Verifies that readiness failure causes the pod to become NotReady.
        4. Restores the healthy configuration and ensures stability.
        """
        logger.info(
            f"Starting test: Verify {probe_type} probe failure resilience for {self.csv_name}"
        )
        logger.info(f"Healthy probe path: {healthy_path}")

        # 1. Trigger and Verify Failure State
        if probe_type == "liveness":
            logger.test_step("Trigger liveness probe failure and verify pod restarts")
            logger.info("Patching CSV with invalid liveness probe path: /bad")
            self._patch_csv(probe_type, "/bad")

            def check_for_any_restarts():
                live_pods = pod_helpers.get_all_pods(
                    namespace=self.namespace,
                    selector=[self.label_value],
                    selector_label=self.label_key,
                )
                logger.debug(f"Checking {len(live_pods)} pods for restarts")

                for p in live_pods:
                    try:
                        status = p.get().get("status", {})
                        container_statuses = status.get("containerStatuses", [])
                        if container_statuses:
                            restarts = container_statuses[0].get("restartCount", 0)
                            if restarts > 0:
                                logger.info(
                                    f"Pod restart detected: {p.name} restarted {restarts} times"
                                )
                                return True
                            else:
                                logger.debug(f"Pod {p.name}: no restarts yet (count=0)")
                    except (CommandFailed, TypeError, IndexError) as e:
                        logger.debug(f"Error checking pod {p.name}: {e}")
                        continue
                return False

            logger.info("Waiting for pod restarts (timeout: 450s, interval: 15s)")
            sample = TimeoutSampler(timeout=450, sleep=15, func=check_for_any_restarts)
            restart_detected = sample.wait_for_func_status(result=True)

            logger.assertion(
                f"Liveness probe failure caused pod restart: expected=True, actual={restart_detected}"
            )
            assert (
                restart_detected
            ), f"Liveness failure: No pod restarts detected for {self.csv_name}."
            logger.info("Liveness probe failure verified: Pod restarted as expected")

        elif probe_type == "readiness":
            logger.test_step(
                "Trigger readiness probe failure and verify pod becomes NotReady"
            )
            bad_path = "/bad-ready" if "ocs-operator" in self.csv_name else "/bad"
            logger.info(f"Patching CSV with invalid readiness probe path: {bad_path}")
            self._patch_csv(probe_type, bad_path)

            def check_for_not_ready():
                live_pods = pod_helpers.get_all_pods(
                    namespace=self.namespace,
                    selector=[self.label_value],
                    selector_label=self.label_key,
                )
                logger.debug(f"Checking {len(live_pods)} pods for NotReady state")

                for p in live_pods:
                    try:
                        pod_data = p.get()
                        # Removed Running phase check to allow for Pods in other states
                        # like 'Pending' or during transition as requested by reviewer.
                        statuses = pod_data.get("status", {}).get(
                            "containerStatuses", []
                        )
                        if statuses:
                            is_ready = statuses[0].get("ready", True)
                            if not is_ready:
                                logger.info(f"Pod NotReady state detected: {p.name}")
                                return True
                            else:
                                logger.debug(f"Pod {p.name}: still ready")
                    except (CommandFailed, TypeError) as e:
                        logger.debug(f"Error checking pod {p.name}: {e}")
                        continue
                return False

            logger.info("Waiting for pod NotReady state (timeout: 300s, interval: 10s)")
            sample = TimeoutSampler(timeout=300, sleep=10, func=check_for_not_ready)
            not_ready_detected = sample.wait_for_func_status(result=True)

            logger.assertion(
                f"Readiness probe failure caused NotReady state: expected=True, actual={not_ready_detected}"
            )
            assert (
                not_ready_detected
            ), f"Readiness failure: Pod did not reach NotReady for {self.csv_name}."
            logger.info(
                "Readiness probe failure verified: Pod became NotReady as expected"
            )

        # 2. Restore and Verify (Cleanup)
        logger.test_step("Restore healthy probe configuration and verify pod recovery")
        logger.info(f"Restoring {probe_type} probe to healthy path: {healthy_path}")
        self._patch_csv(probe_type, healthy_path)

        def final_sync():
            current_pods = pod_helpers.get_all_pods(
                namespace=self.namespace,
                selector=[self.label_value],
                selector_label=self.label_key,
            )
            if not current_pods:
                logger.debug("No pods found yet during recovery check")
                return False

            names = [p.name for p in current_pods]
            logger.debug(f"Checking {len(names)} pods for Running state: {names}")

            all_running = pod_helpers.wait_for_pods_to_be_in_statuses(
                [constants.STATUS_RUNNING], pod_names=names, namespace=self.namespace
            )
            if all_running:
                logger.info(f"All {len(names)} pods returned to Running state")
            return all_running

        logger.info(
            "Waiting for pods to return to Running state (timeout: 300s, interval: 10s)"
        )
        sample = TimeoutSampler(timeout=300, sleep=10, func=final_sync)
        recovery_successful = sample.wait_for_func_status(result=True)

        logger.assertion(
            f"Pods recovered to Running state after probe restoration: "
            f"expected=True, actual={recovery_successful}"
        )
        assert (
            recovery_successful
        ), "Cleanup failed: Pods did not return to healthy Running state."

        logger.info(
            f"Test passed: {probe_type} probe resilience verified for {self.csv_name}"
        )
