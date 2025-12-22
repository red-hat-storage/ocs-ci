import logging
import pytest

from ocs_ci.framework.testlib import ignore_leftovers, ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import get_csv_name_start_with_prefix
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.pytest_customization.marks import tier1

logger = logging.getLogger(__name__)

# Parameterizing ocs & mcg operator probe values
OPERATOR_PROBE_PARAMS = [
    pytest.param("ocs-operator", "liveness", "/healthz"),
    pytest.param("ocs-operator", "readiness", "/readyz"),
    pytest.param("mcg-operator", "liveness", "/readyz"),
    pytest.param("mcg-operator", "readiness", "/readyz"),
]


@tier1
@ignore_leftovers
@pytest.mark.parametrize("csv_prefix, probe_type, healthy_path", OPERATOR_PROBE_PARAMS)
class TestOperatorProbeResilience(ManageTest):

    @pytest.fixture(autouse=True)
    def setup(self, csv_prefix):
        self.namespace = constants.OPENSHIFT_STORAGE_NAMESPACE

        # Using get_csv_name_start_with_prefix logic to resolve the CSV name
        self.csv_name = get_csv_name_start_with_prefix(csv_prefix, self.namespace)
        if not self.csv_name:
            pytest.fail(
                f"Could not find CSV with prefix {csv_prefix} in {self.namespace}"
            )

        logger.info(f"Resolved prefix '{csv_prefix}' to actual CSV: {self.csv_name}")

        self.csv_obj = OCP(
            kind=constants.CLUSTER_SERVICE_VERSION,
            namespace=self.namespace,
            resource_name=self.csv_name,
        )

        # Map labels for pod discovery based on the resolved name
        if "ocs-operator" in self.csv_name:
            self.label_key, self.label_value = "name", "ocs-operator"
        else:
            self.label_key, self.label_value = "noobaa-operator", "deployment"

    def _patch_csv(self, probe_type, path_value):
        """Helper function to apply JSON patch to the CSV."""
        base_path = f"/spec/install/spec/deployments/0/spec/template/spec/containers/0/{probe_type}Probe"
        patch_list = [
            {"op": "replace", "path": f"{base_path}/httpGet/path", "value": path_value},
            {"op": "replace", "path": f"{base_path}/initialDelaySeconds", "value": 5},
            {"op": "replace", "path": f"{base_path}/periodSeconds", "value": 5},
            {"op": "replace", "path": f"{base_path}/failureThreshold", "value": 1},
        ]
        logger.info(f"Patching {self.csv_name} {probe_type} to: {path_value}")
        self.csv_obj.patch(params=patch_list, format_type="json")

    def test_probe_resilience(self, probe_type, healthy_path):
        logger.info(
            f"Starting {probe_type.upper()} Probe Failure Test for {self.csv_name}"
        )

        # 1. Trigger and Verify Failure State
        if probe_type == "liveness":
            logger.info("Verifying Liveness failure: Expecting pod restarts...")
            self._patch_csv(probe_type, "/bad")

            def check_for_any_restarts():
                live_pods = pod_helpers.get_all_pods(
                    namespace=self.namespace,
                    selector=[self.label_value],
                    selector_label=self.label_key,
                )
                for p in live_pods:
                    try:
                        status = p.get().get("status", {})
                        container_statuses = status.get("containerStatuses", [])
                        if container_statuses:
                            restarts = container_statuses[0].get("restartCount", 0)
                            if restarts > 0:
                                logger.info(
                                    f"Confirmed: Pod {p.name} restarted {restarts} times."
                                )
                                return True
                    except (CommandFailed, TypeError, IndexError):
                        continue
                return False

            sample = TimeoutSampler(timeout=450, sleep=15, func=check_for_any_restarts)
            assert sample.wait_for_func_status(
                result=True
            ), f"Liveness failure: No pod restarts detected for {self.csv_name}."

        elif probe_type == "readiness":
            logger.info("Verifying Readiness failure: Expecting NotReady state...")
            bad_path = "/bad-ready" if "ocs-operator" in self.csv_name else "/bad"
            self._patch_csv(probe_type, bad_path)

            def check_for_not_ready():
                live_pods = pod_helpers.get_all_pods(
                    namespace=self.namespace,
                    selector=[self.label_value],
                    selector_label=self.label_key,
                )
                for p in live_pods:
                    try:
                        pod_data = p.get()
                        if pod_data.get("status", {}).get("phase") == "Running":
                            statuses = pod_data.get("status", {}).get(
                                "containerStatuses", []
                            )
                            if statuses and not statuses[0].get("ready"):
                                logger.info(
                                    f"Confirmed: Pod {p.name} is Running but NotReady."
                                )
                                return True
                    except (CommandFailed, TypeError):
                        continue
                return False

            sample = TimeoutSampler(timeout=300, sleep=10, func=check_for_not_ready)
            assert sample.wait_for_func_status(
                result=True
            ), f"Readiness failure: Pod did not reach NotReady for {self.csv_name}."

        # 2. Restore and Verify (Cleanup)
        logger.info(f"Restoring healthy configuration: {healthy_path}")
        self._patch_csv(probe_type, healthy_path)

        def final_sync():
            current_pods = pod_helpers.get_all_pods(
                namespace=self.namespace,
                selector=[self.label_value],
                selector_label=self.label_key,
            )
            if not current_pods:
                return False
            names = [p.name for p in current_pods]
            return pod_helpers.wait_for_pods_to_be_in_statuses(
                [constants.STATUS_RUNNING], pod_names=names, namespace=self.namespace
            )

        sample = TimeoutSampler(timeout=300, sleep=10, func=final_sync)
        assert sample.wait_for_func_status(
            result=True
        ), "Cleanup failed: Pods did not return to healthy Running state."
