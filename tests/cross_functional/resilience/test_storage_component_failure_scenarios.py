import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    resiliency,
    polarion_id,
)
from ocs_ci.resiliency.resiliency_helper import Resiliency
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)


@green_squad
@resiliency
class TestStorageClusterComponentFailurescenarios:
    """
    Test suite for validating ODF storage cluster component resiliency
    under various failure scenarios while I/O workloads are actively running.

    This test suite uses the workload_ops fixture which provides:
    - Automated workload creation and management
    - Background cluster operations
    - Optional workload scaling
    - Configuration via resiliency_tests_config.yaml
    """

    @pytest.mark.parametrize(
        argnames=["scenario_name", "failure_case"],
        argvalues=[
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "OSD_POD_FAILURES",
                marks=polarion_id("OCS-6821"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "MGR_POD_FAILURES",
                marks=polarion_id("OCS-6823"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "MDS_POD_FAILURES",
                marks=polarion_id("OCS-6850"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "MON_POD_FAILURES",
                marks=polarion_id("OCS-6822"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "RGW_POD_FAILURES",
                marks=polarion_id("OCS-6808"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "CEPHFS_POD_FAILURES",
                marks=polarion_id("OCS-6808"),
            ),
        ],
    )
    def test_storage_component_failure_scenarios(
        self,
        scenario_name,
        failure_case,
        workload_ops,
    ):
        """
        Test that validates ODF storage cluster component resiliency under
        failure scenarios while I/O workloads are actively running.

        This test uses the workload_ops fixture which automatically:
        - Creates VDBENCH workloads on CephFS and RBD PVCs
        - Starts background cluster operations
        - Starts background scaling operations (if enabled in config)
        - Validates and cleans up all resources

        Configuration is loaded from resiliency_tests_config.yaml via:
            pytest --ocsci-conf conf/ocsci/resiliency_tests_config.yaml ...

        Steps:
        1. Setup workloads using workload_ops fixture (automated)
        2. Inject specific failure scenario (e.g., OSD, MGR, MDS pod deletion)
        3. Wait for failure injection to complete
        4. Validate workloads and cleanup (automated)

        Args:
            scenario_name: Scenario category (e.g., STORAGECLUSTER_COMPONENT_FAILURES)
            failure_case: Specific failure to inject (e.g., OSD_POD_FAILURES)
            workload_ops: WorkloadOps fixture for workload management
        """
        log.info(f"Running Scenario: {scenario_name}, Failure Case: {failure_case}")

        resiliency_runner = None

        try:
            # Setup workloads (starts workloads, background ops, and scaling)
            log.info("Setting up workloads and background operations")
            workload_ops.setup_workloads()

            # Start failure injection
            log.info("Starting failure injection while workloads are running")
            resiliency_runner = Resiliency(scenario_name, failure_method=failure_case)
            resiliency_runner.start()

            # Cleanup failure injection
            resiliency_runner.cleanup()
            resiliency_runner = None

            # Validate and cleanup workloads
            log.info("Validating and cleaning up workloads")
            workload_ops.validate_and_cleanup()

        except UnexpectedBehaviour as e:
            log.error(f"Test execution failed: {e}")
            raise
        finally:
            # Cleanup failure injection if not already done
            if resiliency_runner:
                try:
                    resiliency_runner.cleanup()
                except UnexpectedBehaviour as cleanup_e:
                    log.warning(f"Failed to cleanup resiliency runner: {cleanup_e}")

        log.info(
            "Test completed successfully - workloads and failure injection completed"
        )
