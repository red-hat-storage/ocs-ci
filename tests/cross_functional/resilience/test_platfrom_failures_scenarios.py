import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad, resiliency
from ocs_ci.resiliency.resiliency_helper import Resiliency
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)


@green_squad
@resiliency
class TestPlatformFailureScenarios:
    """
    Test suite for validating ODF platform resiliency under various
    platform failure scenarios while I/O workloads are actively running.

    This test suite uses the workload_ops fixture which provides:
    - Automated workload creation and management
    - Background cluster operations
    - Optional workload scaling
    - Configuration via resiliency_tests_config.yaml
    """

    @pytest.mark.parametrize(
        "failure_case",
        [
            pytest.param(
                "PLATFORM_INSTANCE_FAILURES",
                marks=[
                    pytest.mark.polarion_id("OCS-6816"),
                    pytest.mark.polarion_id("OCS-6817"),
                ],
            ),
            pytest.param(
                "PLATFORM_NETWORK_FAILURES",
                marks=[
                    pytest.mark.polarion_id("OCS-6809"),
                    pytest.mark.polarion_id("OCS-6810"),
                ],
            ),
            pytest.param(
                "PLATFORM_NETWORK_FAULTS", marks=[pytest.mark.polarion_id("OCS-6817")]
            ),
        ],
    )
    def test_platform_failure_scenarios(
        self,
        failure_case,
        platfrom_failure_scenarios,
        workload_ops,
    ):
        """
        Test that validates ODF platform resiliency against various failure
        scenarios while I/O workloads are actively running.

        This test uses the workload_ops fixture which automatically:
        - Creates VDBENCH workloads on CephFS and RBD PVCs
        - Starts background cluster operations
        - Starts background scaling operations (if enabled in config)
        - Validates and cleans up all resources

        Configuration is loaded from resiliency_tests_config.yaml via:
            pytest --ocsci-conf conf/ocsci/resiliency_tests_config.yaml ...

        Steps:
        1. Setup workloads using workload_ops fixture (automated)
        2. Inject specific platform failure scenario
        3. Wait for failure injection to complete
        4. Validate workloads and cleanup (automated)

        Args:
            failure_case: The failure method to inject (e.g., PLATFORM_INSTANCE_FAILURES)
            platfrom_failure_scenarios: Fixture providing platform failure scenarios
            workload_ops: WorkloadOps fixture for workload management
        """
        scenario = platfrom_failure_scenarios.get("SCENARIO_NAME")
        log.info(f"Running Scenario: {scenario}, Failure Case: {failure_case}")

        resiliency_runner = None

        try:
            # Setup workloads (starts workloads, background ops, and scaling)
            log.info("Setting up workloads and background operations")
            workload_ops.setup_workloads()

            # Start failure injection
            log.info("Starting platform failure injection while workloads are running")
            resiliency_runner = Resiliency(scenario, failure_method=failure_case)
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

    @pytest.mark.parametrize(
        "failure_case",
        [
            pytest.param(
                "PLATFORM_INSTANCE_FAILURES",
                marks=[
                    pytest.mark.polarion_id("OCS-6819"),
                    pytest.mark.polarion_id("OCS-6813"),
                    pytest.mark.polarion_id("OCS-6819"),
                ],
            ),
            pytest.param(
                "PLATFORM_NETWORK_FAILURES",
                marks=[
                    pytest.mark.polarion_id("OCS-6812"),
                    pytest.mark.polarion_id("OCS-6814"),
                ],
            ),
        ],
    )
    def test_platform_failures_with_stress(
        self,
        failure_case,
        platfrom_failure_scenarios,
        workload_ops,
        run_platform_stress,
    ):
        """
        Test that validates ODF platform resiliency under stress conditions
        like high CPU, memory, I/O, and network load while failure scenarios
        are injected.

        This test uses the workload_ops fixture which automatically:
        - Creates VDBENCH workloads on CephFS and RBD PVCs
        - Starts background cluster operations
        - Starts background scaling operations (if enabled in config)
        - Validates and cleans up all resources

        Configuration is loaded from resiliency_tests_config.yaml via:
            pytest --ocsci-conf conf/ocsci/resiliency_tests_config.yaml ...

        Steps:
        1. Setup workloads using workload_ops fixture (automated)
        2. Start platform stress operations
        3. Inject specific platform failure scenario
        4. Wait for failure injection to complete
        5. Validate workloads and cleanup (automated)

        Args:
            failure_case: The failure method to inject
            platfrom_failure_scenarios: Fixture providing platform failure scenarios
            workload_ops: WorkloadOps fixture for workload management
            run_platform_stress: Fixture to run platform stress operations
        """
        scenario = platfrom_failure_scenarios.get("SCENARIO_NAME")
        log.info(f"Running Scenario: {scenario}, Failure Case: {failure_case}")

        resiliency_runner = None

        try:
            # Setup workloads (starts workloads, background ops, and scaling)
            log.info("Setting up workloads and background operations")
            workload_ops.setup_workloads()

            # Start platform stress
            log.info("Starting platform stress operations")
            run_platform_stress()

            # Start failure injection
            log.info("Starting platform failure injection under stress conditions")
            resiliency_runner = Resiliency(scenario, failure_method=failure_case)
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
            "Test completed successfully - workloads, stress, and failure injection completed"
        )
