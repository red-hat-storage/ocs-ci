"""
Test suite for unified multi-stress container chaos scenarios.

This module provides a comprehensive unified test for container chaos scenarios using the Krkn chaos engineering tool.
It includes a single test that creates ONE Krkn configuration with multiple stress levels:
- BASIC CHAOS: Conservative scenarios with safety controls
- STRENGTH TESTING: Moderate stress with multiple patterns
- MAXIMUM CHAOS: Ultimate resilience testing scenarios

The test creates VDBENCH workloads and executes all stress levels together in a single unified Krkn run
to validate system resilience across the complete stress spectrum.
"""

import pytest
import logging

from ocs_ci.ocs import constants

# Container chaos constants are now embedded directly in the test
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.krkn_chaos.krkn_helpers import (
    KrknResultAnalyzer,  # Result analysis helper
    CephHealthHelper,  # Ceph health helper
    ValidationHelper,  # Validation helper
    PodScenarioHelper,  # Pod scenario helper for pod kill tests
    ContainerScenarioHelper,  # Helper for building unified scenarios
)
from ocs_ci.krkn_chaos.krkn_scenario_generator import ContainerScenarios
from ocs_ci.krkn_chaos.logging_helpers import log_test_start

log = logging.getLogger(__name__)


@green_squad
@chaos
class TestKrKnContainerChaosScenarios:
    """
    Test suite for unified container chaos scenarios.

    Contains a comprehensive parameterized test that creates unified Krkn configurations
    with all ODF components and executes them together in single chaos runs with
    different kill signals (SIGKILL, SIGTERM).
    """

    @pytest.mark.parametrize(
        "kill_signal",
        [
            "SIGKILL",
            "SIGTERM",
        ],
        ids=[
            "sigkill-unified-chaos",
            "sigterm-unified-chaos",
        ],
    )
    @polarion_id("OCS-7336")
    def test_krkn_container_kill_signals_chaos(
        self,
        request,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        kill_signal,
    ):
        """
        Unified container chaos testing with configurable kill signals.

        This test demonstrates the flexibility of the unified approach
        by testing different kill signals (SIGKILL, SIGTERM) against all
        components in a unified configuration.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture that provides pre-configured VDBENCH workloads
            kill_signal: Kill signal to use for container termination
        """
        scenario_dir = krkn_scenario_directory

        # Use helper function for standardized test start logging
        log_test_start(
            f"UNIFIED MULTI-COMPONENT container ({kill_signal})",
            "all-components",
            component_name=f"unified-container-chaos-{kill_signal.lower()}",
            instance_count="multiple",
            safety_info=f"Testing multiple components with {kill_signal} signal",
        )

        # WORKLOAD SETUP
        log.info(f"Setting up workloads for unified {kill_signal} testing")
        workload_ops.setup_workloads()

        # Register finalizer for cleanup
        request.addfinalizer(lambda: workload_ops.validate_and_cleanup())

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        try:
            # =================================================================
            # UNIFIED KRKN CONFIG: All components with configurable kill signal
            # =================================================================
            log.info(f"Creating UNIFIED Krkn configuration with {kill_signal} signal")

            # Build unified scenarios using the helper class with specified kill signal
            scenario_helper = ContainerScenarioHelper()
            unified_scenarios = scenario_helper.build_unified_scenarios(
                namespace="openshift-storage",
                kill_signal=kill_signal,
                count=1,
                expected_recovery_time=120,
                container_name="",  # Leave blank to target all containers
            )

            # Log scenario details using helper function
            scenario_helper.log_scenario_details(
                unified_scenarios,
                title="UNIFIED COMPONENT SCENARIOS",
                kill_signal=kill_signal,
                style="detailed",
            )

            # Create unified container chaos scenario file using enhanced container_kill
            scenario_file = ContainerScenarios.container_kill(
                scenario_dir=scenario_dir,
                scenarios=unified_scenarios,
            )

            log.info(f"Created unified scenario file: {scenario_file}")

            # Create a unified Krkn configuration
            unified_config = KrknConfigGenerator()
            unified_config.add_scenario("container_scenarios", scenario_file)

            # =================================================================
            # UNIFIED EXECUTION: Single Krkn run with specified kill signal
            # =================================================================
            # Log execution start using helper function
            scenario_helper.log_execution_start(
                unified_scenarios, kill_signal=kill_signal
            )

            # Configure and write Krkn configuration to file
            unified_config.set_tunings(wait_duration=60, iterations=1)
            unified_config.write_to_file(location=krkn_scenario_directory)

            # Single Krkn execution with all component scenarios
            krkn_runner = KrKnRunner(unified_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            log.info(f"Unified multi-component chaos with {kill_signal} completed")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e,
                f"unified-components-{kill_signal.lower()}",
                f"multi-component container chaos ({kill_signal})",
            )
            raise
        except Exception as e:
            log.error(
                f"❌ Multi-component container chaos with {kill_signal} failed: {e}"
            )
            raise

        # =================================================================
        # UNIFIED RESULTS ANALYSIS
        # =================================================================
        log.info(f"MULTI-COMPONENT CHAOS RESULTS ({kill_signal}):")

        # Analyze overall results from unified execution
        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output,
                f"unified-components-{kill_signal.lower()}",
                detail_level="detailed",
            )
        )

        overall_success_rate = (
            (successful_executed / total_executed * 100) if total_executed > 0 else 0
        )

        # Log execution results using helper function
        scenario_helper.log_execution_results(
            total_executed,
            successful_executed,
            failing_executed,
            overall_success_rate,
            kill_signal=kill_signal,
        )

        # Detailed breakdown by component using helper function
        scenario_helper.log_scenario_summary(
            unified_scenarios,
            total_executed=total_executed,
            success_rate=overall_success_rate,
        )

        # Validate overall success rate
        min_success_rate = (
            70  # Multi-component testing should maintain good success rate
        )
        validator.validate_chaos_execution(
            total_executed,
            successful_executed,
            f"unified-components-{kill_signal.lower()}",
            f"multi-component container chaos ({kill_signal})",
        )

        analyzer.evaluate_chaos_success_rate(
            total_executed,
            successful_executed,
            f"unified-components-{kill_signal.lower()}",
            f"multi-component container chaos ({kill_signal})",
            min_success_rate,
        )

        # Final health check
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            None, f"multi-component container chaos ({kill_signal})"
        )
        assert no_crashes, crash_details

        log.info(
            f"🏆 Multi-component container chaos testing with {kill_signal} completed successfully! "
            f"All components handled unified execution with {overall_success_rate:.1f}% success rate. "
            f"✨ Unified configuration approach: {len(unified_scenarios)} component scenarios in single Krkn run!"
        )

    @pytest.mark.parametrize(
        "stress_level,kill_count,iterations",
        [
            ("basic", 4, 10),
            ("moderate", 7, 10),
            ("high", 12, 10),
        ],
        ids=[
            "basic-4pods-10iterations",
            "moderate-7pods-10iterations",
            "high-12pods-10iterations",
        ],
    )
    @polarion_id("OCS-7337")
    def test_krkn_random_pod_kill(
        self,
        request,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        stress_level,
        kill_count,
        iterations,
    ):
        """
        Random pod kill chaos testing with multiple stress levels.

        This test creates pod kill scenarios targeting the openshift-storage namespace
        with different stress levels:

        🎯 **Stress Levels:**
        - **BASIC**: Kill 4 pods with 10 iterations
        - **MODERATE**: Kill 7 pods with 10 iterations
        - **HIGH**: Kill 12 pods with 10 iterations

        Each test runs for the specified number of iterations to validate system
        resilience under different pod kill scenarios.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture that provides pre-configured VDBENCH workloads
            stress_level: Stress level name (basic/moderate/high)
            kill_count: Number of pods to kill per iteration
            iterations: Number of iterations to run
        """
        scenario_dir = krkn_scenario_directory

        # Use helper function for standardized test start logging
        log_test_start(
            "RANDOM POD KILL",
            f"{stress_level}-{kill_count}pods",
            component_name=f"{stress_level}_pod_kill",
            instance_count=kill_count,
            safety_info=f"Targeting openshift-storage namespace with {kill_count} pod kills",
        )

        # WORKLOAD SETUP
        log.info("Setting up workloads for pod kill testing")
        workload_ops.setup_workloads()

        # Register finalizer for cleanup
        request.addfinalizer(lambda: workload_ops.validate_and_cleanup())

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()
        pod_helper = PodScenarioHelper(
            scenario_dir=scenario_dir, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        try:
            # =================================================================
            # POD KILL SCENARIO CONFIGURATION
            # =================================================================
            log.info(
                f"Creating pod kill configuration for {stress_level} level: Target namespace: openshift-storage,"
                f"Pods to kill per iteration: {kill_count}, Total iterations: {iterations}, Recovery time: 120 seconds"
            )

            # Create Krkn configuration
            unified_config = KrknConfigGenerator()

            # Create pod kill scenario
            scenario_file = pod_helper.create_pod_kill_scenarios(
                kill_count=kill_count, recovery_time=300
            )

            # Add the pod scenario to the configuration
            # Since this is a pod disruption scenario, we need to add it to pod_disruption_scenarios
            unified_config.add_scenario("pod_disruption_scenarios", scenario_file)

            log.info(f"Created pod kill scenario: {scenario_file}")

            # =================================================================
            # KRKN EXECUTION WITH ITERATIONS
            # =================================================================
            log.info(
                f"Executing pod kill chaos: {iterations} iterations, stress level: {stress_level.upper()},"
                f"kill count per iteration: {kill_count}"
            )

            # Configure and write Krkn configuration to file
            unified_config.set_tunings(wait_duration=60, iterations=iterations)
            unified_config.write_to_file(location=krkn_scenario_directory)

            # Execute Krkn with the configuration
            krkn_runner = KrKnRunner(unified_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            log.info("Pod kill chaos execution completed")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e, f"{stress_level}_pod_kill", "random pod kill chaos"
            )
            raise
        except Exception as e:
            log.error(f"❌ Random pod kill chaos failed for {stress_level} level: {e}")
            raise

        # =================================================================
        # RESULTS ANALYSIS
        # =================================================================
        log.info("RANDOM POD KILL CHAOS RESULTS:")

        # Analyze results
        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output, f"{stress_level}_pod_kill", detail_level="detailed"
            )
        )

        overall_success_rate = (
            (successful_executed / total_executed * 100) if total_executed > 0 else 0
        )

        log.info(
            f"POD KILL EXECUTION RESULTS: Stress level: {stress_level.upper()},"
            f"Pods killed per iteration: {kill_count}, Total iterations executed: {total_executed},"
            f"Successful iterations: {successful_executed}, Failed iterations: {failing_executed},"
            f"Overall success rate: {overall_success_rate:.1f}%" + "=" * 60
        )

        # Validate success rate based on stress level
        min_success_rates = {
            "basic": 80,  # Basic level should have high success rate
            "moderate": 70,  # Moderate level allows some failures
            "high": 60,  # High level expects more failures
        }
        min_success_rate = min_success_rates.get(stress_level, 70)

        validator.validate_chaos_execution(
            total_executed,
            successful_executed,
            f"{stress_level}_pod_kill",
            "random pod kill chaos",
        )

        analyzer.evaluate_chaos_success_rate(
            total_executed,
            successful_executed,
            f"{stress_level}_pod_kill",
            "random pod kill chaos",
            min_success_rate,
        )

        # Final health check
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            None, "random pod kill chaos"
        )
        assert no_crashes, crash_details

        log.info(
            f"🏆 Random pod kill chaos testing for {stress_level} level completed successfully!"
            f"Killed {kill_count} pods per iteration across {iterations} iterations with"
            f"{overall_success_rate:.1f}% success rate."
            f"✨ Pod kill resilience validated for openshift-storage namespace!"
        )
