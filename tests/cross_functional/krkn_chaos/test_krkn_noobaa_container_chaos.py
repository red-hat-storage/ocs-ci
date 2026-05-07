"""
Test suite for Krkn NooBaa container kill chaos scenarios with various signals.

This module provides comprehensive tests for NooBaa container disruption using the Krkn chaos
engineering tool with different kill signals (SIGKILL, SIGTERM, SIGHUP, SIGINT, SIGQUIT).

Target Components:
- NooBaa Core (noobaa-core): Main S3 service container
- NooBaa Database (noobaa-db-pg): PostgreSQL database containers
- NooBaa Operator (noobaa-operator): Operator managing NooBaa lifecycle
- NooBaa Endpoint (noobaa-s3): S3 endpoint service containers

Kill Signals:
- SIGKILL (9): Immediate termination, no cleanup
- SIGTERM (15): Graceful termination request
- SIGHUP (1): Hang up signal, typically causes reload
- SIGINT (2): Interrupt signal (Ctrl+C equivalent)
- SIGQUIT (3): Quit signal with core dump

The tests validate system resilience by:
- Running continuous Warp S3 workload during chaos
- Verifying NooBaa containers recover from various signal types
- Ensuring no permanent data corruption
- Validating S3 service availability during container restarts
"""

import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.krkn_chaos.krkn_helpers import (
    KrknResultAnalyzer,
    CephHealthHelper,
    ValidationHelper,
    ContainerScenarioHelper,
)
from ocs_ci.krkn_chaos.krkn_scenario_generator import ContainerScenarios
from ocs_ci.krkn_chaos.logging_helpers import log_test_start

log = logging.getLogger(__name__)


@green_squad
@chaos
class TestKrKnNooBaaContainerChaos:
    """
    Test suite for NooBaa container kill chaos scenarios with various signals.

    Contains parameterized tests that target different NooBaa components with
    different kill signals to validate container-level resilience.
    """

    @pytest.mark.parametrize(
        "kill_signal",
        [
            "SIGKILL",  # Force kill - immediate termination
            "SIGTERM",  # Graceful termination
            "SIGHUP",  # Hang up - often triggers reload
            "SIGINT",  # Interrupt signal
            "SIGQUIT",  # Quit with core dump
        ],
        ids=[
            "sigkill-noobaa",
            "sigterm-noobaa",
            "sighup-noobaa",
            "sigint-noobaa",
            "sigquit-noobaa",
        ],
    )
    @polarion_id("OCS-7344")
    def test_krkn_noobaa_container_kill_signals(
        self,
        request,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        kill_signal,
    ):
        """
        Test NooBaa container disruption with various kill signals.

        This test validates NooBaa resilience by killing all NooBaa containers
        with different signals:
        - SIGKILL: Immediate termination without cleanup
        - SIGTERM: Graceful shutdown allowing cleanup
        - SIGHUP: Hang up signal (often triggers config reload)
        - SIGINT: Interrupt signal (Ctrl+C equivalent)
        - SIGQUIT: Quit signal with core dump

        The test runs Warp S3 workload continuously while killing containers
        to ensure NooBaa can handle container-level disruptions gracefully.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture providing Warp S3 workloads
            kill_signal: Kill signal to use for container termination
        """
        scenario_dir = krkn_scenario_directory

        # Use helper function for standardized test start logging
        log_test_start(
            f"NooBaa container kill ({kill_signal})",
            "all-noobaa-containers",
            component_name=f"noobaa-containers-{kill_signal.lower()}",
            signal=kill_signal,
            safety_info=f"Testing all NooBaa containers with {kill_signal} signal",
        )

        # WORKLOAD SETUP - Start Warp S3 workloads before chaos
        log.info(
            f"Setting up Warp S3 workloads for NooBaa container chaos testing with {kill_signal}"
        )
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
            # NOOBAA CONTAINER KILL SCENARIOS
            # =================================================================
            log.info(
                f"Creating NooBaa container kill configuration with {kill_signal} signal"
            )

            # Define NooBaa-specific components for container kill
            noobaa_components = [
                {
                    "name": "noobaa",
                    "description": "NooBaa (All Components)",
                },
            ]

            # Build container kill scenarios using the helper class
            scenario_helper = ContainerScenarioHelper()
            noobaa_scenarios = scenario_helper.build_unified_scenarios(
                namespace="openshift-storage",
                kill_signal=kill_signal,
                count=1,  # Kill 1 container at a time
                expected_recovery_time=120,
                container_name="",  # Leave blank to target all containers in the pod
                components=noobaa_components,
            )

            # Log scenario details
            scenario_helper.log_scenario_details(
                noobaa_scenarios,
                title=f"NOOBAA CONTAINER KILL SCENARIOS ({kill_signal})",
                kill_signal=kill_signal,
                style="detailed",
            )

            # Create container chaos scenario file
            scenario_file = ContainerScenarios.container_kill(
                scenario_dir=scenario_dir,
                scenarios=noobaa_scenarios,
            )

            log.info(f"Created NooBaa container kill scenario file: {scenario_file}")

            # Create Krkn configuration
            config = KrknConfigGenerator()
            config.add_scenario("container_scenarios", scenario_file)

            # =================================================================
            # EXECUTION: Single Krkn run with specified kill signal
            # =================================================================
            log.info(f"Executing NooBaa container chaos with {kill_signal}")

            # Configure and write Krkn configuration to file
            config.set_tunings(wait_duration=60, iterations=1)
            config.write_to_file(location=krkn_scenario_directory)

            # Execute Krkn
            krkn_runner = KrKnRunner(config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            log.info(f"NooBaa container chaos with {kill_signal} completed")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e,
                f"noobaa-container-{kill_signal.lower()}",
                f"NooBaa container chaos ({kill_signal})",
            )
            raise
        except Exception as e:
            log.error(f"❌ NooBaa container chaos with {kill_signal} failed: {e}")
            raise

        # =================================================================
        # RESULTS ANALYSIS
        # =================================================================
        log.info(f"NOOBAA CONTAINER CHAOS RESULTS ({kill_signal}):")

        # Analyze results
        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output,
                f"noobaa-container-{kill_signal.lower()}",
                detail_level="detailed",
            )
        )

        overall_success_rate = (
            (successful_executed / total_executed * 100) if total_executed > 0 else 0
        )

        log.info(
            f"EXECUTION RESULTS: Signal: {kill_signal}, "
            f"Total scenarios: {total_executed}, "
            f"Successful: {successful_executed}, "
            f"Failed: {failing_executed}, "
            f"Success rate: {overall_success_rate:.1f}%"
        )

        # Validate success rate
        min_success_rate = 70  # NooBaa containers should recover well from signals
        validator.validate_chaos_execution(
            total_executed,
            successful_executed,
            f"noobaa-container-{kill_signal.lower()}",
            f"NooBaa container chaos ({kill_signal})",
        )

        analyzer.evaluate_chaos_success_rate(
            total_executed,
            successful_executed,
            f"noobaa-container-{kill_signal.lower()}",
            f"NooBaa container chaos ({kill_signal})",
            min_success_rate,
        )

        # Final health check
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            None, f"NooBaa container chaos ({kill_signal})"
        )
        assert no_crashes, crash_details

        log.info(
            f"🏆 NooBaa container chaos testing with {kill_signal} completed successfully! "
            f"All NooBaa containers handled {kill_signal} with {overall_success_rate:.1f}% success rate."
        )

    @pytest.mark.parametrize(
        "component,kill_signal,iterations",
        [
            # NooBaa Core - main S3 service
            ("noobaa-core", "SIGKILL", 3),
            ("noobaa-core", "SIGTERM", 3),
            ("noobaa-core", "SIGHUP", 3),
            # NooBaa Database - PostgreSQL
            ("noobaa-db", "SIGKILL", 3),
            ("noobaa-db", "SIGTERM", 3),
            # NooBaa Operator
            ("noobaa-operator", "SIGKILL", 3),
            ("noobaa-operator", "SIGTERM", 3),
            # NooBaa Endpoint - S3 endpoints
            ("noobaa-endpoint", "SIGKILL", 3),
            ("noobaa-endpoint", "SIGTERM", 3),
        ],
        ids=[
            "noobaa-core-sigkill-3x",
            "noobaa-core-sigterm-3x",
            "noobaa-core-sighup-3x",
            "noobaa-db-sigkill-3x",
            "noobaa-db-sigterm-3x",
            "noobaa-operator-sigkill-3x",
            "noobaa-operator-sigterm-3x",
            "noobaa-endpoint-sigkill-3x",
            "noobaa-endpoint-sigterm-3x",
        ],
    )
    @polarion_id("OCS-7345")
    def test_krkn_noobaa_component_container_kill(
        self,
        request,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        component,
        kill_signal,
        iterations,
    ):
        """
        Test individual NooBaa component container disruption with specific signals.

        This test targets specific NooBaa components (core, db, operator, endpoint)
        and kills their containers multiple times to validate component-specific
        resilience.

        Components:
        - noobaa-core: Main NooBaa service handling S3 operations
        - noobaa-db: PostgreSQL database storing NooBaa metadata
        - noobaa-operator: Operator managing NooBaa lifecycle
        - noobaa-endpoint: S3 endpoint service pods

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture providing Warp S3 workloads
            component: Component name (noobaa-core, noobaa-db, etc.)
            kill_signal: Kill signal to use
            iterations: Number of times to repeat the chaos
        """
        scenario_dir = krkn_scenario_directory

        # Map component names to their label selectors
        component_labels = {
            "noobaa-core": constants.NOOBAA_CORE_POD_LABEL,
            "noobaa-db": constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
            "noobaa-operator": constants.NOOBAA_OPERATOR_POD_LABEL,
            "noobaa-endpoint": constants.NOOBAA_ENDPOINT_POD_LABEL,
        }

        if component not in component_labels:
            raise ValueError(f"Unknown component: {component}")

        label_selector = component_labels[component]

        # Use helper function for standardized test start logging
        log_test_start(
            f"{component} container kill ({kill_signal})",
            component,
            component_name=f"{component}-{kill_signal.lower()}",
            signal=kill_signal,
            iterations=iterations,
        )

        # WORKLOAD SETUP
        log.info(
            f"Setting up Warp S3 workloads for {component} container chaos testing"
        )
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
            # COMPONENT-SPECIFIC CONTAINER KILL SCENARIO
            # =================================================================
            log.info(
                f"Creating {component} container kill configuration with {kill_signal} signal, "
                f"{iterations} iterations"
            )

            # Create single scenario for the specific component
            scenario = {
                "name": f"{component.replace('-', '_')}_{kill_signal.lower()}_kill",
                "namespace": "openshift-storage",
                "label_selector": label_selector,
                "container_name": "",  # Target all containers in the pod
                "kill_signal": kill_signal,
                "count": 1,
                "expected_recovery_time": 120,
                "description": f"{component} component",
            }

            log.info(
                f"Scenario: {scenario['name']}, "
                f"Label: {label_selector}, "
                f"Signal: {kill_signal}, "
                f"Iterations: {iterations}"
            )

            # Create container chaos scenario file
            scenario_file = ContainerScenarios.container_kill(
                scenario_dir=scenario_dir,
                scenarios=[scenario],
            )

            log.info(f"Created scenario file: {scenario_file}")

            # Create Krkn configuration
            config = KrknConfigGenerator()
            config.add_scenario("container_scenarios", scenario_file)

            # =================================================================
            # EXECUTION: Repeated chaos with specified iterations
            # =================================================================
            log.info(
                f"Executing {component} container chaos: {iterations} iterations with {kill_signal}"
            )

            # Configure with multiple iterations
            config.set_tunings(wait_duration=60, iterations=iterations)
            config.write_to_file(location=krkn_scenario_directory)

            # Execute Krkn
            krkn_runner = KrKnRunner(config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            log.info(f"{component} container chaos completed")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e,
                f"{component}-{kill_signal.lower()}",
                f"{component} container chaos ({kill_signal})",
            )
            raise
        except Exception as e:
            log.error(f"❌ {component} container chaos with {kill_signal} failed: {e}")
            raise

        # =================================================================
        # RESULTS ANALYSIS
        # =================================================================
        log.info(f"{component.upper()} CONTAINER CHAOS RESULTS ({kill_signal}):")

        # Analyze results
        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output,
                f"{component}-{kill_signal.lower()}",
                detail_level="detailed",
            )
        )

        overall_success_rate = (
            (successful_executed / total_executed * 100) if total_executed > 0 else 0
        )

        log.info(
            f"EXECUTION RESULTS: Component: {component}, "
            f"Signal: {kill_signal}, "
            f"Total iterations: {total_executed}, "
            f"Successful: {successful_executed}, "
            f"Failed: {failing_executed}, "
            f"Success rate: {overall_success_rate:.1f}%"
        )

        # Validate success rate (adjust based on component criticality)
        min_success_rates = {
            "noobaa-core": 70,  # Core service is critical
            "noobaa-db": 60,  # DB might need more recovery time
            "noobaa-operator": 80,  # Operator should be resilient
            "noobaa-endpoint": 75,  # Endpoints should recover quickly
        }
        min_success_rate = min_success_rates.get(component, 70)

        validator.validate_chaos_execution(
            total_executed,
            successful_executed,
            f"{component}-{kill_signal.lower()}",
            f"{component} container chaos ({kill_signal})",
        )

        analyzer.evaluate_chaos_success_rate(
            total_executed,
            successful_executed,
            f"{component}-{kill_signal.lower()}",
            f"{component} container chaos ({kill_signal})",
            min_success_rate,
        )

        # Final health check
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            None, f"{component} container chaos ({kill_signal})"
        )
        assert no_crashes, crash_details

        log.info(
            f"🏆 {component} container chaos testing with {kill_signal} completed successfully! "
            f"Executed {iterations} iterations with {overall_success_rate:.1f}% success rate."
        )

    @pytest.mark.parametrize(
        "stress_level,duration_multiplier,signals",
        [
            ("moderate", 2, ["SIGKILL", "SIGTERM"]),
            ("high", 3, ["SIGKILL", "SIGTERM", "SIGHUP"]),
            ("extreme", 4, ["SIGKILL", "SIGTERM", "SIGHUP", "SIGINT", "SIGQUIT"]),
        ],
        ids=[
            "moderate-2signals",
            "high-3signals",
            "extreme-5signals",
        ],
    )
    @polarion_id("OCS-7346")
    def test_krkn_noobaa_multi_signal_strength_test(
        self,
        request,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        stress_level,
        duration_multiplier,
        signals,
    ):
        """
        Comprehensive NooBaa strength testing with multiple kill signals.

        This test validates NooBaa resilience under stress by:
        - Testing multiple kill signals in sequence
        - Targeting all NooBaa components
        - Running extended duration tests
        - Continuous Warp S3 workload throughout

        Stress Levels:
        - MODERATE: 2 signals (SIGKILL, SIGTERM)
        - HIGH: 3 signals (SIGKILL, SIGTERM, SIGHUP)
        - EXTREME: 5 signals (SIGKILL, SIGTERM, SIGHUP, SIGINT, SIGQUIT)

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture providing Warp S3 workloads
            stress_level: Level of stress testing
            duration_multiplier: Multiplier for base duration
            signals: List of signals to test
        """
        scenario_dir = krkn_scenario_directory

        # Use helper function for standardized test start logging
        log_test_start(
            f"{stress_level.upper()} NooBaa multi-signal strength test",
            "all-noobaa-components",
            component_name=f"noobaa-multi-signal-{stress_level}",
            stress_level=stress_level.upper(),
            signals=", ".join(signals),
        )

        log.info(
            f"⚠️  {stress_level.upper()} STRENGTH TESTING: Multiple signals across all NooBaa components"
        )
        log.info(f"🔥 Signals to test: {', '.join(signals)}")
        log.info(f"⏱️  Duration multiplier: {duration_multiplier}x")

        # WORKLOAD SETUP
        log.info("Setting up Warp S3 workloads for multi-signal strength testing")
        workload_ops.setup_workloads()

        # Register finalizer for cleanup
        request.addfinalizer(lambda: workload_ops.validate_and_cleanup())

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()
        scenario_helper = ContainerScenarioHelper()

        # Collect results from all signal tests
        all_results = []

        try:
            # =================================================================
            # MULTI-SIGNAL STRENGTH TEST
            # =================================================================
            for signal in signals:
                log.info("=" * 80)
                log.info(f"Testing with signal: {signal}")
                log.info("=" * 80)

                # NooBaa components to test
                noobaa_components = [
                    {"name": "noobaa", "description": "NooBaa (All Components)"},
                ]

                # Build scenarios for this signal
                noobaa_scenarios = scenario_helper.build_unified_scenarios(
                    namespace="openshift-storage",
                    kill_signal=signal,
                    count=1,
                    expected_recovery_time=120,
                    container_name="",
                    components=noobaa_components,
                )

                # Create scenario file
                scenario_file = ContainerScenarios.container_kill(
                    scenario_dir=scenario_dir,
                    scenarios=noobaa_scenarios,
                )

                # Create Krkn configuration
                config = KrknConfigGenerator()
                config.add_scenario("container_scenarios", scenario_file)

                # Number of iterations based on stress level
                base_iterations = 2
                iterations = base_iterations * duration_multiplier

                log.info(
                    f"Executing {iterations} iterations of NooBaa container kill with {signal}"
                )

                # Configure and execute
                config.set_tunings(wait_duration=60, iterations=iterations)
                config.write_to_file(location=krkn_scenario_directory)

                # Execute Krkn
                krkn_runner = KrKnRunner(config.global_config)
                krkn_runner.run_async()
                krkn_runner.wait_for_completion(check_interval=60)
                chaos_output = krkn_runner.get_chaos_data()

                log.info(f"Completed testing with {signal}")

                # Analyze results for this signal
                total_executed, successful_executed, failing_executed = (
                    analyzer.analyze_chaos_results(
                        chaos_output,
                        f"noobaa-{signal.lower()}",
                        detail_level="detailed",
                    )
                )

                signal_success_rate = (
                    (successful_executed / total_executed * 100)
                    if total_executed > 0
                    else 0
                )

                all_results.append(
                    {
                        "signal": signal,
                        "total": total_executed,
                        "successful": successful_executed,
                        "failed": failing_executed,
                        "success_rate": signal_success_rate,
                    }
                )

                log.info(
                    f"Signal {signal} results: {successful_executed}/{total_executed} "
                    f"({signal_success_rate:.1f}% success)"
                )

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e,
                f"noobaa-multi-signal-{stress_level}",
                f"NooBaa multi-signal strength test ({stress_level})",
            )
            raise
        except Exception as e:
            log.error(
                f"❌ NooBaa multi-signal strength test ({stress_level}) failed: {e}"
            )
            raise

        # =================================================================
        # COMPREHENSIVE RESULTS ANALYSIS
        # =================================================================
        log.info("=" * 80)
        log.info(f"MULTI-SIGNAL STRENGTH TEST RESULTS ({stress_level.upper()})")
        log.info("=" * 80)

        # Calculate overall statistics
        total_all_signals = sum(r["total"] for r in all_results)
        successful_all_signals = sum(r["successful"] for r in all_results)
        overall_success_rate = (
            (successful_all_signals / total_all_signals * 100)
            if total_all_signals > 0
            else 0
        )

        log.info("Overall Statistics:")
        log.info(f"  Total scenarios across all signals: {total_all_signals}")
        log.info(f"  Successful scenarios: {successful_all_signals}")
        log.info(f"  Overall success rate: {overall_success_rate:.1f}%")
        log.info("")
        log.info("Per-Signal Breakdown:")

        for result in all_results:
            log.info(
                f"  {result['signal']:10s}: {result['successful']:3d}/{result['total']:3d} "
                f"({result['success_rate']:5.1f}%) - "
                f"{'✅ PASS' if result['success_rate'] >= 60 else '⚠️  MARGINAL'}"
            )

        # Validate overall success rate based on stress level
        min_success_rates = {
            "moderate": 75,
            "high": 65,
            "extreme": 55,
        }
        min_success_rate = min_success_rates.get(stress_level, 60)

        log.info(f"Minimum required success rate: {min_success_rate}%")

        validator.validate_chaos_execution(
            total_all_signals,
            successful_all_signals,
            f"noobaa-multi-signal-{stress_level}",
            f"NooBaa multi-signal strength test ({stress_level})",
        )

        analyzer.evaluate_chaos_success_rate(
            total_all_signals,
            successful_all_signals,
            f"noobaa-multi-signal-{stress_level}",
            f"NooBaa multi-signal strength test ({stress_level})",
            min_success_rate,
        )

        # Final health check
        log.info("Performing final NooBaa health validation")
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            None, f"NooBaa multi-signal strength test ({stress_level})"
        )
        assert no_crashes, crash_details

        log.info(
            f"🏆 NooBaa multi-signal strength test ({stress_level.upper()}) completed successfully! "
            f"Tested {len(signals)} signals with {overall_success_rate:.1f}% overall success rate. "
            f"Total scenarios executed: {total_all_signals}"
        )
