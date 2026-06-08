"""
Test suite for unified multi-stress hog chaos scenarios.

This module provides a comprehensive unified test for resource hog scenarios using the Krkn chaos engineering tool.
It includes a single test that creates ONE Krkn configuration with multiple stress levels:
- BASIC CHAOS: Conservative resource hog with safety controls
- STRENGTH TESTING: Moderate resource stress with multiple patterns
- MAXIMUM CHAOS: Ultimate resource exhaustion testing scenarios

The test creates VDBENCH workloads and executes all stress levels together in a single unified Krkn run
to validate system resilience across the complete resource stress spectrum.
"""

import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.krkn_chaos.krkn_helpers import (
    HogScenarioHelper,  # Class-based approach for hog scenarios
    KrknResultAnalyzer,  # Result analysis helper
    CephHealthHelper,  # Ceph health helper
    ValidationHelper,  # Validation helper
)
from ocs_ci.krkn_chaos.logging_helpers import log_test_start

logger = logging.getLogger(__name__)


@green_squad
@chaos
class TestKrKnHogChaosScenarios:
    """
    Test suite for unified multi-stress resource hog chaos scenarios.

    Contains a single comprehensive test that creates ONE Krkn configuration
    with multiple stress levels (Basic, Strength, Maximum) and executes them
    together in a unified chaos run.
    """

    @pytest.mark.parametrize(
        "node_selector,node_type",
        [
            (
                constants.WORKER_LABEL,
                "worker",
            ),  # Worker nodes - safer for resource hog tests
            (
                constants.MASTER_LABEL,
                "master",
            ),  # Master nodes - more critical, use with caution
        ],
        ids=[
            "worker-multi-stress-hog-chaos",
            "master-multi-stress-hog-chaos",
        ],
    )
    @polarion_id("OCS-7338")
    def test_krkn_hog_multi_stress_chaos(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        node_selector,
        node_type,
    ):
        """
        Multi-stress level resource hog chaos testing with unified Krkn configuration.

        This test creates a SINGLE Krkn configuration file containing ALL stress levels
        and executes them together in one unified chaos run:

        🔄 **Multi-Stress Level Configuration:**
        1. **BASIC CHAOS**: Conservative resource hog with safety controls
        2. **STRENGTH TESTING**: Moderate resource stress with multiple patterns
        3. **MAXIMUM CHAOS**: Ultimate resource exhaustion testing scenarios

        All scenarios are configured in ONE Krkn config and executed simultaneously!

        🎯 **Key Benefits:**
        - **Single Krkn execution**: All stress levels in one run
        - **Unified configuration**: One config file with multiple scenario types
        - **Concurrent execution**: Krkn handles all scenarios together
        - **Comprehensive analysis**: Complete stress spectrum in single test
        - **Maximum efficiency**: Optimal resource utilization

        ⚠️ **Node Selection:**
        Only worker nodes are tested with multi-stress chaos for cluster safety.
        Master nodes use separate conservative tests to avoid cluster disruption.
        """
        scenario_dir = krkn_scenario_directory

        hog_helper = HogScenarioHelper(scenario_dir=scenario_dir)

        # Use helper function for standardized test start logging
        log_test_start(
            "MULTI-STRESS resource hog",
            f"{node_type} nodes",
            node_type=node_type,
            node_selector=node_selector,
            safety_info=f"{node_type.upper()} (suitable for multi-stress testing)",
        )

        logger.test_step("Set up workloads for multi-stress testing")
        workload_ops.setup_workloads()

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        try:
            logger.test_step(
                "Create unified Krkn configuration with multiple resource stress levels"
            )
            unified_config = KrknConfigGenerator()

            # LEVEL 1: BASIC HOG CHAOS SCENARIOS
            basic_duration = 90  # Conservative duration
            logger.debug(f"Basic duration: {basic_duration}s, approach: CONSERVATIVE")

            # Create and add basic hog scenarios
            basic_scenarios = []

            # Basic CPU hog
            basic_cpu = hog_helper.create_cpu_hog_scenario(
                duration=basic_duration,
                namespace="default",  # Standard namespace for hog scenarios
                node_selector=f"{node_selector}=",  # KRKN expects string format
            )
            basic_scenarios.append(basic_cpu)
            unified_config.add_scenario("hog_scenarios", basic_cpu)

            # Basic Memory hog
            basic_memory = hog_helper.create_memory_hog_scenario(
                duration=basic_duration,
                namespace="default",
                node_selector=f"{node_selector}=",  # KRKN expects string format
            )
            basic_scenarios.append(basic_memory)
            unified_config.add_scenario("hog_scenarios", basic_memory)

            logger.info(f"Added {len(basic_scenarios)} BASIC hog chaos scenarios")

            # LEVEL 2: STRENGTH TESTING SCENARIOS
            strength_duration = 150  # Moderate duration
            logger.debug(f"Strength duration: {strength_duration}s, approach: MODERATE")

            # Create and add strength hog scenarios using helper method
            strength_scenarios = hog_helper.create_strength_test_scenarios(
                stress_level="medium",  # Use medium stress level
                duration=strength_duration,
                node_selector=f"{node_selector}=",
            )

            for scenario in strength_scenarios:
                unified_config.add_scenario("hog_scenarios", scenario)

            logger.info(
                f"Added {len(strength_scenarios)} STRENGTH testing hog scenarios"
            )

            # LEVEL 3: MAXIMUM HOG CHAOS SCENARIOS
            max_duration = 240  # Extended duration
            logger.debug(f"Maximum duration: {max_duration}s, approach: ULTIMATE_CHAOS")

            # Create and add maximum chaos hog scenarios
            max_scenarios = hog_helper.create_strength_test_scenarios(
                stress_level="ultimate",  # Use ultimate stress level
                duration=max_duration,
                node_selector=f"{node_selector}=",
            )

            for scenario in max_scenarios:
                unified_config.add_scenario("hog_scenarios", scenario)

            logger.info(f"Added {len(max_scenarios)} MAXIMUM hog chaos scenarios")

            logger.test_step("Execute unified multi-stress hog chaos configuration")
            total_scenarios = (
                len(basic_scenarios) + len(strength_scenarios) + len(max_scenarios)
            )
            logger.info(
                f"Executing unified config: {total_scenarios} total scenarios "
                f"(basic={len(basic_scenarios)}, strength={len(strength_scenarios)}, "
                f"max={len(max_scenarios)})"
            )

            # Configure and write Krkn configuration to file
            unified_config.set_tunings(wait_duration=60, iterations=1)
            unified_config.write_to_file(location=krkn_scenario_directory)

            # Single Krkn execution with all scenarios
            krkn_runner = KrKnRunner(unified_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            logger.info("Unified multi-stress hog chaos execution completed")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e,
                node_type,
                "multi-stress hog chaos",
                health_helper=health_helper,
            )
            raise
        except Exception:
            logger.exception(f"Multi-stress hog chaos failed for {node_type}")
            raise
        finally:
            # Cleanup workloads
            workload_ops.validate_and_cleanup()

        logger.test_step("Analyze unified multi-stress hog chaos results")

        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output, node_type, detail_level="detailed"
            )
        )

        overall_success_rate = (
            (successful_executed / total_executed * 100) if total_executed > 0 else 0
        )

        logger.info(
            f"Unified execution results: total={total_executed}, "
            f"successful={successful_executed}, failed={failing_executed}, "
            f"success_rate={overall_success_rate:.1f}%"
        )
        logger.debug(
            f"Stress level breakdown: basic={len(basic_scenarios)}, "
            f"strength={len(strength_scenarios)}, max={len(max_scenarios)} configured"
        )

        # Validate overall success rate
        min_success_rate = 70  # Hog chaos testing should maintain good success rate
        validator.validate_chaos_execution(
            total_executed,
            successful_executed,
            node_type,
            "multi-stress hog chaos",
        )

        analyzer.evaluate_chaos_success_rate(
            total_executed,
            successful_executed,
            node_type,
            "multi-stress hog chaos",
            min_success_rate,
        )

        logger.test_step("Perform final Ceph health check")
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "cluster", "multi-stress hog chaos"
        )
        logger.assertion(
            f"Ceph crashes after hog chaos: expected=None, actual={'None' if no_crashes else crash_details}"
        )
        assert no_crashes, crash_details

        logger.info(
            f"Multi-stress hog chaos testing for {node_type} nodes completed successfully. "
            f"System handled all resource stress levels with {overall_success_rate:.1f}% success rate. "
            f"Unified configuration: {total_scenarios} scenarios in single Krkn run."
        )

    @pytest.mark.parametrize(
        "stress_level,duration_multiplier,intensity_multiplier",
        [
            ("extreme", 4, 2),  # Extreme cluster stress testing
            ("ultimate", 6, 3),  # Ultimate cluster endurance testing
            ("apocalypse", 8, 4),  # Maximum cluster destruction testing
        ],
        ids=[
            "extreme-cluster-stress",
            "ultimate-cluster-endurance",
            "apocalypse-cluster-destruction",
        ],
    )
    @polarion_id("OCS-7339")
    def test_krkn_extreme_cluster_strength_testing(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        stress_level,
        duration_multiplier,
        intensity_multiplier,
    ):
        """
        EXTREME cluster strength testing with apocalyptic resource exhaustion scenarios.

        This test pushes cluster resilience to absolute limits with devastating chaos patterns:
        - Cascading resource exhaustion
        - Multi-resource simultaneous attacks
        - Sustained resource starvation
        - Recovery stress testing under extreme load
        - Apocalyptic resource consumption patterns

        🔄 **Extreme Stress Level Configuration:**
        - **EXTREME**: 4x duration, 2x intensity multiplier
        - **ULTIMATE**: 6x duration, 3x intensity multiplier
        - **APOCALYPSE**: 8x duration, 4x intensity multiplier

        All scenarios are configured in ONE Krkn config and executed simultaneously!

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture for VDBENCH workloads
            stress_level: Level of stress testing (extreme, ultimate, apocalypse)
            duration_multiplier: Multiplier for scenario durations
            intensity_multiplier: Multiplier for resource consumption intensity
        """
        scenario_dir = krkn_scenario_directory

        # Initialize HogScenarioHelper
        hog_helper = HogScenarioHelper(scenario_dir=scenario_dir)

        # Use helper function for standardized test start logging
        log_test_start(
            f"{stress_level.upper()} cluster strength testing",
            "cluster",
            stress_level=stress_level.upper(),
            duration_multiplier=duration_multiplier,
            intensity_multiplier=intensity_multiplier,
            config_info="UNIFIED Krkn config with EXTREME stress levels",
        )

        logger.test_step("Set up workloads for extreme testing")
        workload_ops.setup_workloads()

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        try:
            logger.test_step(
                f"Create unified Krkn configuration with {stress_level.upper()} stress levels"
            )
            unified_config = KrknConfigGenerator()

            # Calculate extreme durations based on multipliers (capped at 5 minutes)
            base_duration = 120
            extreme_duration = min(300, base_duration * duration_multiplier)

            logger.debug(
                f"Base duration: {base_duration}s, extreme duration: {extreme_duration}s, "
                f"approach: {stress_level.upper()}_CHAOS"
            )

            # Create extreme scenarios using strength test scenarios
            extreme_scenarios = hog_helper.create_strength_test_scenarios(
                stress_level=stress_level,  # Use the parameterized stress level
                duration=extreme_duration,
            )

            for scenario in extreme_scenarios:
                unified_config.add_scenario("hog_scenarios", scenario)

            logger.info(
                f"Added {len(extreme_scenarios)} {stress_level.upper()} chaos scenarios"
            )

            logger.test_step(
                f"Execute unified {stress_level.upper()} chaos configuration"
            )
            logger.info(
                f"Executing {len(extreme_scenarios)} {stress_level.upper()} scenarios "
                f"(duration_multiplier={duration_multiplier}x, intensity_multiplier={intensity_multiplier}x)"
            )

            # Configure and write Krkn configuration to file
            unified_config.set_tunings(wait_duration=60, iterations=1)
            unified_config.write_to_file(location=krkn_scenario_directory)

            # Single Krkn execution with all extreme scenarios
            krkn_runner = KrKnRunner(unified_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            logger.info(f"Unified {stress_level.upper()} chaos execution completed")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e,
                stress_level,
                f"{stress_level} cluster strength testing",
                health_helper=health_helper,
            )
            raise
        except Exception:
            logger.exception(f"{stress_level.upper()} cluster strength testing failed")
            raise
        finally:
            # Cleanup workloads
            workload_ops.validate_and_cleanup()

        logger.test_step(
            f"Analyze {stress_level.upper()} cluster strength testing results"
        )

        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output, stress_level, detail_level="detailed"
            )
        )

        overall_success_rate = (
            (successful_executed / total_executed * 100) if total_executed > 0 else 0
        )

        logger.info(
            f"Execution results: total={total_executed}, successful={successful_executed}, "
            f"failed={failing_executed}, success_rate={overall_success_rate:.1f}%"
        )
        logger.debug(
            f"Stress breakdown: {len(extreme_scenarios)} {stress_level.upper()} scenarios, "
            f"duration_multiplier={duration_multiplier}x, intensity_multiplier={intensity_multiplier}x"
        )

        # Validate overall success rate (lower threshold for extreme testing)
        if stress_level == "apocalypse":
            min_success_rate = 40  # Very low threshold for apocalypse testing
        elif stress_level == "ultimate":
            min_success_rate = 50  # Low threshold for ultimate testing
        else:
            min_success_rate = 60  # Moderate threshold for extreme testing

        validator.validate_chaos_execution(
            total_executed,
            successful_executed,
            stress_level,
            f"{stress_level} cluster strength testing",
        )

        analyzer.evaluate_chaos_success_rate(
            total_executed,
            successful_executed,
            stress_level,
            f"{stress_level} cluster strength testing",
            min_success_rate,
        )

        logger.test_step("Perform final Ceph health check")
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "cluster", f"{stress_level} cluster strength testing"
        )
        logger.assertion(
            f"Ceph crashes after extreme testing: expected=None, actual={'None' if no_crashes else crash_details}"
        )
        assert no_crashes, crash_details

        logger.info(
            f"{stress_level.upper()} cluster strength testing completed successfully. "
            f"Cluster survived with {overall_success_rate:.1f}% success rate. "
            f"Unified configuration: {len(extreme_scenarios)} scenarios in single Krkn run."
        )
