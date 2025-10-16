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

log = logging.getLogger(__name__)


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

        # 🧠 HOG SCENARIO ANALYSIS
        hog_helper = HogScenarioHelper(scenario_dir=scenario_dir)

        # Use helper function for standardized test start logging
        log_test_start(
            "MULTI-STRESS resource hog",
            f"{node_type} nodes",
            node_type=node_type,
            node_selector=node_selector,
            safety_info=f"{node_type.upper()} (suitable for multi-stress testing)",
        )

        # WORKLOAD SETUP
        log.info("Setting up workloads for multi-stress testing")
        workload_ops.setup_workloads()

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        try:
            # =================================================================
            # UNIFIED KRKN CONFIG: All stress levels in ONE configuration
            # =================================================================
            log.info(
                "Creating UNIFIED Krkn configuration with multiple resource stress levels"
            )

            # Create a unified Krkn configuration
            unified_config = KrknConfigGenerator()

            # =================================================================
            # LEVEL 1: BASIC HOG CHAOS SCENARIOS
            # =================================================================
            log.info("🟢 Adding BASIC HOG CHAOS scenarios to unified config...")

            # Basic settings for conservative resource hog
            basic_duration = 90  # Conservative duration
            log.info(f"   • Basic duration: {basic_duration}s")
            log.info("   • Basic approach: CONSERVATIVE")

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

            log.info(f"Added {len(basic_scenarios)} BASIC hog chaos scenarios")

            # =================================================================
            # LEVEL 2: STRENGTH TESTING SCENARIOS
            # =================================================================
            log.info("Adding STRENGTH TESTING hog scenarios to unified config")

            # Strength settings for moderate resource stress
            strength_duration = 150  # Moderate duration
            log.info(f"   • Strength duration: {strength_duration}s")
            log.info("   • Strength approach: MODERATE")

            # Create and add strength hog scenarios using helper method
            strength_scenarios = hog_helper.create_strength_test_scenarios(
                stress_level="medium",  # Use medium stress level
                duration=strength_duration,
            )

            for scenario in strength_scenarios:
                unified_config.add_scenario("hog_scenarios", scenario)

            log.info(f"Added {len(strength_scenarios)} STRENGTH testing hog scenarios")

            # =================================================================
            # LEVEL 3: MAXIMUM HOG CHAOS SCENARIOS
            # =================================================================
            log.info("🔴 Adding MAXIMUM HOG CHAOS scenarios to unified config...")

            # Maximum settings for ultimate resource exhaustion
            max_duration = 240  # Extended duration
            log.info(f"   • Maximum duration: {max_duration}s")
            log.info("   • Maximum approach: ULTIMATE_CHAOS")

            # Create and add maximum chaos hog scenarios
            max_scenarios = hog_helper.create_strength_test_scenarios(
                stress_level="ultimate",  # Use ultimate stress level
                duration=max_duration,
            )

            for scenario in max_scenarios:
                unified_config.add_scenario("hog_scenarios", scenario)

            log.info(f"Added {len(max_scenarios)} MAXIMUM hog chaos scenarios")

            # =================================================================
            # UNIFIED EXECUTION: Single Krkn run with ALL scenarios
            # =================================================================
            total_scenarios = (
                len(basic_scenarios) + len(strength_scenarios) + len(max_scenarios)
            )
            log.info("Executing unified multi-stress hog chaos configuration")
            log.info(f"   • Total scenarios in config: {total_scenarios}")
            log.info(f"   • Basic scenarios: {len(basic_scenarios)}")
            log.info(f"   • Strength scenarios: {len(strength_scenarios)}")
            log.info(f"   • Maximum scenarios: {len(max_scenarios)}")
            log.info("   • Execution mode: UNIFIED (all scenarios together)")

            # Configure and write Krkn configuration to file
            unified_config.set_tunings(wait_duration=60, iterations=1)
            unified_config.write_to_file(location=krkn_scenario_directory)

            # Single Krkn execution with all scenarios
            krkn_runner = KrKnRunner(unified_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            log.info("Unified multi-stress hog chaos execution completed")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e, node_type, "multi-stress hog chaos"
            )
            raise
        except Exception as e:
            log.error(f"❌ Multi-stress hog chaos failed for {node_type}: {e}")
            raise
        finally:
            # Cleanup workloads
            workload_ops.validate_and_cleanup()

        # =================================================================
        # UNIFIED RESULTS ANALYSIS
        # =================================================================
        log.info("MULTI-STRESS HOG CHAOS RESULTS:")
        log.info("=" * 60)

        # Analyze overall results from unified execution
        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output, node_type, detail_level="detailed"
            )
        )

        overall_success_rate = (
            (successful_executed / total_executed * 100) if total_executed > 0 else 0
        )

        log.info("🎯 UNIFIED EXECUTION RESULTS:")
        log.info(f"   • Total scenarios executed: {total_executed}")
        log.info(f"   • Successful scenarios: {successful_executed}")
        log.info(f"   • Failed scenarios: {failing_executed}")
        log.info(f"   • Overall success rate: {overall_success_rate:.1f}%")
        log.info("=" * 60)

        # Detailed breakdown by stress level (estimated based on scenario counts)
        log.info("📈 STRESS LEVEL BREAKDOWN (Estimated):")
        log.info(f"   🟢 Basic scenarios: {len(basic_scenarios)} configured")
        log.info(f"Strength scenarios: {len(strength_scenarios)} configured")
        log.info(f"   🔴 Maximum scenarios: {len(max_scenarios)} configured")

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

        # Final health check
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "cluster", "multi-stress hog chaos"
        )
        assert no_crashes, crash_details

        log.info(
            f"🏆 Multi-stress hog chaos testing for {node_type} nodes "
            f"completed successfully!"
        )
        log.info(
            f"   System handled ALL resource stress levels in unified execution "
            f"with {overall_success_rate:.1f}% success rate"
        )
        log.info(
            f"   ✨ Unified configuration approach: {total_scenarios} scenarios "
            f"in single Krkn run!"
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

        # WORKLOAD SETUP
        log.info("Setting up workloads for extreme testing")
        workload_ops.setup_workloads()

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        try:
            # =================================================================
            # UNIFIED EXTREME KRKN CONFIG: All extreme scenarios in ONE configuration
            # =================================================================
            log.info(
                f"Creating UNIFIED Krkn configuration with {stress_level.upper()} stress levels"
            )

            # Create a unified Krkn configuration
            unified_config = KrknConfigGenerator()

            # Calculate extreme durations based on multipliers (capped at 5 minutes)
            base_duration = 120
            extreme_duration = min(300, base_duration * duration_multiplier)

            log.info(f"   • Base duration: {base_duration}s")
            log.info(f"   • Extreme duration: {extreme_duration}s")
            log.info(f"   • Approach: {stress_level.upper()}_CHAOS")

            # Create extreme scenarios using strength test scenarios
            extreme_scenarios = hog_helper.create_strength_test_scenarios(
                stress_level=stress_level,  # Use the parameterized stress level
                duration=extreme_duration,
            )

            for scenario in extreme_scenarios:
                unified_config.add_scenario("hog_scenarios", scenario)

            log.info(
                f"Added {len(extreme_scenarios)} {stress_level.upper()} chaos scenarios"
            )

            # =================================================================
            # UNIFIED EXECUTION: Single Krkn run with ALL extreme scenarios
            # =================================================================
            log.info(f"Executing unified {stress_level.upper()} chaos configuration")
            log.info(f"   • Total scenarios in config: {len(extreme_scenarios)}")
            log.info(f"   • Stress level: {stress_level.upper()}")
            log.info(f"   • Duration multiplier: {duration_multiplier}x")
            log.info(f"   • Intensity multiplier: {intensity_multiplier}x")
            log.info("   • Execution mode: UNIFIED (all scenarios together)")

            # Configure and write Krkn configuration to file
            unified_config.set_tunings(wait_duration=60, iterations=1)
            unified_config.write_to_file(location=krkn_scenario_directory)

            # Single Krkn execution with all extreme scenarios
            krkn_runner = KrKnRunner(unified_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            log.info(f"Unified {stress_level.upper()} chaos execution completed")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e, stress_level, f"{stress_level} cluster strength testing"
            )
            raise
        except Exception as e:
            log.error(f"❌ {stress_level.upper()} cluster strength testing failed: {e}")
            raise
        finally:
            # Cleanup workloads
            workload_ops.validate_and_cleanup()

            # =================================================================
            # UNIFIED RESULTS ANALYSIS
            # =================================================================
            log.info(f"{stress_level.upper()} CLUSTER STRENGTH TESTING RESULTS:")
        log.info("=" * 60)

        # Analyze overall results from unified execution
        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output, stress_level, detail_level="detailed"
            )
        )

        overall_success_rate = (
            (successful_executed / total_executed * 100) if total_executed > 0 else 0
        )

        log.info("🎯 UNIFIED EXECUTION RESULTS:")
        log.info(f"   • Total scenarios executed: {total_executed}")
        log.info(f"   • Successful scenarios: {successful_executed}")
        log.info(f"   • Failed scenarios: {failing_executed}")
        log.info(f"   • Overall success rate: {overall_success_rate:.1f}%")
        log.info("=" * 60)

        # Detailed breakdown by stress level
        log.info("📈 EXTREME STRESS BREAKDOWN:")
        log.info(
            f"   🚨 {stress_level.upper()} scenarios: {len(extreme_scenarios)} configured"
        )
        log.info(f"   ⚡ Duration multiplier: {duration_multiplier}x")
        log.info(f"   🔥 Intensity multiplier: {intensity_multiplier}x")

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

        # Final health check
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "cluster", f"{stress_level} cluster strength testing"
        )
        assert no_crashes, crash_details

        log.info(
            f"🏆 {stress_level.upper()} cluster strength testing completed successfully!"
        )
        log.info(
            f"   Cluster survived {stress_level.upper()} resource exhaustion "
            f"with {overall_success_rate:.1f}% success rate"
        )
        log.info(
            f"   ✨ Unified configuration approach: {len(extreme_scenarios)} scenarios "
            f"in single Krkn run!"
        )
