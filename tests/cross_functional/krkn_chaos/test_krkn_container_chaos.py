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
from ocs_ci.ocs.constants import (
    OSD_APP_LABEL,
    RGW_APP_LABEL,
    CEPHFS_NODEPLUGIN_LABEL,
    RBD_NODEPLUGIN_LABEL,
)
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.krkn_chaos.krkn_helpers import (
    ContainerScenarioHelper,  # Class-based approach for container scenarios
    KrknResultAnalyzer,  # Result analysis helper
    CephHealthHelper,  # Ceph health helper
    ValidationHelper,  # Validation helper
    InstanceDetectionHelper,  # Instance detection helper
)
from ocs_ci.krkn_chaos.logging_helpers import log_test_start

log = logging.getLogger(__name__)


@green_squad
@chaos
@polarion_id("OCS-1240")
class TestKrKnContainerChaosScenarios:
    """
    Test suite for unified multi-stress container chaos scenarios.

    Contains a single comprehensive test that creates ONE Krkn configuration
    with multiple stress levels (Basic, Strength, Maximum) and executes them
    together in a unified chaos run.
    """

    @pytest.mark.parametrize(
        "ceph_component_label,component_name",
        [
            (
                OSD_APP_LABEL,
                "osd",
            ),  # OSDs - most resilient, good for multi-stress testing
            (RGW_APP_LABEL, "rgw"),  # RGW - HA design, handles multi-stress well
            (
                CEPHFS_NODEPLUGIN_LABEL,
                "cephfs-nodeplugin",
            ),  # Node plugins - distributed, good for escalation
            (
                RBD_NODEPLUGIN_LABEL,
                "rbd-nodeplugin",
            ),  # Node plugins - distributed, good for escalation
        ],
        ids=[
            "osd-multi-stress-chaos",
            "rgw-multi-stress-chaos",
            "cephfs-nodeplugin-multi-stress-chaos",
            "rbd-nodeplugin-multi-stress-chaos",
        ],
    )
    def test_krkn_container_multi_stress_chaos(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        component_name,
    ):
        """
        Multi-stress level container chaos testing with unified Krkn configuration.

        This test creates a SINGLE Krkn configuration file containing ALL stress levels
        and executes them together in one unified chaos run:

        🔄 **Multi-Stress Level Configuration:**
        1. **BASIC CHAOS**: Conservative scenarios with safety controls
        2. **STRENGTH TESTING**: Moderate stress with multiple patterns
        3. **MAXIMUM CHAOS**: Ultimate resilience testing scenarios

        All scenarios are configured in ONE Krkn config and executed simultaneously!

        🎯 **Key Benefits:**
        - **Single Krkn execution**: All stress levels in one run
        - **Unified configuration**: One config file with multiple scenario types
        - **Concurrent execution**: Krkn handles all scenarios together
        - **Comprehensive analysis**: Complete stress spectrum in single test
        - **Maximum efficiency**: Optimal resource utilization

        🎯 **Component Selection:**
        This test focuses on resilient components (OSD, RGW, Node Plugins) for multi-stress chaos.
        Critical components (MON, MGR, MDS, Controllers, Rook) get careful chaos in separate tests.
        """
        scenario_dir = krkn_scenario_directory

        # 🔍 DYNAMIC INSTANCE DETECTION
        instance_helper = InstanceDetectionHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        instance_count, pod_names = instance_helper.detect_instances_or_skip(
            ceph_component_label, component_name
        )

        # 🧠 COMPONENT ANALYSIS
        container_helper = ContainerScenarioHelper(
            scenario_dir=scenario_dir, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        # Use helper function for standardized test start logging
        log_test_start(
            "MULTI-STRESS container",
            component_name,
            component_name=component_name,
            instance_count=instance_count,
            safety_info="RESILIENT (suitable for multi-stress testing)",
        )

        # 🏗️ WORKLOAD SETUP
        log.info("🏗️ Setting up workloads for multi-stress testing...")
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
                "⚙️ Creating UNIFIED Krkn configuration with multiple stress levels..."
            )

            # Create a unified Krkn configuration
            unified_config = KrknConfigGenerator()

            # =================================================================
            # LEVEL 1: BASIC CHAOS SCENARIOS
            # =================================================================
            log.info("🟢 Adding BASIC CHAOS scenarios to unified config...")

            basic_settings = container_helper.get_component_settings(
                component_name, instance_count
            )
            log.info(f"   • Basic target instances: {basic_settings['instance_count']}")
            log.info(f"   • Basic kill signal: {basic_settings['kill_signal']}")

            # Create and add basic scenarios
            basic_scenarios = container_helper.create_basic_container_scenarios(
                label_selector=ceph_component_label, settings=basic_settings
            )

            for scenario in basic_scenarios:
                unified_config.add_scenario("container_scenarios", scenario)

            log.info(f"   ✅ Added {len(basic_scenarios)} BASIC chaos scenarios")

            # =================================================================
            # LEVEL 2: STRENGTH TESTING SCENARIOS
            # =================================================================
            log.info("🟡 Adding STRENGTH TESTING scenarios to unified config...")

            strength_multiplier = 3
            strength_pause_multiplier = 4
            strength_settings = container_helper.get_component_settings(
                component_name,
                instance_count,
                duration_multiplier=strength_multiplier,
                pause_multiplier=strength_pause_multiplier,
            )

            log.info(
                f"   • Strength target instances: {strength_settings['instance_count']}"
            )
            log.info(f"   • Duration multiplier: {strength_multiplier}x")
            log.info(f"   • Pause multiplier: {strength_pause_multiplier}x")

            # Create and add strength scenarios
            strength_scenarios = []

            # Pattern 1: Cascading kills
            cascading_scenario = container_helper.create_cascading_kill_scenario(
                label_selector=ceph_component_label, settings=strength_settings
            )
            strength_scenarios.append(cascading_scenario)
            unified_config.add_scenario("container_scenarios", cascading_scenario)

            # Pattern 2: Sustained pauses
            pause_scenario = container_helper.create_sustained_pause_scenario(
                label_selector=ceph_component_label, settings=strength_settings
            )
            strength_scenarios.append(pause_scenario)
            unified_config.add_scenario("container_scenarios", pause_scenario)

            log.info(
                f"   ✅ Added {len(strength_scenarios)} STRENGTH testing scenarios"
            )

            # =================================================================
            # LEVEL 3: MAXIMUM CHAOS SCENARIOS
            # =================================================================
            log.info("🔴 Adding MAXIMUM CHAOS scenarios to unified config...")

            max_target_instances = instance_count  # Target all instances
            max_settings = {
                "kill_signal": "SIGKILL",
                "pause_duration": 120,
                "instance_count": max_target_instances,
                "wait_duration": 300,
                "approach": "MAXIMUM_CHAOS",
            }

            log.info(f"   • Maximum target instances: ALL ({max_target_instances})")
            log.info(f"   • Maximum kill signal: {max_settings['kill_signal']}")

            # Create and add maximum chaos scenarios
            if container_helper.is_resilient_component(component_name):
                max_scenarios = container_helper.create_high_intensity_scenarios(
                    label_selector=ceph_component_label, settings=max_settings
                )
            else:
                # Fallback for edge cases
                max_scenarios = container_helper.create_basic_container_scenarios(
                    label_selector=ceph_component_label, settings=max_settings
                )

            for scenario in max_scenarios:
                unified_config.add_scenario("container_scenarios", scenario)

            log.info(f"   ✅ Added {len(max_scenarios)} MAXIMUM chaos scenarios")

            # =================================================================
            # UNIFIED EXECUTION: Single Krkn run with ALL scenarios
            # =================================================================
            total_scenarios = (
                len(basic_scenarios) + len(strength_scenarios) + len(max_scenarios)
            )
            log.info("🚀 EXECUTING unified multi-stress chaos configuration...")
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

            log.info("✅ Unified multi-stress chaos execution completed!")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e, component_name, "multi-stress container chaos"
            )
            raise
        except Exception as e:
            log.error(
                f"❌ Multi-stress container chaos failed for {component_name}: {e}"
            )
            raise
        finally:
            # Cleanup workloads
            workload_ops.validate_and_cleanup()

        # =================================================================
        # UNIFIED RESULTS ANALYSIS
        # =================================================================
        log.info("📊 MULTI-STRESS CHAOS RESULTS ANALYSIS:")
        log.info("=" * 60)

        # Analyze overall results from unified execution
        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output, component_name, detail_level="detailed"
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
        log.info(f"   🟡 Strength scenarios: {len(strength_scenarios)} configured")
        log.info(f"   🔴 Maximum scenarios: {len(max_scenarios)} configured")

        # Validate overall success rate
        min_success_rate = 70  # Multi-stress testing should maintain good success rate
        validator.validate_chaos_execution(
            total_executed,
            successful_executed,
            component_name,
            "multi-stress container chaos",
        )

        analyzer.evaluate_chaos_success_rate(
            total_executed,
            successful_executed,
            component_name,
            "multi-stress container chaos",
            min_success_rate,
        )

        # Final health check
        assert health_helper.check_ceph_crashes(
            ceph_component_label, "multi-stress container chaos"
        )

        log.info(
            f"🏆 Multi-stress container chaos testing for {component_name} "
            f"completed successfully!"
        )
        log.info(
            f"   Component handled ALL stress levels in unified execution "
            f"with {overall_success_rate:.1f}% success rate"
        )
        log.info(
            f"   ✨ Unified configuration approach: {total_scenarios} scenarios "
            f"in single Krkn run!"
        )
