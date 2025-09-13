"""
Test suite for Krkn container chaos scenarios.

This module provides comprehensive tests for container chaos scenarios using the Krkn chaos engineering tool.
It includes tests for:
- Container kill scenarios with different signals (SIGKILL, SIGTERM)
- Container pause scenarios to simulate temporary hangs
- Targeted container chaos for different Ceph components

The tests create VDBENCH workloads and inject container-level failures to validate system resilience.
"""

import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import (
    MON_APP_LABEL,
    MDS_APP_LABEL,
    MGR_APP_LABEL,
    OSD_APP_LABEL,
    RGW_APP_LABEL,
    CEPHFS_NODEPLUGIN_LABEL,
    RBD_NODEPLUGIN_LABEL,
    CEPHFS_CTRLPLUGIN_LABEL,
    RBD_CTRLPLUGIN_LABEL,
    ROOK_OPERATOR_PODS,
)
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.krkn_chaos.krkn_scenario_generator import ContainerScenarios
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.krkn_chaos.krkn_helpers import (
    ContainerComponentConfig,
    create_basic_container_scenarios,
    check_ceph_crashes,
    evaluate_chaos_success_rate,
    validate_chaos_execution,
    validate_strength_test_results,
    handle_krkn_command_failure,
    handle_workload_validation_failure,
    analyze_chaos_results,
    analyze_strength_test_results,
    detect_instances_or_skip,
)

log = logging.getLogger(__name__)


@green_squad
@chaos
@polarion_id("OCS-1240")
class TestKrKnContainerChaosScenarios:
    """
    Test suite for Krkn container chaos scenarios with organized helper methods.
    """

    @pytest.mark.parametrize(
        "ceph_component_label,component_name",
        [
            (OSD_APP_LABEL, "osd"),  # OSDs can handle container restarts
            (MGR_APP_LABEL, "mgr"),  # Critical: active/standby pair - conservative
            (MON_APP_LABEL, "mon"),  # Critical: NEVER >1 (breaks quorum)
            (MDS_APP_LABEL, "mds"),  # Critical: usually 1-2 active - conservative
            (RGW_APP_LABEL, "rgw"),  # HA design: multiple gateways expected
            (
                CEPHFS_NODEPLUGIN_LABEL,
                "cephfs-nodeplugin",
            ),  # Node plugins are resilient
            (RBD_NODEPLUGIN_LABEL, "rbd-nodeplugin"),  # Node plugins are resilient
            (
                CEPHFS_CTRLPLUGIN_LABEL,
                "cephfs-ctrlplugin",
            ),  # Critical: controller plugins
            (RBD_CTRLPLUGIN_LABEL, "rbd-ctrlplugin"),  # Critical: controller plugins
            (ROOK_OPERATOR_PODS, "rook-operator"),  # Critical: cluster operator
        ],
        ids=[
            "osd-container-chaos",
            "mgr-container-chaos",
            "mon-container-chaos",
            "mds-container-chaos",
            "rgw-container-chaos",
            "cephfs-nodeplugin-container-chaos",
            "rbd-nodeplugin-container-chaos",
            "cephfs-ctrlplugin-container-chaos",
            "rbd-ctrlplugin-container-chaos",
            "rook-operator-container-chaos",
        ],
    )
    def test_krkn_container_chaos(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        component_name,
    ):
        """
        Comprehensive container chaos testing with intelligent safety controls.

        This test provides comprehensive coverage of all ODF components with intelligent
        component-aware configurations that ensure cluster stability while testing resilience.

        Components tested (with safety controls):
        - Critical Components: MON, MGR, MDS, CSI Controllers, Rook Operator (conservative chaos)
        - Resilient Components: OSD, RGW, CSI Node Plugins (moderate chaos)

        The test automatically detects available instances and applies appropriate chaos
        intensity based on component criticality to maintain cluster health.
        """
        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        # 🔍 DYNAMIC INSTANCE DETECTION: Get all available pod instances - one-liner
        instance_count, pod_names = detect_instances_or_skip(
            ceph_component_label, component_name
        )

        # 🧠 INTELLIGENT CONFIGURATION: Get component-specific settings with dynamic count
        settings = ContainerComponentConfig.get_component_settings(
            component_name, instance_count
        )
        is_critical = ContainerComponentConfig.is_critical(component_name)

        log.info(f"🎯 Testing container chaos for {component_name}")
        log.info("📊 Component Configuration:")
        log.info(f"   • Component: {component_name}")
        log.info(f"   • Available instances: {instance_count}")
        log.info(f"   • Target instances: {settings['instance_count']}")
        log.info(f"   • Criticality: {'CRITICAL' if is_critical else 'RESILIENT'}")
        log.info(f"   • Approach: {settings['approach']}")
        log.info(f"   • Kill signal: {settings['kill_signal']}")
        log.info(f"   • Pause duration: {settings['pause_duration']}s")

        # Map Ceph component labels to their container names
        container_name_mapping = {
            "app=rook-ceph-osd": "osd",
            "app=rook-ceph-mon": "mon",
            "app=rook-ceph-mgr": "mgr",
            "app=rook-ceph-mds": "mds",
            "app=rook-ceph-rgw": "rgw",
        }

        # Get the specific container name for this Ceph component
        container_name = container_name_mapping.get(ceph_component_label, "")
        log.info(
            f"Targeting container '{container_name}' in pods with label '{ceph_component_label}'"
        )

        # Create container chaos scenarios targeting the specific Ceph component
        component_name = ceph_component_label.split("=")[1].split("-")[
            -1
        ]  # Extract component name

        # Determine if this is a critical component for safety controls
        is_critical = component_name in ["mon", "mgr", "mds"]

        # 🎭 SCENARIO GENERATION: Create component-appropriate chaos scenarios
        scenarios = create_basic_container_scenarios(
            scenario_dir, openshift_storage_ns, ceph_component_label, settings
        )

        # Add high-intensity scenarios for non-critical components
        if not is_critical:
            # These scenarios are only safe for less critical components (OSD, RGW)
            additional_scenarios = [
                # 🌪️ CHAOS STORM: Multiple rapid kills
                ContainerScenarios.container_kill(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=settings["instance_count"],
                    container_name=container_name,
                    kill_signal="SIGKILL",
                    wait_duration=200,  # Very short wait
                ),
                ContainerScenarios.container_kill(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=max(1, settings["instance_count"] // 2),
                    container_name=container_name,
                    kill_signal="SIGKILL",
                    wait_duration=250,  # Another rapid burst
                ),
                # 💀 ENDURANCE PAUSE: Ultra-long container suspension
                ContainerScenarios.container_pause(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=1,  # Conservative instance count for long pause
                    container_name=container_name,
                    pause_seconds=settings["pause_duration"] * 4,  # 4x longer pause
                    wait_duration=1200,  # Extended recovery time
                ),
                # 🚨 MIXED CHAOS: Alternating kill and pause
                ContainerScenarios.container_kill(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=max(1, settings["instance_count"] // 2),
                    container_name=container_name,
                    kill_signal="SIGTERM",
                    wait_duration=300,
                ),
                ContainerScenarios.container_pause(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=1,
                    container_name=container_name,
                    pause_seconds=settings["pause_duration"] * 3,
                    wait_duration=600,
                ),
            ]
            scenarios.extend(additional_scenarios)
            log.info(
                f"💪 Added {len(additional_scenarios)} maximum chaos scenarios for resilient component"
            )

        log.info(
            f"Generated {len(scenarios)} container chaos scenarios for {component_name}"
        )

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("container_scenarios", scenario)
        config.set_tunings(wait_duration=60, iterations=1)
        config.write_to_file(location=scenario_dir)

        # Execute Krkn chaos scenarios
        krkn = KrKnRunner(config.global_config)
        try:
            log.info(f"Starting container chaos test for {ceph_component_label}")
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(f"Container chaos test completed for {ceph_component_label}")
        except CommandFailed as e:
            handle_krkn_command_failure(e, ceph_component_label, "container chaos")

        # Validate workloads and cleanup
        try:
            workload_ops.validate_and_cleanup()
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(
                f"Workload validation/cleanup issue for {ceph_component_label}: {str(e)}"
            )

        # Analyze chaos run results using helper function
        results = analyze_chaos_results(krkn, ceph_component_label, "container chaos")
        total_scenarios = results["total_scenarios"]
        successful_scenarios = results["successful_scenarios"]

        # Additional logging handled by analyze_chaos_results helper

        # Validate chaos execution results
        validate_chaos_execution(
            total_scenarios,
            successful_scenarios,
            ceph_component_label,
            "container chaos",
        )

        # Check for Ceph crashes after chaos injection
        assert check_ceph_crashes(ceph_component_label, "container chaos")

        log.info(
            f"Container chaos test for {ceph_component_label} completed successfully"
        )

    @pytest.mark.parametrize(
        "ceph_component_label,component_name,stress_level,duration_multiplier,pause_multiplier",
        [
            (
                OSD_APP_LABEL,
                "osd",
                "ultimate",
                5,
                6,
            ),  # Ultimate OSD container stress test - highest intensity
            (
                RGW_APP_LABEL,
                "rgw",
                "high",
                2,
                3,
            ),  # RGWs are resilient but more conservative
            (
                CEPHFS_NODEPLUGIN_LABEL,
                "cephfs-nodeplugin",
                "extreme",
                3,
                4,
            ),  # CephFS node plugins - consolidated extreme stress
            (
                RBD_NODEPLUGIN_LABEL,
                "rbd-nodeplugin",
                "extreme",
                3,
                4,
            ),  # RBD node plugins - consolidated extreme stress
        ],
        ids=[
            "osd-ultimate-container-stress",
            "rgw-high-container-stress",
            "cephfs-nodeplugin-extreme-container-stress",
            "rbd-nodeplugin-extreme-container-stress",
        ],
    )
    def test_krkn_container_strength_testing(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        component_name,
        stress_level,
        duration_multiplier,
        pause_multiplier,
    ):
        """
        Extreme container strength testing with multi-pattern chaos scenarios.

        This test pushes container resilience to the limits with various chaos patterns:
        - Cascading container kills
        - Sustained container pauses
        - Rapid-fire container disruptions
        - Mixed kill/pause patterns
        - Recovery stress testing

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture for VDBENCH workloads
            component_name: Component to target (osd, rgw)
            stress_level: Level of stress testing (high, extreme, ultimate)
            duration_multiplier: Multiplier for wait durations
            pause_multiplier: Multiplier for pause durations
        """
        log.info(
            f"Starting EXTREME container strength testing for {component_name} "
            f"with {stress_level} stress level (duration: {duration_multiplier}x, pause: {pause_multiplier}x)"
        )

        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        # 🔍 DYNAMIC INSTANCE DETECTION: Get all available pod instances
        # Detect instances or skip test - one-liner helper function
        instance_count, pod_names = detect_instances_or_skip(
            ceph_component_label, component_name
        )

        # 🧠 INTELLIGENT CONFIGURATION: Get component-specific settings with dynamic count
        settings = ContainerComponentConfig.get_component_settings(
            component_name, instance_count
        )
        is_critical = ContainerComponentConfig.is_critical(component_name)

        log.info("💪 Strength Testing Configuration:")
        log.info(f"   • Component: {component_name}")
        log.info(f"   • Available instances: {instance_count}")
        log.info(f"   • Target instances: {settings['instance_count']}")
        log.info(f"   • Criticality: {'CRITICAL' if is_critical else 'RESILIENT'}")
        log.info(f"   • Stress level: {stress_level}")
        log.info(f"   • Duration multiplier: {duration_multiplier}x")
        log.info(f"   • Pause multiplier: {pause_multiplier}x")

        # Map component names to container names for Ceph components
        container_name_mapping = {
            "osd": "osd",
            "mgr": "mgr",
            "mon": "mon",
            "mds": "mds",
            "rgw": "rgw",
            "cephfs-nodeplugin": "",  # CSI plugins don't need container name
            "rbd-nodeplugin": "",
            "cephfs-ctrlplugin": "",
            "rbd-ctrlplugin": "",
            "rook-operator": "",
        }
        container_name = container_name_mapping.get(component_name, "")

        # Base parameters scaled by stress level
        base_wait_duration = 300
        base_pause_duration = 90
        max_wait_duration = base_wait_duration * duration_multiplier
        max_pause_duration = base_pause_duration * pause_multiplier

        log.info(
            f"Creating {stress_level} container strength testing scenarios for {component_name}"
        )
        log.info(
            f"Maximum wait duration: {max_wait_duration}s, Maximum pause: {max_pause_duration}s"
        )

        # 🏗️ HIGH-IMPACT STRENGTH TESTING SCENARIOS ONLY
        scenarios = [
            # 🔥 MAXIMUM KILL: Ultimate container termination stress
            ContainerScenarios.container_kill(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=ceph_component_label,
                instance_count=settings["instance_count"],  # Use all target instances
                container_name=container_name,
                kill_signal="SIGKILL",  # Always SIGKILL for maximum impact
                wait_duration=120,  # Very short wait for extreme stress
            ),
            # 💀 EXTREME PAUSE: Maximum disruption pause scenario
            ContainerScenarios.container_pause(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=ceph_component_label,
                instance_count=settings["instance_count"],  # Use all target instances
                container_name=container_name,
                pause_seconds=max_pause_duration,  # Maximum pause duration
                wait_duration=180,  # Short wait for continuous stress
            ),
        ]

        log.info(
            f"Generated {len(scenarios)} container strength testing scenarios for {component_name} "
            f"({stress_level} level)"
        )

        # Generate Krkn configuration with extended wait times for strength testing
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("container_scenarios", scenario)

        # Longer wait duration for strength testing
        extended_wait = 90 if stress_level == "ultimate" else 60
        config.set_tunings(wait_duration=extended_wait, iterations=1)
        config.write_to_file(location=scenario_dir)

        log.info(
            f"Krkn container strength testing configuration written (wait_duration={extended_wait}s)"
        )

        # Execute Krkn chaos scenarios
        krkn = KrKnRunner(config.global_config)
        try:
            log.info(
                f"🚀 Starting {stress_level} container strength testing for {component_name}"
            )
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(
                f"✅ Container strength testing completed for {component_name} ({stress_level} level)"
            )
        except CommandFailed as e:
            handle_krkn_command_failure(e, component_name, "container strength testing")

        # Enhanced validation for strength testing
        try:
            workload_ops.validate_and_cleanup()
            log.info(
                "💪 Workloads survived container strength testing - container resilience confirmed!"
            )
        except (UnexpectedBehaviour, CommandFailed) as e:
            handle_workload_validation_failure(
                e, component_name, f"{stress_level} container strength testing"
            )

        # Analyze container strength testing results using helper function
        results = analyze_strength_test_results(
            krkn, component_name, stress_level, "container strength testing"
        )
        total_scenarios = results["total_scenarios"]
        strength_score = results["strength_score"]

        # Container strength testing success criteria (more lenient than basic tests)
        # Validate strength test results
        validate_strength_test_results(
            strength_score,
            total_scenarios,
            component_name,
            stress_level,
            min_success_rate=65,
        )

        # Final Ceph health check after container strength testing
        assert check_ceph_crashes(
            component_name, f"{stress_level} container strength testing"
        )

        log.info(
            f"🏁 Container strength testing for {component_name} completed successfully "
            f"({stress_level} level, {strength_score:.1f}% container strength score)"
        )

    @pytest.mark.parametrize(
        "ceph_component_label,component_name",
        [
            (
                OSD_APP_LABEL,
                "osd",
            ),  # OSDs can handle multiple failures - unique all-instances logic
            (RGW_APP_LABEL, "rgw"),  # RGW with maximum chaos intensity
            (
                CEPHFS_NODEPLUGIN_LABEL,
                "cephfs-nodeplugin",
            ),  # Node plugins - most resilient
            (RBD_NODEPLUGIN_LABEL, "rbd-nodeplugin"),  # Node plugins - most resilient
        ],
        ids=[
            "osd-maximum-chaos",
            "rgw-maximum-chaos",
            "cephfs-nodeplugin-maximum-chaos",
            "rbd-nodeplugin-maximum-chaos",
        ],
    )
    def test_krkn_container_maximum_chaos(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        component_name,
    ):
        """
        Maximum intensity container chaos testing for most resilient components.

        This test focuses on the most resilient components (OSD, RGW, Node Plugins)
        and applies maximum chaos intensity to test their ultimate resilience limits.
        Unlike the main test, this targets ALL available instances with aggressive scenarios.

        Components tested (maximum chaos):
        - OSD: All instances with maximum disruption patterns
        - RGW: All instances with high-intensity chaos
        - CSI Node Plugins: All instances with extreme chaos patterns

        Critical components (MON, MGR, MDS, Controllers, Rook) are excluded as they
        are already covered by the main test with appropriate safety controls.
        """
        from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
        from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
        from ocs_ci.krkn_chaos.krkn_scenario_generator import ContainerScenarios
        from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour

        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = "openshift-storage"

        # 🔍 DYNAMIC INSTANCE DETECTION: Get all available pod instances - one-liner
        instance_count, pod_names = detect_instances_or_skip(
            ceph_component_label, component_name
        )

        # 🎯 COMPONENT-AWARE CONFIGURATION: Adjust chaos intensity based on criticality
        is_critical = component_name in [
            "mon",
            "mgr",
            "mds",
            "cephfs-ctrlplugin",
            "rbd-ctrlplugin",
            "rook-operator",
        ]

        if is_critical:
            # 🛡️ CONSERVATIVE APPROACH: Critical components get gentler treatment
            kill_signal = "SIGTERM"  # Graceful termination
            pause_duration = 45  # Shorter pause
            target_instances = min(1, instance_count)  # Never more than 1 for critical
            wait_duration = 600  # Longer recovery time
            log.info(
                f"🛡️ Using CONSERVATIVE settings for critical component {component_name}"
            )
        else:
            # 💥 AGGRESSIVE APPROACH: Resilient components get full chaos
            kill_signal = "SIGKILL"  # Immediate termination
            pause_duration = 90  # Longer pause
            target_instances = instance_count  # ALL instances for resilient components
            wait_duration = 480  # Standard recovery time
            log.info(
                f"💥 Using AGGRESSIVE settings for resilient component {component_name}"
            )

        log.info("📊 Chaos Configuration:")
        log.info(f"   • Total instances available: {instance_count}")
        log.info(f"   • Target instances for chaos: {target_instances}")
        log.info(f"   • Kill signal: {kill_signal}")
        log.info(f"   • Pause duration: {pause_duration}s")
        log.info(
            f"   • Component criticality: {'CRITICAL' if is_critical else 'RESILIENT'}"
        )

        # 🎭 SCENARIO GENERATION: Create comprehensive container chaos scenarios
        log.info(
            f"🎭 Generating container chaos scenarios for ALL {component_name} instances"
        )

        scenarios = [
            # 🎯 PRIMARY KILL: Standard container kill scenario (ALL instances)
            ContainerScenarios.container_kill(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=ceph_component_label,
                instance_count=target_instances,
                kill_signal=kill_signal,
                wait_duration=wait_duration,
            ),
            # 🔥 AGGRESSIVE KILL: Rapid container termination (ALL instances)
            ContainerScenarios.container_kill(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=ceph_component_label,
                instance_count=target_instances,
                kill_signal="SIGKILL",  # Always SIGKILL for aggressive scenario
                wait_duration=wait_duration - 120,  # Shorter wait for rapid succession
            ),
            # ⏸️ PRIMARY PAUSE: Standard container pause scenario
            ContainerScenarios.container_pause(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=ceph_component_label,
                instance_count=(
                    max(1, target_instances // 2) if is_critical else target_instances
                ),
                pause_seconds=pause_duration,
                wait_duration=wait_duration,
            ),
            # 💥 EXTENDED PAUSE: Longer container suspension
            ContainerScenarios.container_pause(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=ceph_component_label,
                instance_count=(
                    max(1, target_instances // 2) if is_critical else target_instances
                ),
                pause_seconds=pause_duration * 2,  # 2x longer pause
                wait_duration=wait_duration + 240,  # Extended wait for recovery
            ),
        ]

        # Add extra scenarios for resilient components
        if not is_critical:
            extra_scenarios = [
                # ⚡ RAPID-FIRE KILL: Quick successive container kills
                ContainerScenarios.container_kill(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=target_instances,
                    kill_signal=kill_signal,
                    wait_duration=300,  # Shorter wait for rapid-fire
                ),
                # 🌪️ CHAOS STORM: Maximum intensity container chaos
                ContainerScenarios.container_kill(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=target_instances,
                    kill_signal="SIGKILL",
                    wait_duration=240,  # Minimal wait for maximum chaos
                ),
            ]
            scenarios.extend(extra_scenarios)
            log.info(
                f"💪 Added {len(extra_scenarios)} extra high-intensity scenarios for resilient component"
            )

        log.info(
            f"📋 Generated {len(scenarios)} container chaos scenarios for {component_name}"
        )

        # 🔧 KRKN CONFIGURATION: Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("container_scenarios", scenario)
        config.set_tunings(wait_duration=60, iterations=1)
        config.write_to_file(location=scenario_dir)
        log.info("✅ Krkn configuration file written successfully")

        # 🚀 CHAOS EXECUTION: Execute Krkn container chaos scenarios
        krkn = KrKnRunner(config.global_config)
        try:
            log.info(
                f"🚀 Starting container chaos injection on ALL {component_name} instances"
            )
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(
                f"✅ Container chaos injection completed successfully for {component_name}"
            )
        except CommandFailed as e:
            handle_krkn_command_failure(e, component_name, "container chaos")

        # 🔍 WORKLOAD VALIDATION: Validate workloads and cleanup
        try:
            workload_ops.validate_and_cleanup()
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(
                f"Workload validation/cleanup issue for {component_name}: {str(e)}"
            )

        # 📊 RESULTS ANALYSIS: Analyze chaos run results using helper function
        results = analyze_chaos_results(
            krkn, component_name, "container chaos", detailed_logging=False
        )
        success_rate = results["success_rate"]

        # Additional context logging for maximum chaos test
        log.info(f"   • Instances tested: {target_instances}/{instance_count}")
        log.info(f"   • Component type: {'CRITICAL' if is_critical else 'RESILIENT'}")

        # 🎯 SUCCESS CRITERIA: Evaluate success rate against thresholds
        evaluate_chaos_success_rate(
            success_rate, component_name, "container chaos", is_critical
        )

        log.info(
            f"🏁 Container chaos test for ALL {component_name} instances completed successfully"
        )
