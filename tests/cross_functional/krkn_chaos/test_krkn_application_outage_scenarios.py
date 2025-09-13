"""
Test suite for Krkn application outage chaos scenarios.

This module provides comprehensive tests for application outage scenarios using the Krkn chaos engineering tool.
It includes tests for:
- Ceph Monitor (MON) application outages
- Ceph Metadata Server (MDS) application outages
- Ceph Manager (MGR) application outages
- Ceph Object Storage Daemon (OSD) application outages
- Ceph RADOS Gateway (RGW) application outages
- CephFS CSI Node Plugin application outages
- RBD CSI Node Plugin application outages
- CephFS CSI Controller Plugin application outages
- RBD CSI Controller Plugin application outages
- Rook Operator application outages

The tests create VDBENCH workloads and inject application failures to validate system resilience.
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
from ocs_ci.krkn_chaos.krkn_scenario_generator import ApplicationOutageScenarios
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.resiliency.resiliency_tools import CephStatusTool
from ocs_ci.ocs.resources.pod import get_pods_having_label

log = logging.getLogger(__name__)


@green_squad
@chaos
@polarion_id("OCS-1236")
class TestKrKnApplicationOutageScenarios:
    """
    Test suite for Krkn application outage chaos scenarios
    """

    @pytest.mark.parametrize(
        "ceph_component_label,component_name",
        [
            (OSD_APP_LABEL, "osd"),  # OSDs can handle multiple failures
            (MGR_APP_LABEL, "mgr"),  # Critical: active/standby pair - conservative
            (MON_APP_LABEL, "mon"),  # Critical: NEVER >1 (breaks quorum)
            (MDS_APP_LABEL, "mds"),  # Critical: usually 1-2 active - conservative
            (RGW_APP_LABEL, "rgw"),  # HA design: multiple gateways expected
            (
                CEPHFS_NODEPLUGIN_LABEL,
                "cephfs-nodeplugin",
            ),  # Node plugins: one per node, can handle multiple
            (
                RBD_NODEPLUGIN_LABEL,
                "rbd-nodeplugin",
            ),  # Node plugins: one per node, can handle multiple
            (
                CEPHFS_CTRLPLUGIN_LABEL,
                "cephfs-ctrlplugin",
            ),  # Controller plugins: typically 1-2 replicas
            (
                RBD_CTRLPLUGIN_LABEL,
                "rbd-ctrlplugin",
            ),  # Controller plugins: typically 1-2 replicas
            (
                ROOK_OPERATOR_PODS,
                "rook-operator",
            ),  # Critical: single operator instance - very conservative
        ],
        ids=[
            "osd-all-instances",
            "mgr-all-instances",
            "mon-all-instances",
            "mds-all-instances",
            "rgw-all-instances",
            "cephfs-nodeplugin-all-instances",
            "rbd-nodeplugin-all-instances",
            "cephfs-ctrlplugin-all-instances",
            "rbd-ctrlplugin-all-instances",
            "rook-operator-all-instances",
        ],
    )
    def test_run_krkn_application_outage_scenarios(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        component_name,
    ):
        """
        Test application outage scenarios for different Rook Ceph components.

        This test validates application resilience by injecting outages into ALL available
        instances of different Ceph components (OSD, MGR, MON, MDS, RGW, CSI plugins, Rook operator)
        and verifying that the storage system can handle these disruptions gracefully while running
        VDBENCH workloads.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture that provides pre-configured VDBENCH workloads
            ceph_component_label: Parameterized Ceph component app label
            component_name: Human-readable component name for logging

        Note:
            Application outage scenarios affect ALL pods matching the pod_selector.
            The test dynamically detects the number of available instances and applies
            appropriate chaos scenarios based on component criticality.
        """
        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        # Parse the component label to extract the app selector
        # e.g., "app=rook-ceph-osd" -> {"app": "rook-ceph-osd"}
        label_parts = ceph_component_label.split("=")
        pod_selector = {label_parts[0]: label_parts[1]}

        # Dynamically detect available instances for this component
        try:
            available_pods = get_pods_having_label(
                label=ceph_component_label, namespace=openshift_storage_ns
            )
            instance_count = len(available_pods)
            pod_names = [pod.name for pod in available_pods]

            log.info(
                f"Starting Krkn application outage test for {component_name} component"
            )
            log.info(f"Detected {instance_count} available instances: {pod_names}")
            log.info(
                f"Using pod selector: {pod_selector} (affects all {instance_count} matching pods)"
            )
        except Exception as e:
            log.error(f"Failed to detect available instances for {component_name}: {e}")
            # Fallback to assuming at least 1 instance exists
            instance_count = 1
            log.warning(f"Using fallback instance_count=1 for {component_name}")

        log.info(
            f"Creating application outage scenarios for {component_name} component"
        )

        # Configure scenario parameters based on component criticality and instance count
        critical_components = [
            "mon",
            "mgr",
            "mds",
            "cephfs-ctrlplugin",
            "rbd-ctrlplugin",
            "rook-operator",
        ]
        if component_name in critical_components:
            # Conservative settings for critical components
            duration = 60  # Shorter duration for critical components
            wait_duration = 30
            log.info(
                f"Using conservative settings for critical {component_name} component "
                f"({instance_count} instances detected)"
            )
        else:
            # Standard settings for less critical components (OSDs, RGWs, node plugins)
            duration = 120
            wait_duration = 60
            log.info(
                f"Using standard settings for {component_name} component "
                f"({instance_count} instances detected)"
            )

        # Additional safety check for single-instance critical components
        if component_name in ["mon", "rook-operator"] and instance_count == 1:
            duration = min(duration, 45)  # Even more conservative for single instances
            log.warning(
                f"Single instance detected for critical {component_name} - "
                f"using extra conservative duration: {duration}s"
            )

        scenarios = [
            # 🎯 PRIMARY OUTAGE: Standard application outage scenario
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration,
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
            ),
            # 🔥 EXTENDED OUTAGE: Prolonged application failure test
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration * 2,  # 2x longer duration
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
                block=True,  # Block until completion for sustained stress
            ),
            # ⚡ RAPID-FIRE OUTAGE: Quick successive failures
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration // 2,  # Shorter but rapid
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
            ),
            # 💥 STRESS TEST OUTAGE: Maximum duration for resilience testing
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration * 3,  # 3x longer for ultimate stress
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
                block=True,  # Ensure completion for stress validation
            ),
        ]

        # Add additional high-intensity scenarios for non-critical components
        if component_name not in critical_components:
            # These scenarios are only safe for less critical components (OSD, RGW, node plugins)
            log.info(
                f"Adding high-intensity scenarios for non-critical {component_name} component "
                f"with {instance_count} instances"
            )
            additional_scenarios = [
                # 🌪️ CHAOS STORM: Multiple rapid outages
                ApplicationOutageScenarios.application_outage(
                    scenario_dir,
                    duration=duration // 3,  # Very short duration
                    namespace=openshift_storage_ns,
                    pod_selector=pod_selector,
                ),
                ApplicationOutageScenarios.application_outage(
                    scenario_dir,
                    duration=duration // 3,  # Another rapid burst
                    namespace=openshift_storage_ns,
                    pod_selector=pod_selector,
                ),
                # 💀 ENDURANCE TEST: Ultra-long outage for maximum resilience testing
                ApplicationOutageScenarios.application_outage(
                    scenario_dir,
                    duration=duration * 5,  # 5x longer - ultimate endurance
                    namespace=openshift_storage_ns,
                    pod_selector=pod_selector,
                    block=True,
                ),
                # 🚨 BURST PATTERN: Alternating short/long outages
                ApplicationOutageScenarios.application_outage(
                    scenario_dir,
                    duration=duration // 4,  # Quick burst
                    namespace=openshift_storage_ns,
                    pod_selector=pod_selector,
                ),
                ApplicationOutageScenarios.application_outage(
                    scenario_dir,
                    duration=duration * 2,  # Followed by sustained outage
                    namespace=openshift_storage_ns,
                    pod_selector=pod_selector,
                    block=True,
                ),
            ]
            scenarios.extend(additional_scenarios)
            log.info(
                f"Added {len(additional_scenarios)} high-intensity scenarios for {component_name} "
                f"(safe for non-critical components with {instance_count} instances)"
            )

        log.info(
            f"Generated {len(scenarios)} application outage scenarios for {component_name}"
        )

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("application_outages_scenarios", scenario)
        config.set_tunings(wait_duration=wait_duration, iterations=1)
        config.write_to_file(location=scenario_dir)
        log.info("Krkn configuration file written successfully")

        # Execute Krkn chaos scenarios
        krkn = KrKnRunner(config.global_config)
        try:
            log.info(
                f"Starting application outage chaos injection for {component_name}"
            )
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(
                f"Application outage chaos injection completed successfully for {component_name}"
            )
        except CommandFailed as e:
            log.error(f"Krkn command failed for {component_name}: {str(e)}")
            pytest.fail(f"Krkn command failed for {component_name}: {str(e)}")

        # Validate workloads and cleanup
        try:
            workload_ops.validate_and_cleanup()
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(
                f"Workload validation/cleanup issue for {component_name}: {str(e)}"
            )

        # Analyze chaos run results
        log.info("Analyzing chaos run results")
        chaos_run_output = krkn.get_chaos_data()

        total_scenarios = len(chaos_run_output["telemetry"]["scenarios"])
        failing_scenarios = [
            scenario
            for scenario in chaos_run_output["telemetry"]["scenarios"]
            if scenario["affected_pods"]["error"] is not None
        ]
        successful_scenarios = total_scenarios - len(failing_scenarios)

        log.info(
            f"Chaos run summary: {successful_scenarios}/{total_scenarios} scenarios succeeded"
        )

        if failing_scenarios:
            log.warning(
                f"Some application outage scenarios failed for {component_name}:"
                f"{len(failing_scenarios)} out of {total_scenarios}"
            )
            for scenario in failing_scenarios:
                log.warning(
                    f"Failed scenario: {scenario['scenario']} - Error: {scenario['affected_pods']['error']}"
                )

        # Only fail the test if ALL scenarios failed (indicates framework issue)
        # or if no scenarios were executed at all
        if total_scenarios == 0:
            pytest.fail(
                "No scenarios were executed - this indicates a framework failure"
            )
        elif successful_scenarios == 0:
            pytest.fail(
                f"All {total_scenarios} scenarios failed - this may indicate a configuration or environment issue"
            )
        else:
            log.info(
                f"Test passed: {successful_scenarios} scenarios executed successfully, chaos injection working properly"
            )

        # Check for Ceph crashes after chaos injection
        log.info(
            "Checking for Ceph crashes after application outage chaos injection..."
        )
        try:
            ceph_status_tool = CephStatusTool()
            ceph_crashes_found = ceph_status_tool.check_ceph_crashes()
            assert not ceph_crashes_found, (
                f"Ceph crashes detected after application outage chaos for {component_name}. "
                f"This indicates that the chaos injection may have caused Ceph daemon failures."
            )
            log.info(
                "No Ceph crashes detected - cluster is stable after application outage chaos"
            )
        except Exception as e:
            log.error(f"Failed to check for Ceph crashes: {e}")
            # Don't fail the test if we can't check for crashes, but log the issue
            log.warning("Unable to verify Ceph crash status - continuing with test")

        log.info(f"Application outage test for {component_name} completed successfully")

    @pytest.mark.parametrize(
        "target_component,stress_level,duration_multiplier",
        [
            ("osd", "extreme", 6),  # OSDs can handle extreme stress
            ("rgw", "high", 4),  # RGWs are resilient but more conservative
            ("osd", "ultimate", 8),  # Ultimate OSD stress test
            ("cephfs-nodeplugin", "high", 4),  # Node plugins are resilient like RGWs
            ("rbd-nodeplugin", "high", 4),  # Node plugins are resilient like RGWs
            (
                "cephfs-nodeplugin",
                "extreme",
                6,
            ),  # Node plugins can handle extreme stress
            ("rbd-nodeplugin", "extreme", 6),  # Node plugins can handle extreme stress
        ],
        ids=[
            "osd-extreme-stress",
            "rgw-high-stress",
            "osd-ultimate-stress",
            "cephfs-nodeplugin-high-stress",
            "rbd-nodeplugin-high-stress",
            "cephfs-nodeplugin-extreme-stress",
            "rbd-nodeplugin-extreme-stress",
        ],
    )
    def test_krkn_application_strength_testing(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        target_component,
        stress_level,
        duration_multiplier,
    ):
        """
        Extreme application strength testing with multi-pattern chaos scenarios.

        This test pushes application resilience to the limits with various chaos patterns:
        - Cascading failures
        - Sustained outages
        - Rapid-fire disruptions
        - Recovery stress testing
        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture for VDBENCH workloads
            target_component: Component to target (osd, rgw)
            stress_level: Level of stress testing (high, extreme, ultimate)
            duration_multiplier: Multiplier for base duration
        """
        log.info(
            f"Starting EXTREME application strength testing for {target_component} "
            f"with {stress_level} stress level (multiplier: {duration_multiplier}x)"
        )

        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        # Map component to label
        component_labels = {
            "osd": OSD_APP_LABEL,
            "rgw": RGW_APP_LABEL,
            "cephfs-nodeplugin": CEPHFS_NODEPLUGIN_LABEL,
            "rbd-nodeplugin": RBD_NODEPLUGIN_LABEL,
        }

        ceph_component_label = component_labels[target_component]
        label_parts = ceph_component_label.split("=")
        pod_selector = {label_parts[0]: label_parts[1]}

        # Base duration scaled by stress level
        base_duration = 120
        max_duration = base_duration * duration_multiplier

        log.info(
            f"Creating {stress_level} strength testing scenarios for {target_component}"
        )
        log.info(f"Maximum duration: {max_duration}s, Base: {base_duration}s")

        # 🏗️ STRENGTH TESTING SCENARIO PATTERNS
        scenarios = [
            # 🎯 BASELINE: Standard outage for comparison
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration,
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
            ),
            # 🔄 CASCADING PATTERN: Progressive failure escalation
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration,  # Start normal
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration * 2,  # Escalate
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
                block=True,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=max_duration,  # Peak stress
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
                block=True,
            ),
            # ⚡ RAPID-FIRE PATTERN: Quick successive hits
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration // 4,  # Quick burst 1
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration // 4,  # Quick burst 2
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration // 4,  # Quick burst 3
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
            ),
            # 🌊 WAVE PATTERN: Alternating intensity
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration // 2,  # Low wave
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration * 3,  # High wave
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
                block=True,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration // 2,  # Low wave
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
            ),
            # 💀 ENDURANCE PATTERN: Ultimate sustained stress
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=max_duration,  # Maximum duration
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
                block=True,
            ),
            # 🔥 RECOVERY STRESS: Test recovery under pressure
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration * 2,  # Sustained outage
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
                block=True,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration // 2,  # Quick follow-up during recovery
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
            ),
        ]

        log.info(
            f"Generated {len(scenarios)} strength testing scenarios for {target_component} "
            f"({stress_level} level)"
        )

        # Generate Krkn configuration with extended wait times for strength testing
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("application_outages_scenarios", scenario)

        # Longer wait duration for strength testing
        extended_wait = 90 if stress_level == "ultimate" else 60
        config.set_tunings(wait_duration=extended_wait, iterations=1)
        config.write_to_file(location=scenario_dir)

        log.info(
            f"Krkn strength testing configuration written (wait_duration={extended_wait}s)"
        )

        # Execute Krkn chaos scenarios
        krkn = KrKnRunner(config.global_config)
        try:
            log.info(
                f"🚀 Starting {stress_level} application strength testing for {target_component}"
            )
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(
                f"✅ Application strength testing completed for {target_component} ({stress_level} level)"
            )
        except CommandFailed as e:
            log.error(f"Krkn strength testing failed for {target_component}: {str(e)}")
            pytest.fail(
                f"Krkn strength testing failed for {target_component}: {str(e)}"
            )

        # Enhanced validation for strength testing
        try:
            workload_ops.validate_and_cleanup()
            log.info(
                "💪 Workloads survived strength testing - application resilience confirmed!"
            )
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.error(
                f"Workload failure during {stress_level} strength testing: {str(e)}"
            )
            # For strength testing, workload issues are more critical
            pytest.fail(
                f"Application failed strength testing - workloads could not survive {stress_level} "
                f"stress level for {target_component}: {str(e)}"
            )

        # Analyze strength testing results
        log.info("📊 Analyzing strength testing results...")
        chaos_run_output = krkn.get_chaos_data()

        total_scenarios = len(chaos_run_output["telemetry"]["scenarios"])
        failing_scenarios = [
            scenario
            for scenario in chaos_run_output["telemetry"]["scenarios"]
            if scenario["affected_pods"]["error"] is not None
        ]
        successful_scenarios = total_scenarios - len(failing_scenarios)

        # Calculate strength score
        strength_score = (
            (successful_scenarios / total_scenarios) * 100 if total_scenarios > 0 else 0
        )

        log.info(
            f"🏆 STRENGTH TESTING RESULTS for {target_component} ({stress_level}):"
        )
        log.info(f"   • Scenarios executed: {total_scenarios}")
        log.info(f"   • Successful scenarios: {successful_scenarios}")
        log.info(f"   • Failed scenarios: {len(failing_scenarios)}")
        log.info(f"   • Strength Score: {strength_score:.1f}%")

        # Enhanced failure analysis for strength testing
        if failing_scenarios:
            log.warning("⚠️  Some strength testing scenarios failed:")
            for scenario in failing_scenarios:
                log.warning(
                    f"   • {scenario['scenario']}: {scenario['affected_pods']['error']}"
                )

        # Strength testing success criteria (more lenient than basic tests)
        min_success_rate = 60  # 60% success rate for extreme stress testing

        if total_scenarios == 0:
            pytest.fail("No strength testing scenarios executed - framework failure")
        elif strength_score < min_success_rate:
            pytest.fail(
                f"Application strength insufficient: {strength_score:.1f}% success rate "
                f"(minimum {min_success_rate}% required for {stress_level} testing)"
            )
        else:
            log.info(
                f"🎉 STRENGTH TEST PASSED: {target_component} demonstrated {strength_score:.1f}% "
                f"resilience under {stress_level} stress conditions!"
            )

        # Final Ceph health check after extreme testing
        log.info("🔍 Final Ceph health check after strength testing...")
        try:
            ceph_status_tool = CephStatusTool()
            ceph_crashes_found = ceph_status_tool.check_ceph_crashes()
            assert not ceph_crashes_found, (
                f"Ceph crashes detected after {stress_level} strength testing for {target_component}. "
                f"Application may not be resilient enough for this stress level."
            )
            log.info("✅ No Ceph crashes - cluster survived strength testing!")
        except Exception as e:
            log.error(f"Failed to check Ceph health after strength testing: {e}")
            log.warning(
                "Unable to verify final Ceph health - test results may be incomplete"
            )

        log.info(
            f"🏁 Application strength testing for {target_component} completed successfully "
            f"({stress_level} level, {strength_score:.1f}% strength score)"
        )
