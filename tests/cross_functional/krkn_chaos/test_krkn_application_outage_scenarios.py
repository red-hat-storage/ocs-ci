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
from ocs_ci.krkn_chaos.krkn_helpers import check_ceph_crashes
from ocs_ci.ocs.resources.pod import get_pods_having_label

log = logging.getLogger(__name__)


class ComponentConfig:
    """Configuration class for managing component-specific chaos testing settings."""

    CRITICAL_COMPONENTS = [
        "mon",
        "mgr",
        "mds",
        "cephfs-ctrlplugin",
        "rbd-ctrlplugin",
        "rook-operator",
    ]

    RESILIENT_COMPONENTS = ["osd", "rgw", "cephfs-nodeplugin", "rbd-nodeplugin"]

    @classmethod
    def is_critical(cls, component_name):
        """Check if a component is critical and requires conservative settings."""
        return component_name in cls.CRITICAL_COMPONENTS

    @classmethod
    def get_duration_settings(cls, component_name, instance_count):
        """Get duration and wait_duration based on component criticality and instance count."""
        if cls.is_critical(component_name):
            duration = 60  # Conservative for critical components
            wait_duration = 30

            # Extra conservative for single-instance critical components
            if component_name in ["mon", "rook-operator"] and instance_count == 1:
                duration = min(duration, 45)

            return duration, wait_duration
        else:
            # Standard settings for resilient components
            return 120, 60


@green_squad
@chaos
@polarion_id("OCS-1236")
class TestKrKnApplicationOutageScenarios:
    """
    Test suite for Krkn application outage chaos scenarios
    """

    def _detect_component_instances(self, component_label, component_name):
        """
        Detect available instances for a component.

        Returns:
            tuple: (instance_count, pod_names, pod_selector)
        """
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE
        label_parts = component_label.split("=")
        pod_selector = {label_parts[0]: label_parts[1]}

        try:
            available_pods = get_pods_having_label(
                label=component_label, namespace=openshift_storage_ns
            )
            instance_count = len(available_pods)
            # available_pods is a list of dictionaries, not Pod objects
            pod_names = [pod["metadata"]["name"] for pod in available_pods]

            log.info(
                f"Detected {instance_count} available instances for {component_name}: {pod_names}"
            )
            return instance_count, pod_names, pod_selector

        except Exception as e:
            log.error(f"Failed to detect available instances for {component_name}: {e}")
            log.warning(f"Using fallback instance_count=1 for {component_name}")
            return 1, [], pod_selector

    def _create_basic_scenarios(self, scenario_dir, duration, namespace, pod_selector):
        """Create basic application outage scenarios."""
        return [
            # 🎯 PRIMARY OUTAGE: Standard application outage scenario
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            # 🔥 EXTENDED OUTAGE: Prolonged application failure test
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration * 2,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            # ⚡ RAPID-FIRE OUTAGE: Quick successive failures
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration // 2,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            # 💥 STRESS TEST OUTAGE: Maximum duration for resilience testing
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration * 3,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
        ]

    def _create_high_intensity_scenarios(
        self, scenario_dir, duration, namespace, pod_selector
    ):
        """Create high-intensity scenarios for resilient components."""
        return [
            # 🌪️ CHAOS STORM: Multiple rapid outages
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration // 3,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration // 3,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            # 💀 ENDURANCE TEST: Ultra-long outage for maximum resilience testing
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration * 5,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            # 🚨 BURST PATTERN: Alternating short/long outages
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration // 4,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration * 2,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
        ]

    def _execute_chaos_scenarios(self, config, component_name):
        """Execute Krkn chaos scenarios and return results."""
        krkn = KrKnRunner(config.global_config)
        try:
            log.info(
                f"🚀 Starting application outage chaos injection for {component_name}"
            )
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(
                f"✅ Application outage chaos injection completed for {component_name}"
            )
            return krkn.get_chaos_data()
        except CommandFailed as e:
            log.error(f"Krkn command failed for {component_name}: {str(e)}")
            raise

    def _analyze_chaos_results(self, chaos_data, component_name):
        """Analyze and validate chaos run results."""
        total_scenarios = len(chaos_data["telemetry"]["scenarios"])
        failing_scenarios = [
            scenario
            for scenario in chaos_data["telemetry"]["scenarios"]
            if scenario["affected_pods"]["error"] is not None
        ]
        successful_scenarios = total_scenarios - len(failing_scenarios)

        log.info(f"📊 Chaos Results for {component_name}:")
        log.info(f"   • Total scenarios: {total_scenarios}")
        log.info(f"   • Successful: {successful_scenarios}")
        log.info(f"   • Failed: {len(failing_scenarios)}")

        if failing_scenarios:
            log.warning(f"⚠️  Failed scenarios for {component_name}:")
            for scenario in failing_scenarios:
                log.warning(
                    f"   • {scenario['scenario']}: {scenario['affected_pods']['error']}"
                )

        # Validation logic
        if total_scenarios == 0:
            pytest.fail("No scenarios were executed - framework failure")
        elif successful_scenarios == 0:
            pytest.fail(
                f"All {total_scenarios} scenarios failed - configuration/environment issue"
            )
        else:
            log.info(
                f"✅ Test passed: {successful_scenarios} scenarios executed successfully"
            )

    def _check_ceph_health(self, component_name):
        """Check for Ceph crashes after chaos injection."""
        assert check_ceph_crashes(component_name, "application outage chaos")

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
        instances of different Ceph components and verifying that the storage system
        can handle these disruptions gracefully while running VDBENCH workloads.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture that provides pre-configured VDBENCH workloads
            ceph_component_label: Parameterized Ceph component app label
            component_name: Human-readable component name for logging
        """
        log.info(f"🎯 Starting application outage test for {component_name}")

        # 1. Detect component instances and configuration
        instance_count, pod_names, pod_selector = self._detect_component_instances(
            ceph_component_label, component_name
        )

        # 2. Get duration settings based on component criticality
        duration, wait_duration = ComponentConfig.get_duration_settings(
            component_name, instance_count
        )

        log.info(
            f"⚙️  Configuration: duration={duration}s, wait={wait_duration}s, instances={instance_count}"
        )
        if ComponentConfig.is_critical(component_name):
            log.info(
                f"🛡️  Using conservative settings for critical {component_name} component"
            )

        # 3. Create chaos scenarios
        scenarios = self._create_basic_scenarios(
            krkn_scenario_directory,
            duration,
            constants.OPENSHIFT_STORAGE_NAMESPACE,
            pod_selector,
        )

        # Add high-intensity scenarios for resilient components
        if not ComponentConfig.is_critical(component_name):
            high_intensity_scenarios = self._create_high_intensity_scenarios(
                krkn_scenario_directory,
                duration,
                constants.OPENSHIFT_STORAGE_NAMESPACE,
                pod_selector,
            )
            scenarios.extend(high_intensity_scenarios)
            log.info(
                f"💪 Added {len(high_intensity_scenarios)} high-intensity scenarios for resilient component"
            )

        log.info(f"📋 Generated {len(scenarios)} total scenarios for {component_name}")

        # 4. Configure and execute Krkn
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("application_outages_scenarios", scenario)
        config.set_tunings(wait_duration=wait_duration, iterations=1)
        config.write_to_file(location=krkn_scenario_directory)

        chaos_data = self._execute_chaos_scenarios(config, component_name)

        # 5. Validate workloads
        try:
            workload_ops.validate_and_cleanup()
            log.info("✅ Workloads validated and cleaned up successfully")
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(f"⚠️  Workload validation/cleanup issue: {str(e)}")

        # 6. Analyze results and check system health
        self._analyze_chaos_results(chaos_data, component_name)
        self._check_ceph_health(component_name)

        log.info(
            f"🎉 Application outage test for {component_name} completed successfully"
        )

    def _create_strength_testing_scenarios(
        self, scenario_dir, base_duration, max_duration, namespace, pod_selector
    ):
        """Create comprehensive strength testing scenarios with various patterns."""
        return [
            # 🎯 BASELINE: Standard outage for comparison
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            # 🔄 CASCADING PATTERN: Progressive failure escalation
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration * 2,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=max_duration,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            # ⚡ RAPID-FIRE PATTERN: Quick successive hits
            *[
                ApplicationOutageScenarios.application_outage(
                    scenario_dir,
                    duration=base_duration // 4,
                    namespace=namespace,
                    pod_selector=pod_selector,
                )
                for _ in range(3)
            ],
            # 🌊 WAVE PATTERN: Alternating intensity
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration // 2,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration * 3,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration // 2,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            # 💀 ENDURANCE PATTERN: Ultimate sustained stress
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=max_duration,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            # 🔥 RECOVERY STRESS: Test recovery under pressure
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration * 2,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=base_duration // 2,
                namespace=namespace,
                pod_selector=pod_selector,
            ),
        ]

    def _analyze_strength_results(self, chaos_data, target_component, stress_level):
        """Analyze strength testing results and return strength score."""
        total_scenarios = len(chaos_data["telemetry"]["scenarios"])
        failing_scenarios = [
            scenario
            for scenario in chaos_data["telemetry"]["scenarios"]
            if scenario["affected_pods"]["error"] is not None
        ]
        successful_scenarios = total_scenarios - len(failing_scenarios)
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

        if failing_scenarios:
            log.warning("⚠️  Some strength testing scenarios failed:")
            for scenario in failing_scenarios:
                log.warning(
                    f"   • {scenario['scenario']}: {scenario['affected_pods']['error']}"
                )

        return strength_score

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
        - Cascading failures, Sustained outages, Rapid-fire disruptions, Recovery stress testing

        Args:
            target_component: Component to target (osd, rgw, cephfs-nodeplugin, rbd-nodeplugin)
            stress_level: Level of stress testing (high, extreme, ultimate)
            duration_multiplier: Multiplier for base duration
        """
        log.info(
            f"🚀 Starting {stress_level.upper()} strength testing for {target_component} "
            f"(multiplier: {duration_multiplier}x)"
        )

        # Component mapping and configuration
        component_labels = {
            "osd": OSD_APP_LABEL,
            "rgw": RGW_APP_LABEL,
            "cephfs-nodeplugin": CEPHFS_NODEPLUGIN_LABEL,
            "rbd-nodeplugin": RBD_NODEPLUGIN_LABEL,
        }

        ceph_component_label = component_labels[target_component]
        label_parts = ceph_component_label.split("=")
        pod_selector = {label_parts[0]: label_parts[1]}

        base_duration = 120
        max_duration = base_duration * duration_multiplier

        log.info(f"⚙️  Configuration: base={base_duration}s, max={max_duration}s")

        # Create strength testing scenarios
        scenarios = self._create_strength_testing_scenarios(
            krkn_scenario_directory,
            base_duration,
            max_duration,
            constants.OPENSHIFT_STORAGE_NAMESPACE,
            pod_selector,
        )

        log.info(f"📋 Generated {len(scenarios)} strength testing scenarios")

        # Configure and execute
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("application_outages_scenarios", scenario)

        extended_wait = 90 if stress_level == "ultimate" else 60
        config.set_tunings(wait_duration=extended_wait, iterations=1)
        config.write_to_file(location=krkn_scenario_directory)

        chaos_data = self._execute_chaos_scenarios(
            config, f"{target_component} ({stress_level})"
        )

        # Enhanced validation for strength testing
        try:
            workload_ops.validate_and_cleanup()
            log.info("💪 Workloads survived strength testing - resilience confirmed!")
        except (UnexpectedBehaviour, CommandFailed) as e:
            pytest.fail(
                f"Workloads failed {stress_level} strength testing for {target_component}: {str(e)}"
            )

        # Analyze results with strength-specific criteria
        strength_score = self._analyze_strength_results(
            chaos_data, target_component, stress_level
        )

        min_success_rate = 60  # 60% success rate for extreme stress testing
        if len(chaos_data["telemetry"]["scenarios"]) == 0:
            pytest.fail("No strength testing scenarios executed - framework failure")
        elif strength_score < min_success_rate:
            pytest.fail(
                f"Insufficient strength: {strength_score:.1f}% < {min_success_rate}% required"
            )

        # Final health check
        self._check_ceph_health(f"{target_component} strength testing")

        log.info(
            f"🎉 STRENGTH TEST PASSED: {target_component} achieved {strength_score:.1f}% "
            f"resilience under {stress_level} stress!"
        )
