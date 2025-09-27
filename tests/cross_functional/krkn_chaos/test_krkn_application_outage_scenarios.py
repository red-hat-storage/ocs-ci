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
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.krkn_chaos.krkn_helpers import (
    ApplicationScenarioHelper,  # New application scenario helper
    CephHealthHelper,  # New Ceph health helper
    InstanceDetectionHelper,  # New instance detection helper
    KrknExecutionHelper,  # New Krkn execution helper
    KrknResultAnalyzer,  # New result analyzer helper
    ValidationHelper,  # New validation helper
)
from ocs_ci.krkn_chaos.logging_helpers import log_test_start

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
            (OSD_APP_LABEL, "osd"),  # OSDs - chaos test all instances
            (MGR_APP_LABEL, "mgr"),  # MGR - chaos test with careful approach
            (MON_APP_LABEL, "mon"),  # MON - chaos test with careful approach
            (MDS_APP_LABEL, "mds"),  # MDS - chaos test with careful approach
            (RGW_APP_LABEL, "rgw"),  # RGW - chaos test all instances
            (
                CEPHFS_NODEPLUGIN_LABEL,
                "cephfs-nodeplugin",
            ),  # CephFS nodeplugin - chaos test all instances
            (
                RBD_NODEPLUGIN_LABEL,
                "rbd-nodeplugin",
            ),  # RBD nodeplugin - chaos test all instances
            (
                CEPHFS_CTRLPLUGIN_LABEL,
                "cephfs-ctrlplugin",
            ),  # CephFS controller - chaos test with careful approach
            (
                RBD_CTRLPLUGIN_LABEL,
                "rbd-ctrlplugin",
            ),  # RBD controller - chaos test with careful approach
            (
                ROOK_OPERATOR_PODS,
                "rook-operator",
            ),  # Rook operator - chaos test with careful approach
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
        Test application outage scenarios for ALL Rook Ceph components.

        This chaos test validates system resilience by injecting outages into Ceph components
        and verifying that the storage system can handle these disruptions gracefully while
        running VDBENCH workloads. ALL components are tested - critical components get careful
        chaos (SIGTERM, single instance) while resilient components get full chaos.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture that provides pre-configured VDBENCH workloads
            ceph_component_label: Parameterized Ceph component app label
            component_name: Human-readable component name for logging
        """
        # Use helper function for standardized test start logging
        log_test_start(
            "application outage", component_name, component_name=component_name
        )

        # 1. Detect component instances and configuration using InstanceDetectionHelper
        instance_helper = InstanceDetectionHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        instance_count, pod_names, pod_selector = (
            instance_helper.detect_component_instances(
                ceph_component_label,
                component_name,
                with_selector=True,
                fallback_on_error=True,
            )
        )

        app_helper = ApplicationScenarioHelper(
            scenario_dir=krkn_scenario_directory,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

        # 2. Create complete scenario list based on component criticality
        scenarios, duration, wait_duration = app_helper.create_complete_scenario_list(
            pod_selector, component_name, instance_count
        )

        log.info(
            f"⚙️  Configuration: duration={duration}s, wait={wait_duration}s, instances={instance_count}"
        )
        log.info(f"🔥 Chaos testing ALL components - {component_name} will be tested!")

        log.info(f"📋 Generated {len(scenarios)} total scenarios for {component_name}")

        # 4. Configure and execute Krkn
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("application_outages_scenarios", scenario)
        config.set_tunings(wait_duration=wait_duration, iterations=1)
        config.write_to_file(location=krkn_scenario_directory)

        # 4. Execute chaos scenarios using KrknExecutionHelper
        executor = KrknExecutionHelper(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        chaos_data = executor.execute_chaos_scenarios(
            config, component_name, "application outage"
        )

        # 5. Validate workloads
        try:
            workload_ops.validate_and_cleanup()
            log.info("✅ Workloads validated and cleaned up successfully")
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(f"⚠️  Workload validation/cleanup issue: {str(e)}")

        # 6. Analyze results and check system health
        analyzer = KrknResultAnalyzer()
        total_scenarios, successful_scenarios, failing_scenarios = (
            analyzer.analyze_application_outage_results(chaos_data, component_name)
        )

        # Validate chaos execution using ValidationHelper
        validator = ValidationHelper()
        validator.validate_chaos_execution(
            total_scenarios,
            successful_scenarios,
            component_name,
            "application outage chaos",
        )

        # Final Ceph health check using CephHealthHelper
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        assert health_helper.check_ceph_crashes(
            component_name, "application outage chaos"
        )

        log.info(
            f"🎉 Application outage test for {component_name} completed successfully"
        )

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
        # Use helper function for standardized test start logging
        log_test_start(
            f"{stress_level.upper()} strength testing",
            target_component,
            component_name=target_component,
            stress_level=stress_level.upper(),
            duration_multiplier=duration_multiplier,
        )

        # Get component label using ApplicationScenarioHelper (inherits from BaseScenarioHelper)
        app_helper = ApplicationScenarioHelper()
        ceph_component_label = app_helper.get_component_label(target_component)
        # Use InstanceDetectionHelper to get pod_selector (we don't need instance detection here)
        instance_helper = InstanceDetectionHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        _, _, pod_selector = instance_helper.detect_component_instances(
            ceph_component_label,
            target_component,
            with_selector=True,
            fallback_on_error=True,
        )

        log.info(f"⚙️  Configuration: duration_multiplier={duration_multiplier}x")

        app_helper = ApplicationScenarioHelper(
            scenario_dir=krkn_scenario_directory,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

        # Create strength testing scenarios using the helper
        scenarios = app_helper.create_strength_test_scenarios(
            pod_selector=pod_selector,
            stress_level=stress_level,
            duration_multiplier=duration_multiplier,
        )

        log.info(f"📋 Generated {len(scenarios)} strength testing scenarios")

        # Configure and execute
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("application_outages_scenarios", scenario)

        extended_wait = 90 if stress_level == "ultimate" else 60
        config.set_tunings(wait_duration=extended_wait, iterations=1)
        config.write_to_file(location=krkn_scenario_directory)

        # Execute strength test scenarios using KrknExecutionHelper
        executor = KrknExecutionHelper(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        chaos_data = executor.execute_strength_test_scenarios(
            config, target_component, stress_level
        )

        # Enhanced validation for strength testing
        try:
            workload_ops.validate_and_cleanup()
            log.info("💪 Workloads survived strength testing - resilience confirmed!")
        except (UnexpectedBehaviour, CommandFailed) as e:
            validator = ValidationHelper()
            validator.handle_workload_validation_failure(
                e, target_component, f"{stress_level} strength testing"
            )

        # Analyze results with strength-specific criteria using KrknResultAnalyzer
        analyzer = KrknResultAnalyzer()
        total_scenarios, successful_scenarios, strength_score = (
            analyzer.analyze_strength_test_results(
                chaos_data, target_component, stress_level
            )
        )

        validator = ValidationHelper()
        validator.validate_strength_test_results(
            strength_score,
            len(chaos_data["telemetry"]["scenarios"]),
            target_component,
            stress_level,
            min_success_rate=60,
        )

        # Final Ceph health check using CephHealthHelper
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        assert health_helper.check_ceph_crashes(
            f"{target_component} strength testing",
            "application outage strength testing",
        )

        log.info(
            f"🎉 STRENGTH TEST PASSED: {target_component} achieved {strength_score:.1f}% "
            f"resilience under {stress_level} stress!"
        )
