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

logger = logging.getLogger(__name__)


@green_squad
@chaos
class TestKrKnApplicationOutageScenarios:
    """
    Test suite for Krkn application outage chaos scenarios
    """

    @pytest.mark.parametrize(
        "group_name,duration,block_directions",
        [
            ("osd_only", 300, ["Ingress", "Egress"]),  # OSD pods only
            ("mgr_only", 300, ["Ingress", "Egress"]),  # MGR pods only
            ("mds_only", 300, ["Ingress", "Egress"]),  # MDS pods only
            (
                "osd_mgr_mds_mon",
                300,
                ["Ingress", "Egress"],
            ),  # Combined OSD + MGR + MDS + MON
            ("all_rook_ceph", 300, ["Ingress", "Egress"]),  # All rook-ceph components
        ],
        ids=[
            "osd-only-outage",
            "mgr-only-outage",
            "mds-only-outage",
            "osd-mgr-mds-mon-combined-outage",
            "all-rook-ceph-outage",
        ],
    )
    @polarion_id("OCS-7340")
    def test_run_krkn_application_outage_scenarios(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        group_name,
        duration,
        block_directions,
    ):
        """
        Test grouped application outage scenarios for Rook Ceph components.

        This chaos test validates system resilience by injecting outages into groups of Ceph components
        and verifying that the storage system can handle these disruptions gracefully while
        running VDBENCH workloads. Components are grouped to test realistic failure scenarios.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture that provides pre-configured VDBENCH workloads
            group_name: Name of the component group to test
            duration: Duration of the outage in seconds
            block_directions: Network traffic directions to block
        """
        # Use helper function for standardized test start logging
        log_test_start(
            "grouped application outage", group_name, component_name=group_name
        )

        logger.test_step("Set up workloads for application outage testing")
        workload_ops.setup_workloads()

        logger.test_step(
            "Initialize application scenario helper and validate group configuration"
        )
        app_helper = ApplicationScenarioHelper(
            scenario_dir=krkn_scenario_directory,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

        # 2. Get group configuration and validate
        groups = app_helper.get_component_groups()
        if group_name not in groups:
            raise ValueError(
                f"Unknown group '{group_name}'. Available groups: {list(groups.keys())}"
            )

        group_config = groups[group_name]
        logger.info(f"Testing group '{group_name}': {group_config['description']}")
        logger.debug(f"Target pod selectors: {group_config['pod_selectors']}")

        logger.test_step(
            "Create grouped application outage scenario and configure Krkn"
        )
        scenario_file = app_helper.create_grouped_application_outage_scenario(
            group_name=group_name, duration=duration, block=block_directions
        )

        logger.debug(f"Configuration: duration={duration}s, block={block_directions}")
        logger.info(f"Chaos testing group: {group_name}")
        logger.debug(f"Generated scenario file: {scenario_file}")

        # 4. Configure and execute Krkn
        config = KrknConfigGenerator()
        config.add_scenario("application_outages_scenarios", scenario_file)
        config.set_tunings(wait_duration=300, iterations=1)  # Standard wait duration
        config.write_to_file(location=krkn_scenario_directory)

        logger.test_step("Execute chaos scenarios using KrknExecutionHelper")
        executor = KrknExecutionHelper(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        chaos_data = executor.execute_chaos_scenarios(
            config, group_name, "grouped application outage"
        )

        logger.test_step("Validate workloads after chaos execution")
        try:
            workload_ops.validate_and_cleanup()
            logger.info("Workloads validated and cleaned up successfully")
        except (UnexpectedBehaviour, CommandFailed) as e:
            logger.warning(f"Workload validation/cleanup issue: {str(e)}")

        # 7. Analyze results and check system health
        analyzer = KrknResultAnalyzer()
        total_scenarios, successful_scenarios, failing_scenarios = (
            analyzer.analyze_application_outage_results(chaos_data, group_name)
        )

        # Validate chaos execution using ValidationHelper
        validator = ValidationHelper()
        validator.validate_chaos_execution(
            total_scenarios,
            successful_scenarios,
            group_name,
            "grouped application outage chaos",
        )

        logger.test_step("Perform final Ceph health check")
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            group_name, "grouped application outage chaos"
        )
        logger.assertion(
            f"Ceph crashes after outage: expected=None, actual={'None' if no_crashes else crash_details}"
        )
        assert no_crashes, crash_details

        logger.info(
            f"Grouped application outage test for {group_name} completed successfully"
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
    @polarion_id("OCS-7341")
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

        logger.test_step("Set up workloads for strength testing")
        workload_ops.setup_workloads()

        logger.test_step("Detect component instances and get pod selector")
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

        logger.debug(f"Configuration: duration_multiplier={duration_multiplier}x")

        logger.test_step("Create strength test scenarios and configure Krkn")
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

        logger.info(f"Generated {len(scenarios)} strength testing scenarios")

        # Configure and execute
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("application_outages_scenarios", scenario)

        extended_wait = 90 if stress_level == "ultimate" else 60
        config.set_tunings(wait_duration=extended_wait, iterations=1)
        config.write_to_file(location=krkn_scenario_directory)

        logger.test_step("Execute strength test scenarios using KrknExecutionHelper")
        executor = KrknExecutionHelper(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        chaos_data = executor.execute_strength_test_scenarios(
            config, target_component, stress_level
        )

        logger.test_step("Validate workloads after strength testing")
        try:
            workload_ops.validate_and_cleanup()
            logger.info("Workloads survived strength testing - resilience confirmed")
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

        logger.test_step("Perform final Ceph health check after strength testing")
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            f"{target_component} strength testing",
            "application outage strength testing",
        )
        logger.assertion(
            f"Ceph crashes after strength test: expected=None, actual={'None' if no_crashes else crash_details}"
        )
        assert no_crashes, crash_details

        logger.info(
            f"Strength test passed: {target_component} achieved {strength_score:.1f}% "
            f"resilience under {stress_level} stress"
        )
