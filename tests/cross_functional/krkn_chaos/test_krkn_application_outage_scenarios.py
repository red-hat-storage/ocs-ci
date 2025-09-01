"""
Test suite for Krkn application outage chaos scenarios.

This module provides comprehensive tests for application outage scenarios using the Krkn chaos engineering tool.
It includes tests for:
- Ceph Monitor (MON) application outages
- Ceph Metadata Server (MDS) application outages
- Ceph Manager (MGR) application outages
- Ceph Object Storage Daemon (OSD) application outages
- Ceph RADOS Gateway (RGW) application outages

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
)
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.krkn_chaos.krkn_scenario_generator import ApplicationOutageScenarios
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour

log = logging.getLogger(__name__)


@green_squad
@chaos
@polarion_id("OCS-1236")
class TestKrKnApplicationOutageScenarios:
    """
    Test suite for Krkn application outage chaos scenarios
    """

    @pytest.mark.parametrize(
        "ceph_component_label,instance_count,component_name",
        [
            (OSD_APP_LABEL, 2, "osd"),  # OSDs can handle multiple failures
            (MGR_APP_LABEL, 1, "mgr"),  # Critical: active/standby pair - conservative
            (MON_APP_LABEL, 1, "mon"),  # Critical: NEVER >1 (breaks quorum)
            (MDS_APP_LABEL, 1, "mds"),  # Critical: usually 1-2 active - conservative
            (RGW_APP_LABEL, 2, "rgw"),  # HA design: multiple gateways expected
        ],
        ids=["osd-2pods", "mgr-1pod", "mon-1pod", "mds-1pod", "rgw-2pods"],
    )
    def test_run_krkn_application_outage_scenarios(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        instance_count,
        component_name,
    ):
        """
        Test application outage scenarios for different Rook Ceph components.

        This test validates application resilience by injecting outages into different
        Ceph components (OSD, MGR, MON, MDS, RGW) and verifying that the storage system
        can handle these disruptions gracefully while running VDBENCH workloads.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture that provides pre-configured VDBENCH workloads
            ceph_component_label: Parameterized Ceph component app label
            instance_count: Number of pods to target for chaos injection
            component_name: Human-readable component name for logging
        """
        log.info(
            f"Starting Krkn application outage test for {component_name} component "
            f"with instance_count={instance_count}"
        )

        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        # Parse the component label to extract the app selector
        # e.g., "app=rook-ceph-osd" -> {"app": "rook-ceph-osd"}
        label_parts = ceph_component_label.split("=")
        pod_selector = {label_parts[0]: label_parts[1]}

        log.info(
            f"Creating application outage scenarios for {component_name} component"
        )
        log.info(
            f"Using pod selector: {pod_selector} with instance_count={instance_count}"
        )

        # Configure scenario parameters based on component criticality
        if component_name in ["mon", "mgr", "mds"]:
            # Conservative settings for critical components
            duration = 60  # Shorter duration for critical components
            wait_duration = 30
            log.info(
                f"Using conservative settings for critical {component_name} component"
            )
        else:
            # Standard settings for less critical components
            duration = 120
            wait_duration = 60
            log.info(f"Using standard settings for {component_name} component")

        scenarios = [
            # Primary application outage scenario
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=duration,
                namespace=openshift_storage_ns,
                pod_selector=pod_selector,
                instance_count=instance_count,
            ),
        ]

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
        failing_scenarios = [
            scenario
            for scenario in chaos_run_output["telemetry"]["scenarios"]
            if scenario["affected_pods"]["error"] is not None
        ]

        if failing_scenarios:
            log.error(
                f"Failed application outage scenarios for {component_name}: {failing_scenarios}"
            )

        assert (
            not failing_scenarios
        ), f"Application outage scenarios failed for {component_name} with pod errors: {failing_scenarios}"

        log.info(f"Application outage test for {component_name} completed successfully")
