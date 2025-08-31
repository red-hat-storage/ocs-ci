"""
Test suite for Krkn network outage chaos scenarios.

This module provides comprehensive tests for network outage scenarios using the Krkn chaos engineering tool.
It includes tests for:
- General pod network outage with various traffic direction filters
- Targeted pod network outage using specific pod names
- Selective port blocking for ingress/egress traffic

The tests create VDBENCH workloads and inject network outage failures to validate system resilience.
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
from ocs_ci.krkn_chaos.krkn_scenario_generator import NetworkOutageScenarios
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour

log = logging.getLogger(__name__)


@green_squad
@chaos
@polarion_id("OCS-1235")
class TestKrKnNetworkChaosScenarios:
    """
    Test suite for Krkn network outage chaos scenarios
    """

    @pytest.mark.parametrize(
        "ceph_component_label",
        [
            OSD_APP_LABEL,
            MGR_APP_LABEL,
            MON_APP_LABEL,
            MDS_APP_LABEL,
            RGW_APP_LABEL,
        ],
        ids=["osd", "mgr", "mon", "mds", "rgw"],
    )
    def test_krkn_ceph_component_network_outage(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
    ):
        """
        Test network outage scenarios for different Rook Ceph components.

        This test validates network resilience by injecting network outages
        into different Ceph components (OSD, MGR, MON, MDS, RGW) and verifying
        that the system can handle these disruptions gracefully.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario files
            workload_ops: Workload operations fixture
            ceph_component_label: Parameterized Ceph component label
        """
        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        log.info(f"Testing network outage for Ceph component: {ceph_component_label}")

        # Create network outage scenarios targeting the specific Ceph component
        scenarios = [
            # Test egress and ingress blocking
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["egress", "ingress"],
                label_selector=ceph_component_label,
                instance_count=1,
                wait_duration=180,
                test_duration=60,
            ),
            # Test egress-only blocking with specific ports
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["egress"],
                egress_ports=[6789, 6800, 6801, 6802, 6803, 6804, 6805],  # Ceph ports
                label_selector=ceph_component_label,
                instance_count=1,
                wait_duration=180,
                test_duration=45,
            ),
            # Test ingress-only blocking
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["ingress"],
                label_selector=ceph_component_label,
                instance_count=1,
                wait_duration=180,
                test_duration=30,
            ),
        ]

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("pod_network_scenarios", scenario)
        config.set_tunings(wait_duration=60, iterations=1)
        config.write_to_file(location=scenario_dir)

        # Execute Krkn chaos scenarios
        krkn = KrKnRunner(config.global_config)
        try:
            log.info(f"Starting network chaos test for {ceph_component_label}")
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(f"Network chaos test completed for {ceph_component_label}")
        except CommandFailed as e:
            log.error(f"Krkn command failed for {ceph_component_label}: {str(e)}")
            pytest.fail(f"Krkn command failed for {ceph_component_label}: {str(e)}")

        # Validate workloads and cleanup
        try:
            workload_ops.validate_and_cleanup()
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(
                f"Workload validation/cleanup issue for {ceph_component_label}: {str(e)}"
            )

        # Analyze chaos run results
        chaos_run_output = krkn.get_chaos_data()
        failing_scenarios = [
            scenario
            for scenario in chaos_run_output["telemetry"]["scenarios"]
            if scenario["affected_pods"]["error"] is not None
        ]

        if failing_scenarios:
            log.error(
                f"Failed scenarios for {ceph_component_label}: {failing_scenarios}"
            )

        assert (
            not failing_scenarios
        ), f"Network outage scenarios failed for {ceph_component_label} with pod errors: {failing_scenarios}"

        log.info(
            f"Network outage test for {ceph_component_label} completed successfully"
        )
