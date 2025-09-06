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
)
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.krkn_chaos.krkn_scenario_generator import ContainerScenarios
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour

log = logging.getLogger(__name__)


@green_squad
@chaos
@polarion_id("OCS-1240")
class TestKrKnContainerChaosScenarios:
    """
    Test suite for Krkn container chaos scenarios
    """

    @pytest.mark.parametrize(
        "ceph_component_label,instance_count,kill_signal,pause_duration",
        [
            (
                OSD_APP_LABEL,
                2,
                "SIGKILL",
                90,
            ),  # OSDs can handle container restarts
            (
                MGR_APP_LABEL,
                1,
                "SIGTERM",
                60,
            ),  # MGR with graceful termination
            (
                MON_APP_LABEL,
                1,
                "SIGTERM",
                45,
            ),  # MON with minimal disruption (critical component)
            (
                MDS_APP_LABEL,
                1,
                "SIGTERM",
                60,
            ),  # MDS with moderate disruption
            (
                RGW_APP_LABEL,
                2,
                "SIGKILL",
                90,
            ),  # RGW can handle aggressive container restarts
        ],
        ids=[
            "osd-container-chaos",
            "mgr-container-chaos",
            "mon-container-chaos",
            "mds-container-chaos",
            "rgw-container-chaos",
        ],
    )
    def test_krkn_container_chaos(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        instance_count,
        kill_signal,
        pause_duration,
    ):
        """
        Test container chaos scenarios using Krkn container kill and pause templates.

        This test validates container-level resilience by killing and pausing containers
        in different Ceph component pods and verifying that the system can handle
        these container-level disruptions gracefully.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario files
            workload_ops: Workload operations fixture
            ceph_component_label: Parameterized Ceph component label
            instance_count: Number of pods to target for chaos injection
            kill_signal: Signal to use for container kill (SIGKILL/SIGTERM)
            pause_duration: Duration in seconds to pause containers
        """
        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        log.info(
            f"Testing container chaos for Ceph component: {ceph_component_label} "
            f"with instance_count={instance_count}, kill_signal={kill_signal}, "
            f"pause_duration={pause_duration}s"
        )

        # Create container chaos scenarios targeting the specific Ceph component
        scenarios = [
            # Container kill scenario - more aggressive disruption
            ContainerScenarios.container_kill(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=ceph_component_label,
                instance_count=instance_count,
                container_name="",  # Random container selection
                kill_signal=kill_signal,
                wait_duration=600,
            ),
            # Container pause scenario - temporary disruption without restart
            ContainerScenarios.container_pause(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=ceph_component_label,
                instance_count=max(1, instance_count // 2),  # Fewer instances for pause
                container_name="",  # Random container selection
                pause_seconds=pause_duration,
                wait_duration=480,
            ),
        ]

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
            if scenario.get("affected_pods", {}).get("error") is not None
        ]

        if failing_scenarios:
            log.error(
                f"Failed scenarios for {ceph_component_label}: {failing_scenarios}"
            )

        assert (
            not failing_scenarios
        ), f"Container chaos scenarios failed for {ceph_component_label} with pod errors: {failing_scenarios}"

        log.info(
            f"Container chaos test for {ceph_component_label} completed successfully"
        )
