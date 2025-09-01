"""
Test suite for Krkn hog chaos scenarios.

This module provides comprehensive tests for resource hog scenarios using the Krkn chaos engineering tool.
It includes tests for:
- CPU hog scenarios that stress CPU resources on worker nodes
- Memory hog scenarios that consume memory resources
- IO hog scenarios that stress disk I/O operations

The tests create VDBENCH workloads and inject resource exhaustion failures to validate system resilience.
"""

import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.krkn_chaos.krkn_scenario_generator import HogScenarios
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour

log = logging.getLogger(__name__)


@green_squad
@chaos
@polarion_id("OCS-1234")
class TestKrKnHogScenarios:
    """
    Test suite for Krkn resource hog chaos scenarios
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
        ids=["worker-nodes", "master-nodes"],
    )
    def test_run_krkn_hog_scenarios(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        node_selector,
        node_type,
    ):
        """
        Test resource hog scenarios (CPU, Memory, and IO) using Krkn chaos engineering.

        This test validates system resilience by injecting resource exhaustion scenarios
        on different node types (worker/master) while running VDBENCH workloads. It tests
        CPU hog, memory hog, and IO hog scenarios to ensure the storage system can handle
        resource stress on various node types.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture that provides pre-configured VDBENCH workloads
            node_selector: Parameterized node selector (worker or master)
            node_type: Parameterized node type string for logging purposes
        """
        log.info(f"Starting Krkn hog scenarios chaos test for {node_type} nodes")

        scenario_dir = krkn_scenario_directory
        ns = workload_ops.namespace
        selector = node_selector

        log.info(f"Creating hog scenarios for namespace: {ns}")
        log.info(f"Using node selector: {selector} (targeting {node_type} nodes)")

        # Warning for master node testing
        if node_type == "master":
            log.warning(
                "Testing hog scenarios on master nodes - this may impact cluster stability. "
                "Use reduced durations and monitor cluster health closely."
            )

        # Configure scenario parameters based on node type
        if node_type == "master":
            # Conservative settings for master nodes to avoid cluster disruption
            duration = 60  # Shorter duration for master nodes
            cpu_load_percentage = 70  # Lower CPU load percentage
            number_of_nodes = 1  # Target only 1 master node
            io_write_bytes = "500m"  # Smaller I/O operations
            log.info("Using conservative settings for master node testing")
        else:
            # Standard settings for worker nodes
            duration = 120
            cpu_load_percentage = 90
            number_of_nodes = 3
            io_write_bytes = "1g"
            log.info("Using standard settings for worker node testing")

        scenarios = [
            # CPU Hog Scenario: Stress CPU resources on target nodes
            HogScenarios.cpu_hog(
                scenario_dir,
                duration=duration,
                workers="''",
                namespace=ns,
                cpu_load_percentage=cpu_load_percentage,
                cpu_method="all",
                node_name=None,
                node_selector=selector,
                number_of_nodes=number_of_nodes,
                taints=[],
            ),
            # Memory Hog Scenario: Consume memory resources on target nodes
            HogScenarios.memory_hog(
                scenario_dir,
                duration=duration,
                namespace=ns,
                node_selector=selector,
                number_of_nodes=number_of_nodes,
            ),
            # IO Hog Scenario: Stress disk I/O operations on target nodes
            HogScenarios.io_hog(
                scenario_dir,
                duration=duration,
                namespace=ns,
                node_selector=selector,
                io_block_size="1m",
                io_write_bytes=io_write_bytes,
                io_target_pod_folder="/hog-data",
                io_target_pod_volume=None,
                number_of_nodes=number_of_nodes,
            ),
        ]

        log.info(
            f"Generated {len(scenarios)} hog scenarios (CPU, Memory, IO) for {node_type} nodes"
        )

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("hog_scenarios", scenario)
        config.set_tunings(wait_duration=60, iterations=2)
        config.write_to_file(location=scenario_dir)
        log.info("Krkn configuration file written successfully")

        # Execute Krkn chaos scenarios
        krkn = KrKnRunner(config.global_config)
        try:
            log.info(f"Starting hog scenarios chaos injection on {node_type} nodes")
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(
                f"Hog scenarios chaos injection completed successfully on {node_type} nodes"
            )
        except CommandFailed as e:
            log.error(
                f"Krkn command failed during hog scenarios on {node_type} nodes: {str(e)}"
            )
            pytest.fail(
                f"Krkn command failed during hog scenarios on {node_type} nodes: {str(e)}"
            )

        # Validate workloads and cleanup
        try:
            workload_ops.validate_and_cleanup()
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(
                f"Workload validation/cleanup issue during hog scenarios on {node_type} nodes: {str(e)}"
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
            log.error(f"Failed hog scenarios on {node_type} nodes: {failing_scenarios}")

        assert (
            not failing_scenarios
        ), f"Hog scenarios failed on {node_type} nodes with pod errors: {failing_scenarios}"

        log.info(
            f"Hog scenarios chaos test completed successfully on {node_type} nodes"
        )
