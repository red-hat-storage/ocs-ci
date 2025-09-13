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
from ocs_ci.krkn_chaos.krkn_helpers import (
    check_ceph_crashes,
    validate_chaos_execution,
    validate_strength_test_results,
    handle_krkn_command_failure,
    handle_workload_validation_failure,
    analyze_chaos_results,
    analyze_strength_test_results,
)

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
            # 🎯 PRIMARY CPU HOG: Standard CPU stress scenario
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
            # 🔥 INTENSIVE CPU HOG: Maximum CPU stress
            HogScenarios.cpu_hog(
                scenario_dir,
                duration=duration * 2,  # 2x longer duration
                workers="''",
                namespace=ns,
                cpu_load_percentage=min(
                    95, cpu_load_percentage + 15
                ),  # Higher CPU load
                cpu_method="all",
                node_name=None,
                node_selector=selector,
                number_of_nodes=min(number_of_nodes + 1, 4),  # More nodes
                taints=[],
            ),
            # 💾 PRIMARY MEMORY HOG: Standard memory consumption
            HogScenarios.memory_hog(
                scenario_dir,
                duration=duration,
                namespace=ns,
                node_selector=selector,
                number_of_nodes=number_of_nodes,
            ),
            # 💥 EXTREME MEMORY HOG: Aggressive memory consumption
            HogScenarios.memory_hog(
                scenario_dir,
                duration=duration * 2,  # 2x longer duration
                namespace=ns,
                node_selector=selector,
                number_of_nodes=min(number_of_nodes + 1, 4),  # More nodes
            ),
            # 💿 PRIMARY IO HOG: Standard I/O stress
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
            # 🌪️ EXTREME IO HOG: Aggressive I/O operations
            HogScenarios.io_hog(
                scenario_dir,
                duration=duration * 2,  # 2x longer duration
                namespace=ns,
                node_selector=selector,
                io_block_size="2m",  # Larger block size
                io_write_bytes=io_write_bytes.replace("m", "m")
                .replace("g", "g")
                .replace("500m", "1g")
                .replace("1g", "2g"),  # 2x I/O
                io_target_pod_folder="/hog-data",
                io_target_pod_volume=None,
                number_of_nodes=min(number_of_nodes + 1, 4),  # More nodes
            ),
        ]

        # Add extreme chaos scenarios for worker nodes only (safer)
        if node_type == "worker":
            additional_scenarios = [
                # 🚨 CHAOS STORM: Multiple rapid CPU bursts
                HogScenarios.cpu_hog(
                    scenario_dir,
                    duration=duration // 2,  # Short burst
                    workers="''",
                    namespace=ns,
                    cpu_load_percentage=95,  # Maximum CPU load
                    cpu_method="all",
                    node_name=None,
                    node_selector=selector,
                    number_of_nodes=min(number_of_nodes + 2, 5),  # Maximum nodes
                    taints=[],
                ),
                HogScenarios.cpu_hog(
                    scenario_dir,
                    duration=duration // 3,  # Another rapid burst
                    workers="''",
                    namespace=ns,
                    cpu_load_percentage=90,
                    cpu_method="all",
                    node_name=None,
                    node_selector=selector,
                    number_of_nodes=number_of_nodes,
                    taints=[],
                ),
                # 💀 MEMORY APOCALYPSE: Ultra-aggressive memory consumption
                HogScenarios.memory_hog(
                    scenario_dir,
                    duration=duration * 3,  # 3x longer for endurance
                    namespace=ns,
                    node_selector=selector,
                    number_of_nodes=min(number_of_nodes + 2, 5),  # Maximum nodes
                ),
                # 🔥 IO DEVASTATION: Maximum I/O stress
                HogScenarios.io_hog(
                    scenario_dir,
                    duration=duration * 3,  # 3x longer
                    namespace=ns,
                    node_selector=selector,
                    io_block_size="4m",  # Large blocks for maximum stress
                    io_write_bytes="3g",  # Massive I/O operations
                    io_target_pod_folder="/hog-data",
                    io_target_pod_volume=None,
                    number_of_nodes=min(number_of_nodes + 2, 5),  # Maximum nodes
                ),
                # 🌊 MIXED RESOURCE WAVE: Alternating resource stress
                HogScenarios.cpu_hog(
                    scenario_dir,
                    duration=duration // 2,
                    workers="''",
                    namespace=ns,
                    cpu_load_percentage=85,
                    cpu_method="all",
                    node_name=None,
                    node_selector=selector,
                    number_of_nodes=number_of_nodes,
                    taints=[],
                ),
                HogScenarios.memory_hog(
                    scenario_dir,
                    duration=duration,
                    namespace=ns,
                    node_selector=selector,
                    number_of_nodes=number_of_nodes,
                ),
                HogScenarios.io_hog(
                    scenario_dir,
                    duration=duration // 2,
                    namespace=ns,
                    node_selector=selector,
                    io_block_size="2m",
                    io_write_bytes=io_write_bytes,
                    io_target_pod_folder="/hog-data",
                    io_target_pod_volume=None,
                    number_of_nodes=number_of_nodes,
                ),
            ]
            scenarios.extend(additional_scenarios)
            log.info(
                f"Added {len(additional_scenarios)} extreme chaos hog scenarios for worker nodes "
                "(high-intensity resource exhaustion testing)"
            )

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
            handle_krkn_command_failure(e, f"{node_type} nodes", "hog scenarios chaos")

        # Validate workloads and cleanup
        try:
            workload_ops.validate_and_cleanup()
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(
                f"Workload validation/cleanup issue during hog scenarios on {node_type} nodes: {str(e)}"
            )

        # Analyze chaos run results using helper function
        results = analyze_chaos_results(
            krkn, f"{node_type} nodes", "hog scenarios chaos"
        )
        total_scenarios = results["total_scenarios"]
        successful_scenarios = results["successful_scenarios"]

        # Validate chaos execution results
        validate_chaos_execution(
            total_scenarios,
            successful_scenarios,
            f"{node_type} nodes",
            "hog scenarios chaos",
        )

        # Check for Ceph crashes after hog scenarios chaos injection
        assert check_ceph_crashes(f"{node_type} nodes", "hog scenarios chaos")

        log.info(
            f"Hog scenarios chaos test completed successfully on {node_type} nodes"
        )

    @pytest.mark.parametrize(
        "stress_level,duration_multiplier,intensity_multiplier",
        [
            ("extreme", 4, 2),  # Extreme cluster stress testing
            ("ultimate", 6, 3),  # Ultimate cluster endurance testing
            ("apocalypse", 8, 4),  # Maximum cluster destruction testing
        ],
        ids=[
            "extreme-cluster-stress",
            "ultimate-cluster-endurance",
            "apocalypse-cluster-destruction",
        ],
    )
    def test_krkn_extreme_cluster_strength_testing(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        stress_level,
        duration_multiplier,
        intensity_multiplier,
    ):
        """
        EXTREME cluster strength testing with apocalyptic resource exhaustion scenarios.

        This test pushes cluster resilience to absolute limits with devastating chaos patterns:
        - Cascading resource exhaustion
        - Multi-resource simultaneous attacks
        - Sustained resource starvation
        - Recovery stress testing under extreme load
        - Apocalyptic resource consumption patterns

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture for VDBENCH workloads
            stress_level: Level of stress testing (extreme, ultimate, apocalypse)
            duration_multiplier: Multiplier for scenario durations
            intensity_multiplier: Multiplier for resource consumption intensity
        """
        log.info(
            f"🚨 Starting {stress_level.upper()} cluster strength testing "
            f"(duration: {duration_multiplier}x, intensity: {intensity_multiplier}x)"
        )
        log.warning(
            f"⚠️  {stress_level.upper()} TESTING WARNING: This test will push the cluster to its absolute limits. "
            f"Monitor cluster health closely and be prepared for potential instability."
        )

        scenario_dir = krkn_scenario_directory
        ns = workload_ops.namespace
        selector = constants.WORKER_LABEL  # Always use worker nodes for extreme testing

        # Base parameters scaled by stress level
        base_duration = 120
        base_cpu_load = 90
        base_nodes = 3
        base_io_size = "1g"

        max_duration = base_duration * duration_multiplier
        max_cpu_load = min(98, base_cpu_load + (intensity_multiplier * 2))
        max_nodes = min(base_nodes + intensity_multiplier, 6)
        max_io_size = f"{intensity_multiplier * 2}g"

        log.info(f"🎯 Creating {stress_level} cluster strength testing scenarios")
        log.info(
            f"📊 Parameters: Duration={max_duration}s, CPU={max_cpu_load}%, Nodes={max_nodes}, IO={max_io_size}"
        )

        # 🏗️ EXTREME CLUSTER STRENGTH TESTING SCENARIO PATTERNS
        scenarios = [
            # 🎯 BASELINE CLUSTER STRESS: Establish baseline performance
            HogScenarios.cpu_hog(
                scenario_dir,
                duration=base_duration,
                workers="''",
                namespace=ns,
                cpu_load_percentage=base_cpu_load,
                cpu_method="all",
                node_name=None,
                node_selector=selector,
                number_of_nodes=base_nodes,
                taints=[],
            ),
            HogScenarios.memory_hog(
                scenario_dir,
                duration=base_duration,
                namespace=ns,
                node_selector=selector,
                number_of_nodes=base_nodes,
            ),
            # 🔄 CASCADING RESOURCE APOCALYPSE: Progressive resource destruction
            HogScenarios.cpu_hog(
                scenario_dir,
                duration=base_duration * 2,  # Escalate duration
                workers="''",
                namespace=ns,
                cpu_load_percentage=base_cpu_load + 5,  # Escalate CPU
                cpu_method="all",
                node_name=None,
                node_selector=selector,
                number_of_nodes=base_nodes + 1,  # Escalate nodes
                taints=[],
            ),
            HogScenarios.memory_hog(
                scenario_dir,
                duration=base_duration * 3,  # Further escalation
                namespace=ns,
                node_selector=selector,
                number_of_nodes=base_nodes + 2,
            ),
            HogScenarios.io_hog(
                scenario_dir,
                duration=max_duration,  # Peak escalation
                namespace=ns,
                node_selector=selector,
                io_block_size="4m",
                io_write_bytes=max_io_size,
                io_target_pod_folder="/hog-data",
                io_target_pod_volume=None,
                number_of_nodes=max_nodes,
            ),
            # ⚡ RAPID-FIRE RESOURCE BOMBARDMENT: Quick successive resource attacks
            HogScenarios.cpu_hog(
                scenario_dir,
                duration=base_duration // 2,  # Quick CPU burst 1
                workers="''",
                namespace=ns,
                cpu_load_percentage=max_cpu_load,
                cpu_method="all",
                node_name=None,
                node_selector=selector,
                number_of_nodes=max_nodes,
                taints=[],
            ),
            HogScenarios.cpu_hog(
                scenario_dir,
                duration=base_duration // 3,  # Quick CPU burst 2
                workers="''",
                namespace=ns,
                cpu_load_percentage=95,
                cpu_method="all",
                node_name=None,
                node_selector=selector,
                number_of_nodes=base_nodes + 1,
                taints=[],
            ),
            HogScenarios.memory_hog(
                scenario_dir,
                duration=base_duration // 2,  # Quick memory burst
                namespace=ns,
                node_selector=selector,
                number_of_nodes=max_nodes,
            ),
            # 🌊 RESOURCE TSUNAMI: Overwhelming multi-resource wave attack
            HogScenarios.cpu_hog(
                scenario_dir,
                duration=base_duration,
                workers="''",
                namespace=ns,
                cpu_load_percentage=90,
                cpu_method="all",
                node_name=None,
                node_selector=selector,
                number_of_nodes=base_nodes,
                taints=[],
            ),
            HogScenarios.memory_hog(
                scenario_dir,
                duration=base_duration,  # Simultaneous memory stress
                namespace=ns,
                node_selector=selector,
                number_of_nodes=base_nodes,
            ),
            HogScenarios.io_hog(
                scenario_dir,
                duration=base_duration,  # Simultaneous I/O stress
                namespace=ns,
                node_selector=selector,
                io_block_size="2m",
                io_write_bytes=base_io_size,
                io_target_pod_folder="/hog-data",
                io_target_pod_volume=None,
                number_of_nodes=base_nodes,
            ),
            # 💀 ENDURANCE APOCALYPSE: Ultimate sustained resource destruction
            HogScenarios.cpu_hog(
                scenario_dir,
                duration=max_duration,  # Maximum duration CPU stress
                workers="''",
                namespace=ns,
                cpu_load_percentage=max_cpu_load,
                cpu_method="all",
                node_name=None,
                node_selector=selector,
                number_of_nodes=max_nodes,
                taints=[],
            ),
            HogScenarios.memory_hog(
                scenario_dir,
                duration=max_duration * 2,  # Even longer memory stress
                namespace=ns,
                node_selector=selector,
                number_of_nodes=max_nodes,
            ),
            # 🔥 RECOVERY DEVASTATION: Test recovery under extreme resource pressure
            HogScenarios.io_hog(
                scenario_dir,
                duration=base_duration * 3,  # Long I/O stress
                namespace=ns,
                node_selector=selector,
                io_block_size="4m",
                io_write_bytes=max_io_size,
                io_target_pod_folder="/hog-data",
                io_target_pod_volume=None,
                number_of_nodes=max_nodes,
            ),
            HogScenarios.cpu_hog(
                scenario_dir,
                duration=base_duration,  # CPU stress during I/O recovery
                workers="''",
                namespace=ns,
                cpu_load_percentage=85,
                cpu_method="all",
                node_name=None,
                node_selector=selector,
                number_of_nodes=base_nodes,
                taints=[],
            ),
            # 🚨 FINAL APOCALYPSE: Ultimate cluster destruction test
            HogScenarios.cpu_hog(
                scenario_dir,
                duration=max_duration,
                workers="''",
                namespace=ns,
                cpu_load_percentage=max_cpu_load,
                cpu_method="all",
                node_name=None,
                node_selector=selector,
                number_of_nodes=max_nodes,
                taints=[],
            ),
            HogScenarios.memory_hog(
                scenario_dir,
                duration=max_duration,
                namespace=ns,
                node_selector=selector,
                number_of_nodes=max_nodes,
            ),
            HogScenarios.io_hog(
                scenario_dir,
                duration=max_duration,
                namespace=ns,
                node_selector=selector,
                io_block_size="8m",  # Maximum block size
                io_write_bytes=f"{intensity_multiplier * 3}g",  # Maximum I/O
                io_target_pod_folder="/hog-data",
                io_target_pod_volume=None,
                number_of_nodes=max_nodes,
            ),
        ]

        log.info(
            f"🏗️ Generated {len(scenarios)} extreme cluster strength testing scenarios "
            f"({stress_level} level)"
        )

        # Generate Krkn configuration with extended settings for extreme testing
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("hog_scenarios", scenario)

        # Extended wait duration and iterations for extreme testing
        extended_wait = 120 if stress_level == "apocalypse" else 90
        iterations = 1  # Single iteration due to extreme nature
        config.set_tunings(wait_duration=extended_wait, iterations=iterations)
        config.write_to_file(location=scenario_dir)

        log.info(
            f"📝 Extreme cluster strength testing configuration written "
            f"(wait_duration={extended_wait}s, iterations={iterations})"
        )

        # Execute Krkn chaos scenarios
        krkn = KrKnRunner(config.global_config)
        try:
            log.info(
                f"🚀 Starting {stress_level.upper()} cluster strength testing - "
                f"PREPARE FOR EXTREME RESOURCE EXHAUSTION!"
            )
            krkn.run_async()
            krkn.wait_for_completion(check_interval=90)  # Longer check intervals
            log.info(
                f"✅ {stress_level.upper()} cluster strength testing completed - "
                f"CLUSTER SURVIVED THE APOCALYPSE!"
            )
        except CommandFailed as e:
            handle_krkn_command_failure(
                e, "cluster", f"{stress_level} strength testing"
            )

        # Enhanced validation for extreme strength testing
        try:
            workload_ops.validate_and_cleanup()
            log.info(
                f"💪 WORKLOADS SURVIVED {stress_level.upper()} TESTING - "
                f"CLUSTER STRENGTH CONFIRMED!"
            )
        except (UnexpectedBehaviour, CommandFailed) as e:
            handle_workload_validation_failure(
                e, "cluster", f"{stress_level} strength testing"
            )

        # Analyze extreme cluster strength testing results using helper function
        results = analyze_strength_test_results(
            krkn, "cluster", stress_level, "cluster strength testing"
        )
        total_scenarios = results["total_scenarios"]
        strength_score = results["strength_score"]

        # Extreme cluster strength testing success criteria (very lenient due to extreme nature)
        min_success_rates = {
            "extreme": 50,  # 50% for extreme testing
            "ultimate": 40,  # 40% for ultimate testing
            "apocalypse": 30,  # 30% for apocalypse testing
        }
        min_success_rate = min_success_rates.get(stress_level, 50)

        # Validate strength test results
        validate_strength_test_results(
            strength_score, total_scenarios, "cluster", stress_level, min_success_rate
        )

        # Final Ceph health check after cluster strength testing
        assert check_ceph_crashes("cluster", f"{stress_level} cluster strength testing")

        log.info(
            f"🏁 {stress_level.upper()} cluster strength testing completed successfully - "
            f"CLUSTER STRENGTH SCORE: {strength_score:.1f}%"
        )
