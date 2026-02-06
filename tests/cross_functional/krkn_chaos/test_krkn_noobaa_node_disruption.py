"""
Test suite for Krkn node disruption scenarios targeting NooBaa database nodes.

This module provides comprehensive node disruption testing specifically for nodes
hosting NooBaa database pods. The tests validate NooBaa's resilience when the
underlying infrastructure fails.

Node Disruption Actions:
- node_stop_start: Stop and restart the node
- node_reboot: Reboot the node

Target Nodes:
- Nodes hosting NooBaa DB primary pod (noobaa-db-pg-cluster-1)
- Nodes hosting NooBaa DB replica pods (noobaa-db-pg-cluster-2, etc.)
- Nodes hosting NooBaa core pods

The tests validate:
- NooBaa database failover from primary to replica
- S3 service availability during node disruption
- Data integrity after node recovery
- Pod rescheduling to healthy nodes
"""

import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.krkn_chaos.krkn_scenario_generator import NodeScenarios
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.krkn_chaos.krkn_helpers import (
    KrknResultAnalyzer,
    CephHealthHelper,
    ValidationHelper,
    get_krkn_cloud_type,
)
from ocs_ci.krkn_chaos.logging_helpers import log_test_start
from ocs_ci.krkn_chaos.noobaa_chaos_helper import (
    get_node_hosting_noobaa_db_primary,
    get_nodes_hosting_noobaa_db_replicas,
    get_nodes_hosting_noobaa_core,
    get_unique_noobaa_nodes,
)

log = logging.getLogger(__name__)


@green_squad
@chaos
class TestKrknNooBaaNodeDisruption:
    """
    Test suite for NooBaa node disruption scenarios.

    Tests node-level failures for nodes hosting NooBaa database and core pods,
    validating database failover and S3 service resilience.
    """

    @pytest.mark.parametrize(
        "action,target_component",
        [
            ("node_stop_start_scenario", "db_primary"),
            ("node_reboot_scenario", "db_primary"),
            ("node_stop_start_scenario", "db_replica"),
            ("node_reboot_scenario", "db_replica"),
            ("node_stop_start_scenario", "core"),
            ("node_reboot_scenario", "core"),
        ],
        ids=[
            "stop-start-db-primary",
            "reboot-db-primary",
            "stop-start-db-replica",
            "reboot-db-replica",
            "stop-start-core",
            "reboot-core",
        ],
    )
    @polarion_id("OCS-7347")
    def test_krkn_noobaa_node_disruption(
        self,
        request,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        action,
        target_component,
    ):
        """
        Test node disruption for nodes hosting NooBaa components.

        This test:
        1. Identifies nodes hosting NooBaa components (DB primary, replica, core)
        2. Performs node disruption (stop/start or reboot)
        3. Validates NooBaa recovery and S3 service availability
        4. Runs Warp S3 workload continuously during disruption

        Components:
        - db_primary: Node hosting NooBaa database primary pod
        - db_replica: Node hosting NooBaa database replica pod
        - core: Node hosting NooBaa core service pod

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture providing Warp S3 workloads
            action: Node action (stop_start or reboot)
            target_component: NooBaa component whose node to disrupt
        """
        scenario_dir = krkn_scenario_directory
        platform = config.ENV_DATA.get("platform", "").lower()
        cloud_type = get_krkn_cloud_type()

        # Get node hosting the target component
        log.info(f"Identifying node hosting NooBaa {target_component}")

        if target_component == "db_primary":
            target_node = get_node_hosting_noobaa_db_primary()
        elif target_component == "db_replica":
            replica_nodes = get_nodes_hosting_noobaa_db_replicas()
            if not replica_nodes:
                pytest.skip("No NooBaa DB replica pods found")
            target_node = replica_nodes[0]  # Target first replica node
        elif target_component == "core":
            core_nodes = get_nodes_hosting_noobaa_core()
            if not core_nodes:
                pytest.skip("No NooBaa core pods found")
            target_node = core_nodes[0]  # Target first core node
        else:
            raise ValueError(f"Unknown target component: {target_component}")

        log.info(f"Target node for {target_component}: {target_node}")

        # Use helper function for standardized test start logging
        log_test_start(
            f"NooBaa node disruption: {action} on {target_component}",
            target_node,
            platform=platform,
            cloud_type=cloud_type,
            action=action,
            component=target_component,
        )

        # WORKLOAD SETUP - Start Warp S3 workloads before disruption
        log.info("Setting up Warp S3 workloads for NooBaa node disruption testing")
        workload_ops.setup_workloads()

        # Register finalizer for cleanup
        request.addfinalizer(lambda: workload_ops.validate_and_cleanup())

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        try:
            # =================================================================
            # NODE DISRUPTION SCENARIO CONFIGURATION
            # =================================================================
            log.info(
                f"Creating node disruption configuration: {action} on node {target_node}"
            )

            # Create Krkn configuration
            krkn_config = KrknConfigGenerator()

            # Build scenario configuration
            scenario_params = {
                "actions": [action],
                "cloud_type": cloud_type,
                "node_name": target_node,  # Target specific node
                "instance_count": 1,
            }

            # Set timeout and duration based on action
            if action == "node_stop_start_scenario":
                scenario_params["timeout"] = 600  # Longer timeout for NooBaa recovery
                scenario_params["duration"] = 180  # 3 minutes down time
            else:  # reboot
                scenario_params["timeout"] = 300

            # Add platform-specific parameters
            if cloud_type in [
                constants.KRKN_CLOUD_AWS,
                constants.KRKN_CLOUD_AZURE,
            ]:
                scenario_params["parallel"] = False  # Sequential for specific node
                scenario_params["kube_check"] = True

            if cloud_type == constants.KRKN_CLOUD_AWS:
                scenario_params["poll_interval"] = 15

            if cloud_type == constants.KRKN_CLOUD_IBM:
                scenario_params["disable_ssl_verification"] = True

            if cloud_type == constants.KRKN_CLOUD_BAREMETAL:
                scenario_params["parallel"] = False
                scenario_params["kube_check"] = True

            if cloud_type == constants.KRKN_CLOUD_VMWARE:
                scenario_params["parallel"] = False

            # Generate node scenario YAML
            scenario_file = NodeScenarios.node_scenarios(
                scenario_dir=scenario_dir,
                cloud_type=cloud_type,
                scenarios=[scenario_params],
            )

            log.info(f"Created scenario file: {scenario_file}")

            # Add scenario to Krkn config
            krkn_config.add_scenario("node_scenarios", scenario_file)

            # =================================================================
            # EXECUTION: Node disruption
            # =================================================================
            log.info(
                f"Executing node disruption: {action} on {target_node} hosting {target_component}"
            )

            # Configure and write Krkn configuration
            krkn_config.set_tunings(wait_duration=120, iterations=1)
            krkn_config.write_to_file(location=krkn_scenario_directory)

            # Execute Krkn
            krkn_runner = KrKnRunner(krkn_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            log.info(f"Node disruption completed for {target_component}")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e,
                f"noobaa-{target_component}",
                f"NooBaa node disruption ({action})",
            )
            raise
        except Exception as e:
            log.error(
                f"❌ NooBaa node disruption failed for {target_component} with {action}: {e}"
            )
            raise

        # =================================================================
        # RESULTS ANALYSIS
        # =================================================================
        log.info(f"NOOBAA NODE DISRUPTION RESULTS ({target_component}):")

        # Analyze results
        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output,
                f"noobaa-{target_component}",
                detail_level="detailed",
            )
        )

        overall_success_rate = (
            (successful_executed / total_executed * 100) if total_executed > 0 else 0
        )

        log.info(
            f"EXECUTION RESULTS: Component: {target_component}, "
            f"Action: {action}, "
            f"Node: {target_node}, "
            f"Total scenarios: {total_executed}, "
            f"Successful: {successful_executed}, "
            f"Failed: {failing_executed}, "
            f"Success rate: {overall_success_rate:.1f}%"
        )

        # Validate success rate
        # DB primary disruption is more critical, lower threshold
        min_success_rates = {
            "db_primary": 60,  # Database failover takes time
            "db_replica": 75,  # Replica disruption less impact
            "core": 70,  # Core service disruption moderate impact
        }
        min_success_rate = min_success_rates.get(target_component, 70)

        validator.validate_chaos_execution(
            total_executed,
            successful_executed,
            f"noobaa-{target_component}",
            f"NooBaa node disruption ({action})",
        )

        analyzer.evaluate_chaos_success_rate(
            total_executed,
            successful_executed,
            f"noobaa-{target_component}",
            f"NooBaa node disruption ({action})",
            min_success_rate,
        )

        # Final health check
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            None, f"NooBaa node disruption ({target_component})"
        )
        assert no_crashes, crash_details

        log.info(
            f"🏆 NooBaa node disruption test for {target_component} completed successfully! "
            f"Node {target_node} recovered with {overall_success_rate:.1f}% success rate."
        )

    @pytest.mark.parametrize(
        "iterations",
        [2, 3, 5],
        ids=[
            "2-iterations",
            "3-iterations",
            "5-iterations",
        ],
    )
    @polarion_id("OCS-7348")
    def test_krkn_noobaa_db_primary_node_reboot_repeated(
        self,
        request,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        iterations,
    ):
        """
        Repeated node reboot test for NooBaa database primary node.

        This stress test repeatedly reboots the node hosting NooBaa database
        primary pod to validate:
        - Database failover mechanism under repeated stress
        - Pod rescheduling reliability
        - S3 service recovery consistency
        - Data integrity across multiple failovers

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture providing Warp S3 workloads
            iterations: Number of times to reboot the node
        """
        scenario_dir = krkn_scenario_directory
        platform = config.ENV_DATA.get("platform", "").lower()
        cloud_type = get_krkn_cloud_type()

        # Get node hosting NooBaa DB primary
        log.info("Identifying node hosting NooBaa database primary pod")
        target_node = get_node_hosting_noobaa_db_primary()
        log.info(f"Target node: {target_node}")

        # Use helper function for standardized test start logging
        log_test_start(
            f"NooBaa DB primary node reboot (repeated {iterations}x)",
            target_node,
            platform=platform,
            cloud_type=cloud_type,
            iterations=iterations,
        )

        # WORKLOAD SETUP
        log.info("Setting up Warp S3 workloads for repeated node reboot testing")
        workload_ops.setup_workloads()

        # Register finalizer for cleanup
        request.addfinalizer(lambda: workload_ops.validate_and_cleanup())

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        try:
            # =================================================================
            # REPEATED NODE REBOOT SCENARIO
            # =================================================================
            log.info(
                f"Creating repeated node reboot configuration: {iterations} iterations on {target_node}"
            )

            # Create Krkn configuration
            krkn_config = KrknConfigGenerator()

            # Build scenario configuration
            scenario_params = {
                "actions": ["node_reboot_scenario"],
                "cloud_type": cloud_type,
                "node_name": target_node,
                "instance_count": 1,
                "timeout": 300,
            }

            # Add platform-specific parameters
            if cloud_type in [
                constants.KRKN_CLOUD_AWS,
                constants.KRKN_CLOUD_AZURE,
            ]:
                scenario_params["parallel"] = False
                scenario_params["kube_check"] = True

            if cloud_type == constants.KRKN_CLOUD_AWS:
                scenario_params["poll_interval"] = 15

            if cloud_type == constants.KRKN_CLOUD_IBM:
                scenario_params["disable_ssl_verification"] = True

            if cloud_type == constants.KRKN_CLOUD_BAREMETAL:
                scenario_params["parallel"] = False
                scenario_params["kube_check"] = True

            if cloud_type == constants.KRKN_CLOUD_VMWARE:
                scenario_params["parallel"] = False

            # Generate node scenario YAML
            scenario_file = NodeScenarios.node_scenarios(
                scenario_dir=scenario_dir,
                cloud_type=cloud_type,
                scenarios=[scenario_params],
            )

            log.info(f"Created scenario file: {scenario_file}")

            # Add scenario to Krkn config
            krkn_config.add_scenario("node_scenarios", scenario_file)

            # =================================================================
            # EXECUTION: Repeated node reboots
            # =================================================================
            log.info(
                f"Executing repeated node reboot: {iterations} iterations on {target_node}"
            )

            # Configure with multiple iterations
            krkn_config.set_tunings(wait_duration=120, iterations=iterations)
            krkn_config.write_to_file(location=krkn_scenario_directory)

            # Execute Krkn
            krkn_runner = KrKnRunner(krkn_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            log.info(f"Repeated node reboot completed: {iterations} iterations")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e,
                "noobaa-db-primary-repeated",
                f"NooBaa DB primary node reboot ({iterations}x)",
            )
            raise
        except Exception as e:
            log.error(
                f"❌ NooBaa DB primary repeated node reboot failed ({iterations}x): {e}"
            )
            raise

        # =================================================================
        # RESULTS ANALYSIS
        # =================================================================
        log.info(f"REPEATED NODE REBOOT RESULTS ({iterations} iterations):")

        # Analyze results
        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output,
                "noobaa-db-primary-repeated",
                detail_level="detailed",
            )
        )

        overall_success_rate = (
            (successful_executed / total_executed * 100) if total_executed > 0 else 0
        )

        log.info(
            f"EXECUTION RESULTS: "
            f"Iterations requested: {iterations}, "
            f"Total executed: {total_executed}, "
            f"Successful: {successful_executed}, "
            f"Failed: {failing_executed}, "
            f"Success rate: {overall_success_rate:.1f}%"
        )

        # Validate success rate (lower threshold for repeated stress)
        min_success_rates = {
            2: 65,  # 2 iterations should have high success
            3: 60,  # 3 iterations moderate stress
            5: 55,  # 5 iterations high stress
        }
        min_success_rate = min_success_rates.get(iterations, 60)

        validator.validate_chaos_execution(
            total_executed,
            successful_executed,
            "noobaa-db-primary-repeated",
            f"NooBaa DB primary node reboot ({iterations}x)",
        )

        analyzer.evaluate_chaos_success_rate(
            total_executed,
            successful_executed,
            "noobaa-db-primary-repeated",
            f"NooBaa DB primary node reboot ({iterations}x)",
            min_success_rate,
        )

        # Final health check
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            None, f"NooBaa DB primary repeated reboot ({iterations}x)"
        )
        assert no_crashes, crash_details

        log.info(
            f"🏆 NooBaa DB primary repeated node reboot test completed successfully! "
            f"Node {target_node} recovered from {iterations} reboots with {overall_success_rate:.1f}% success rate."
        )

    @polarion_id("OCS-7349")
    def test_krkn_noobaa_all_nodes_stop_start(
        self,
        request,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
    ):
        """
        Stop and start all nodes hosting NooBaa components simultaneously.

        This extreme test stops all nodes hosting NooBaa pods at once to validate:
        - Complete NooBaa infrastructure failure
        - Cluster-wide recovery
        - S3 service restoration after total outage
        - Data integrity after catastrophic failure

        Warning: This is a destructive test that causes complete NooBaa outage.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture providing Warp S3 workloads
        """
        scenario_dir = krkn_scenario_directory
        platform = config.ENV_DATA.get("platform", "").lower()
        cloud_type = get_krkn_cloud_type()

        # Get all nodes hosting NooBaa components
        log.info("Identifying all nodes hosting NooBaa components")

        # Use helper function to get unique nodes
        all_nodes = list(get_unique_noobaa_nodes())

        log.info(f"Total unique nodes hosting NooBaa: {len(all_nodes)}")
        log.info(f"Nodes: {', '.join(all_nodes)}")

        if len(all_nodes) == 0:
            pytest.skip("No nodes found hosting NooBaa components")

        # Use helper function for standardized test start logging
        log_test_start(
            "NooBaa all nodes stop/start",
            f"{len(all_nodes)} nodes",
            platform=platform,
            cloud_type=cloud_type,
            nodes=all_nodes,
        )

        log.warning(
            f"⚠️  EXTREME TEST: This will stop ALL {len(all_nodes)} nodes hosting NooBaa simultaneously"
        )

        # WORKLOAD SETUP
        log.info("Setting up Warp S3 workloads (will fail during total outage)")
        workload_ops.setup_workloads()

        # Register finalizer for cleanup
        request.addfinalizer(lambda: workload_ops.validate_and_cleanup())

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        try:
            # =================================================================
            # ALL NODES STOP/START SCENARIO
            # =================================================================
            log.info(
                f"Creating all-nodes stop/start configuration for {len(all_nodes)} nodes"
            )

            # Create Krkn configuration
            krkn_config = KrknConfigGenerator()

            # Create scenarios for each node
            scenarios = []
            for node in all_nodes:
                scenario_params = {
                    "actions": ["node_stop_start_scenario"],
                    "cloud_type": cloud_type,
                    "node_name": node,
                    "instance_count": 1,
                    "timeout": 600,  # Longer timeout for recovery
                    "duration": 240,  # 4 minutes down time
                }

                # Add platform-specific parameters
                if cloud_type in [
                    constants.KRKN_CLOUD_AWS,
                    constants.KRKN_CLOUD_AZURE,
                ]:
                    scenario_params["parallel"] = True  # Stop all nodes simultaneously
                    scenario_params["kube_check"] = True

                if cloud_type == constants.KRKN_CLOUD_AWS:
                    scenario_params["poll_interval"] = 15

                if cloud_type == constants.KRKN_CLOUD_IBM:
                    scenario_params["disable_ssl_verification"] = True

                if cloud_type == constants.KRKN_CLOUD_BAREMETAL:
                    scenario_params["parallel"] = True
                    scenario_params["kube_check"] = True

                if cloud_type == constants.KRKN_CLOUD_VMWARE:
                    scenario_params["parallel"] = True

                scenarios.append(scenario_params)

            # Generate node scenario YAML
            scenario_file = NodeScenarios.node_scenarios(
                scenario_dir=scenario_dir,
                cloud_type=cloud_type,
                scenarios=scenarios,
            )

            log.info(f"Created scenario file with {len(scenarios)} node disruptions")

            # Add scenario to Krkn config
            krkn_config.add_scenario("node_scenarios", scenario_file)

            # =================================================================
            # EXECUTION: Stop/start all NooBaa nodes
            # =================================================================
            log.info(
                f"Executing all-nodes stop/start: {len(all_nodes)} nodes simultaneously"
            )

            # Configure and write Krkn configuration
            krkn_config.set_tunings(wait_duration=180, iterations=1)
            krkn_config.write_to_file(location=krkn_scenario_directory)

            # Execute Krkn
            krkn_runner = KrKnRunner(krkn_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            log.info("All-nodes stop/start completed")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e,
                "noobaa-all-nodes",
                "NooBaa all nodes stop/start",
            )
            raise
        except Exception as e:
            log.error(f"❌ NooBaa all nodes stop/start failed: {e}")
            raise

        # =================================================================
        # RESULTS ANALYSIS
        # =================================================================
        log.info("ALL NODES STOP/START RESULTS:")

        # Analyze results
        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output,
                "noobaa-all-nodes",
                detail_level="detailed",
            )
        )

        overall_success_rate = (
            (successful_executed / total_executed * 100) if total_executed > 0 else 0
        )

        log.info(
            f"EXECUTION RESULTS: "
            f"Nodes disrupted: {len(all_nodes)}, "
            f"Total scenarios: {total_executed}, "
            f"Successful: {successful_executed}, "
            f"Failed: {failing_executed}, "
            f"Success rate: {overall_success_rate:.1f}%"
        )

        # Validate success rate (very low threshold for catastrophic failure test)
        min_success_rate = 50  # Complete outage expected, focus on recovery

        validator.validate_chaos_execution(
            total_executed,
            successful_executed,
            "noobaa-all-nodes",
            "NooBaa all nodes stop/start",
        )

        analyzer.evaluate_chaos_success_rate(
            total_executed,
            successful_executed,
            "noobaa-all-nodes",
            "NooBaa all nodes stop/start",
            min_success_rate,
        )

        # Final health check
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            None, "NooBaa all nodes stop/start"
        )
        assert no_crashes, crash_details

        log.info(
            f"🏆 NooBaa all nodes stop/start test completed successfully! "
            f"All {len(all_nodes)} nodes recovered with {overall_success_rate:.1f}% success rate. "
            f"✨ NooBaa survived catastrophic infrastructure failure!"
        )
