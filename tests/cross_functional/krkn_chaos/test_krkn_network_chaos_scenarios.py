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
from ocs_ci.krkn_chaos.krkn_helpers import (
    NetworkPortHelper,  # Network port helper
    NetworkScenarioHelper,  # Network scenario helper
    CephHealthHelper,  # Ceph health helper
    ValidationHelper,  # Validation helper
)

log = logging.getLogger(__name__)


@green_squad
@chaos
class TestKrKnNetworkChaosScenarios:
    """
    Test suite for Krkn network outage chaos scenarios
    """

    @pytest.mark.parametrize(
        "ceph_component_label,instance_count",
        [
            (OSD_APP_LABEL, 2),  # OSDs can handle multiple failures
            (MGR_APP_LABEL, 1),  # Critical: active/standby pair - conservative
            (MON_APP_LABEL, 1),  # Critical: NEVER >1 (breaks quorum)
            (MDS_APP_LABEL, 1),  # Critical: usually 1-2 active - conservative
            (RGW_APP_LABEL, 2),  # HA design: multiple gateways expected
        ],
        ids=["osd-2pods", "mgr-1pod", "mon-1pod", "mds-1pod", "rgw-2pods"],
    )
    @polarion_id("OCS-7342")
    def test_krkn_ceph_component_network_outage(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        instance_count,
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
            instance_count: Number of pods to target for chaos injection
        """
        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        log.info(
            f"Testing network outage for Ceph component: {ceph_component_label} "
            f"with instance_count={instance_count}"
        )

        # WORKLOAD SETUP - Start workloads and background cluster operations
        log.info("Setting up workloads for network outage testing")
        workload_ops.setup_workloads()

        try:
            port_helper = NetworkPortHelper(
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            # Use the comprehensive dynamic port discovery method
            dynamic_port_mapping = port_helper.get_dynamic_port_ranges()
            # Combine all discovered ports into a single list
            dynamic_ceph_ports = []
            for component_ports in dynamic_port_mapping.values():
                dynamic_ceph_ports.extend(component_ports)
            dynamic_ceph_ports = sorted(
                list(set(dynamic_ceph_ports))
            )  # Remove duplicates and sort
            log.info(
                f"Dynamically discovered {len(dynamic_ceph_ports)} Ceph ports "
                f"from {len(dynamic_port_mapping)} components: {dynamic_ceph_ports}"
            )
        except Exception as e:
            log.warning(f"Failed to discover dynamic ports, using fallback: {e}")
            # Fallback to common Ceph ports
            dynamic_ceph_ports = [
                6789,
                6800,
                6801,
                6802,
                6803,
                6804,
                6805,
                3300,
                7000,
                9283,
            ]

        # Initialize NetworkScenarioHelper for intelligent scenario generation
        network_helper = NetworkScenarioHelper(
            scenario_dir=scenario_dir, namespace=openshift_storage_ns
        )

        # Determine component criticality for intelligent scenario generation
        component_name = network_helper.extract_component_name(ceph_component_label)
        is_critical = network_helper.is_critical_component(component_name)

        # Generate scenarios with progressive intensity levels
        scenarios = []

        # LEVEL 1: Basic egress and ingress blocking
        basic_outage = network_helper.create_pod_network_outage(
            label_selector=ceph_component_label,
            instance_count=instance_count,
            direction=["egress", "ingress"],
            test_duration=300,
        )
        scenarios.append(basic_outage)

        # LEVEL 2: Egress-only blocking with dynamic ports
        egress_outage = network_helper.create_pod_network_outage(
            label_selector=ceph_component_label,
            instance_count=instance_count,
            direction=["egress"],
            egress_ports=dynamic_ceph_ports,
            test_duration=180,
        )
        scenarios.append(egress_outage)

        # LEVEL 3: Ingress-only blocking
        ingress_outage = network_helper.create_pod_network_outage(
            label_selector=ceph_component_label,
            instance_count=instance_count,
            direction=["ingress"],
            test_duration=120,
        )
        scenarios.append(ingress_outage)

        # LEVEL 4: High intensity scenarios (only for resilient components)
        if not is_critical:
            # Maximum outage chaos - complete isolation
            max_outage = network_helper.create_pod_network_outage(
                label_selector=ceph_component_label,
                instance_count=min(instance_count * 2, 6),
                direction=["egress", "ingress"],
                test_duration=300,
            )
            scenarios.append(max_outage)

            # Extended duration chaos with port blocking
            extended_outage = network_helper.create_pod_network_outage(
                label_selector=ceph_component_label,
                instance_count=min(instance_count * 3, 8),
                direction=["egress", "ingress"],
                egress_ports=dynamic_ceph_ports + [80, 443, 8080, 8443, 9090, 9443],
                ingress_ports=dynamic_ceph_ports + [80, 443, 8080, 8443, 9090, 9443],
                test_duration=300,
            )
            scenarios.append(extended_outage)

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
            validator = ValidationHelper()
            validator.handle_krkn_command_failure(
                e, ceph_component_label, "network chaos"
            )

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

        # Check for Ceph crashes after network outage chaos injection
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            ceph_component_label, "network outage chaos"
        )
        assert no_crashes, crash_details

        log.info(
            f"Network outage test for {ceph_component_label} completed successfully"
        )

    @pytest.mark.parametrize(
        "ceph_component_label,instance_count,duration,execution,egress_config",
        [
            (
                OSD_APP_LABEL,
                2,
                300,
                "parallel",
                {"latency": "50ms", "loss": "7%"},  # Basic: 50ms latency, 5-10% loss
            ),  # OSDs with network latency and packet loss
            (
                MGR_APP_LABEL,
                1,
                240,
                "serial",
                {
                    "latency": "100ms",
                    "loss": "15%",
                },  # Moderate: 100ms latency, 10-20% loss
            ),  # MGR with higher latency and loss
            (
                MON_APP_LABEL,
                1,
                180,
                "serial",
                {
                    "latency": "50ms",
                    "loss": "5%",
                },  # Basic: 50ms latency, 5-10% loss (conservative for MON)
            ),  # MON with minimal disruption (critical component)
            (
                MDS_APP_LABEL,
                1,
                240,
                "serial",
                {"latency": "50ms", "loss": "8%"},  # Basic: 50ms latency, 5-10% loss
            ),  # MDS with moderate network impairment
            (
                RGW_APP_LABEL,
                2,
                300,
                "parallel",
                {"latency": "50ms", "loss": "9%"},  # Basic: 50ms latency, 5-10% loss
            ),  # RGW with network impairment
        ],
        ids=[
            "osd-network-chaos",
            "mgr-network-chaos",
            "mon-network-chaos",
            "mds-network-chaos",
            "rgw-network-chaos",
        ],
    )
    @polarion_id("OCS-7343")
    def test_krkn_network_chaos(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        instance_count,
        duration,
        execution,
        egress_config,
    ):
        """
        Test network chaos scenarios using Krkn network_chaos template.

        This test validates network resilience by injecting network chaos
        (latency and packet loss) into different Ceph components and verifying
        that the system can handle these network impairments gracefully.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario files
            workload_ops: Workload operations fixture
            ceph_component_label: Parameterized Ceph component label
            instance_count: Number of pods to target for chaos injection
            duration: Duration in seconds for network chaos
            execution: Execution mode ('serial' or 'parallel')
            egress_config: Egress network impairment configuration
        """
        scenario_dir = krkn_scenario_directory
        log.info(
            f"Testing network chaos for Ceph component: {ceph_component_label} "
            f"with instance_count={instance_count}, duration={duration}s, "
            f"execution={execution}, egress_config={egress_config}"
        )

        # WORKLOAD SETUP - Start workloads and background cluster operations
        log.info("Setting up workloads for network chaos testing")
        workload_ops.setup_workloads()

        port_helper = NetworkPortHelper(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)

        # Get active network interfaces from worker nodes
        worker_interfaces = port_helper.get_default_network_interfaces(
            node_type="worker"
        )

        # Get active network interfaces from all nodes (worker + master)
        all_interfaces = port_helper.get_default_network_interfaces(node_type="all")

        # Create network chaos scenarios targeting the specific Ceph component
        # Initialize NetworkScenarioHelper for intelligent scenario generation
        network_helper = NetworkScenarioHelper(
            scenario_dir=scenario_dir, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        # Determine component criticality for intelligent scenario generation
        component_name = network_helper.extract_component_name(ceph_component_label)
        is_critical = network_helper.is_critical_component(component_name)

        # Generate scenarios with progressive intensity levels
        # Chaos Level Guidelines:
        # - Basic: 50ms latency, 5-10% packet loss
        # - Moderate: 100ms latency, 10-20% packet loss
        # - High: 200ms latency, up to 50% packet loss
        scenarios = []

        # LEVEL 1: Basic network chaos with mild impairment
        # Use node selector instead of pod selector for network chaos
        node_selector = "node-role.kubernetes.io/worker"
        basic_scenario = NetworkOutageScenarios.pod_network_chaos(
            scenario_dir,
            duration=duration,
            label_selector=node_selector,  # Target nodes, not pods
            instance_count=min(instance_count, 2),  # Limit to 2 nodes for safety
            interfaces=worker_interfaces,
            execution=execution,
            egress=egress_config,
        )
        scenarios.append(basic_scenario)

        # LEVEL 2: Multi-interface chaos with moderate impairment
        moderate_scenario = NetworkOutageScenarios.pod_network_chaos(
            scenario_dir,
            duration=duration // 2 if is_critical else duration,
            label_selector=node_selector,  # Target nodes, not pods
            instance_count=min(
                2, max(1, instance_count // 2) if is_critical else instance_count
            ),
            interfaces=all_interfaces,
            execution="serial" if is_critical else "parallel",
            egress={
                "latency": "100ms",  # Moderate: 100ms latency
                "loss": "8%" if is_critical else "15%",  # Moderate: 10-20% loss
            },
        )
        scenarios.append(moderate_scenario)

        # LEVEL 3: High intensity chaos - ALL components get tested!
        high_intensity_scenario = NetworkOutageScenarios.pod_network_chaos(
            scenario_dir,
            duration=duration * (1.5 if is_critical else 2),  # Careful vs full chaos
            label_selector=node_selector,  # Target nodes, not pods
            instance_count=min(
                2, 1 if is_critical else instance_count * 2
            ),  # Max 2 nodes
            interfaces=all_interfaces,
            execution="serial" if is_critical else "parallel",
            egress={
                "latency": "200ms",  # High: 200ms latency
                "loss": "15%" if is_critical else "25%",  # High: up to 50% loss
            },
        )
        scenarios.append(high_intensity_scenario)

        # LEVEL 4: Maximum chaos - ALL components get tested with appropriate intensity!
        maximum_scenario = NetworkOutageScenarios.pod_network_chaos(
            scenario_dir,
            duration=duration * (2 if is_critical else 3),  # Careful vs full chaos
            label_selector=node_selector,  # Target nodes, not pods
            instance_count=min(
                3, 1 if is_critical else instance_count * 3
            ),  # Max 3 nodes
            interfaces=worker_interfaces + all_interfaces,
            execution="serial" if is_critical else "parallel",
            egress={
                "latency": "200ms",  # Maximum: 200ms latency (same as high for safety)
                "loss": "20%" if is_critical else "50%",  # Maximum: up to 50% loss
            },
        )
        scenarios.append(maximum_scenario)

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("network_chaos_scenarios", scenario)
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
            validator = ValidationHelper()
            validator.handle_krkn_command_failure(
                e, ceph_component_label, "network chaos"
            )

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
        ), f"Network chaos scenarios failed for {ceph_component_label} with pod errors: {failing_scenarios}"

        # Check for Ceph crashes after network chaos injection
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            ceph_component_label, "network chaos"
        )
        assert no_crashes, crash_details

        log.info(
            f"Network chaos test for {ceph_component_label} completed successfully"
        )

    @pytest.mark.parametrize(
        "node_label_selector,instance_count,execution_type,network_params",
        [
            (
                constants.WORKER_LABEL,
                2,
                "parallel",
                {"latency": "50ms", "loss": "8%", "bandwidth": "50mbit"},
            ),  # Worker nodes with network ingress chaos
            (
                constants.MASTER_LABEL,
                1,
                "serial",
                {"latency": "50ms", "loss": "5%", "bandwidth": "100mbit"},
            ),  # Master nodes with conservative ingress chaos
        ],
        ids=["worker-ingress-chaos", "master-ingress-chaos"],
    )
    def test_krkn_network_chaos_ingress(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        node_label_selector,
        instance_count,
        execution_type,
        network_params,
    ):
        """
        Test network chaos ingress scenarios using Krkn network_chaos_ingress template.

        This test validates network resilience by injecting ingress network chaos
        (latency, packet loss, bandwidth limitation) at the node level and verifying
        that the system can handle these network impairments gracefully.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario files
            workload_ops: Workload operations fixture
            node_label_selector: Node label selector for targeting
            instance_count: Number of nodes to target for chaos injection
            execution_type: Execution mode ('serial' or 'parallel')
            network_params: Network impairment configuration
        """
        scenario_dir = krkn_scenario_directory

        node_type = "worker" if "worker" in node_label_selector else "master"
        log.info(
            f"Testing network ingress chaos for {node_type} nodes "
            f"with instance_count={instance_count}, execution_type={execution_type}, "
            f"network_params={network_params}"
        )

        # WORKLOAD SETUP - Start workloads and background cluster operations
        log.info("Setting up workloads for network ingress chaos testing")
        workload_ops.setup_workloads()

        # Warning for master node testing
        if node_type == "master":
            log.warning(
                "Running network chaos on master nodes - this could affect cluster stability!"
            )

        # Initialize NetworkScenarioHelper for intelligent scenario generation
        network_helper = NetworkScenarioHelper(
            scenario_dir=scenario_dir, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        # Determine node criticality for intelligent scenario generation
        is_master_node = node_type == "master"

        # Generate scenarios with progressive intensity levels
        scenarios = [
            # LEVEL 1: Primary ingress chaos scenario using helper
            network_helper.create_network_chaos_ingress(
                label_selector=node_label_selector,
                instance_count=instance_count if not is_master_node else 1,
                test_duration=300,
            ),
            # LEVEL 2: Secondary scenario with conservative settings
            network_helper.create_network_chaos_ingress(
                label_selector=node_label_selector,
                instance_count=max(1, instance_count // 2),
                test_duration=180,
            ),
            # LEVEL 3: High-intensity cluster capacity stress (only for worker nodes)
            network_helper.create_network_chaos_ingress(
                label_selector=node_label_selector,
                instance_count=(
                    min(2, instance_count * 2) if not is_master_node else 1
                ),  # Max 2 nodes for safety
                test_duration=300,  # Reduced duration for faster testing
            ),
            # LEVEL 4: Extreme network degradation (only for worker nodes)
            network_helper.create_network_chaos_ingress(
                label_selector=node_label_selector,
                instance_count=instance_count if not is_master_node else 1,
                test_duration=300,
            ),
            # LEVEL 5: Burst capacity test (only for worker nodes)
            network_helper.create_network_chaos_ingress(
                label_selector=node_label_selector,
                instance_count=max(2, instance_count) if not is_master_node else 1,
                test_duration=300,  # Reduced duration for faster testing
            ),
            # LEVEL 6: Multi-phase capacity stress (only for worker nodes)
            network_helper.create_network_chaos_ingress(
                label_selector=node_label_selector,
                instance_count=(
                    min(3, instance_count + 1) if not is_master_node else 1
                ),  # Max 3 nodes available
                test_duration=300,  # Reduced duration for faster testing
            ),
        ]

        # Add extreme scenarios only for worker nodes (too intense for master nodes)
        if not is_master_node:
            extreme_scenarios = [
                # LEVEL 7: Maximum chaos - catastrophic network degradation
                network_helper.create_network_chaos_ingress(
                    label_selector=node_label_selector,
                    instance_count=min(6, instance_count * 3),  # Target MANY more nodes
                    test_duration=300,  # Reduced duration for faster testing
                ),
                # LEVEL 8: Total network meltdown scenario
                network_helper.create_network_chaos_ingress(
                    label_selector=node_label_selector,
                    instance_count=min(
                        3, instance_count * 2
                    ),  # Max 3 worker nodes available
                    test_duration=300,  # Reduced duration for faster testing
                ),
                # LEVEL 9: Chaos storm - rapid-fire network disruptions
                network_helper.create_network_chaos_ingress(
                    label_selector=node_label_selector,
                    instance_count=min(
                        3, max(2, instance_count * 2)
                    ),  # Max 3 worker nodes
                    test_duration=300,  # Reduced duration for faster testing
                ),
            ]
            scenarios.extend(extreme_scenarios)

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("pod_network_scenarios", scenario)
        config.set_tunings(wait_duration=60, iterations=1)
        config.write_to_file(location=scenario_dir)

        # Execute Krkn chaos scenarios
        krkn = KrKnRunner(config.global_config)
        try:
            log.info(f"Starting network ingress chaos test for {node_type} nodes")
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(f"Network ingress chaos test completed for {node_type} nodes")
        except CommandFailed as e:
            log.error(f"Krkn command failed for {node_type} nodes: {str(e)}")
            pytest.fail(f"Krkn command failed for {node_type} nodes: {str(e)}")

        # Validate workloads and cleanup
        try:
            workload_ops.validate_and_cleanup()
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(
                f"Workload validation/cleanup issue for {node_type} nodes: {str(e)}"
            )

        # Analyze chaos run results
        chaos_run_output = krkn.get_chaos_data()
        failing_scenarios = [
            scenario
            for scenario in chaos_run_output["telemetry"]["scenarios"]
            if scenario.get("affected_pods", {}).get("error") is not None
        ]

        if failing_scenarios:
            log.error(f"Failed scenarios for {node_type} nodes: {failing_scenarios}")

        assert (
            not failing_scenarios
        ), f"Network ingress chaos scenarios failed for {node_type} nodes with errors: {failing_scenarios}"

        # Check for Ceph crashes after network ingress chaos injection
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            f"{node_type} nodes", "network ingress chaos"
        )
        assert no_crashes, crash_details

        log.info(
            f"Network ingress chaos test for {node_type} nodes completed successfully"
        )

    @pytest.mark.parametrize(
        "ceph_component_label,instance_count,execution_type,traffic_direction,network_params",
        [
            # Combined egress and ingress shaping tests
            (
                OSD_APP_LABEL,
                2,
                "parallel",
                "egress",
                {"latency": "60ms", "loss": "'0.50%'", "bandwidth": "80mbit"},
            ),  # OSDs with egress traffic shaping (moderate)
            (
                OSD_APP_LABEL,
                2,
                "parallel",
                "ingress",
                {"latency": "55ms", "loss": "'0.22%'", "bandwidth": "85mbit"},
            ),  # OSDs with ingress traffic shaping
            (
                MGR_APP_LABEL,
                1,
                "serial",
                "egress",
                {"latency": "40ms", "loss": "'0.20%'", "bandwidth": "100mbit"},
            ),  # MGR with basic egress shaping
            (
                MGR_APP_LABEL,
                1,
                "serial",
                "ingress",
                {"latency": "35ms", "loss": "'0.12%'", "bandwidth": "110mbit"},
            ),  # MGR with conservative ingress shaping
            (
                MON_APP_LABEL,
                1,
                "serial",
                "egress",
                {"latency": "30ms", "loss": "'0.20%'", "bandwidth": "120mbit"},
            ),  # MON with basic egress disruption
            (
                MON_APP_LABEL,
                1,
                "serial",
                "ingress",
                {"latency": "25ms", "loss": "'0.08%'", "bandwidth": "130mbit"},
            ),  # MON with minimal ingress disruption (critical component)
            (
                RGW_APP_LABEL,
                2,
                "parallel",
                "egress",
                {"latency": "70ms", "loss": "'0.90%'", "bandwidth": "60mbit"},
            ),  # RGW with high egress bandwidth constraints
            (
                RGW_APP_LABEL,
                2,
                "parallel",
                "ingress",
                {"latency": "65ms", "loss": "'0.28%'", "bandwidth": "70mbit"},
            ),  # RGW with ingress bandwidth constraints
        ],
        ids=[
            "osd-egress-shaping",
            "osd-ingress-shaping",
            "mgr-egress-shaping",
            "mgr-ingress-shaping",
            "mon-egress-shaping",
            "mon-ingress-shaping",
            "rgw-egress-shaping",
            "rgw-ingress-shaping",
        ],
    )
    def test_krkn_pod_traffic_shaping(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        instance_count,
        execution_type,
        traffic_direction,
        network_params,
    ):
        """
        Test pod traffic shaping scenarios (both egress and ingress) using Krkn templates.

        This test validates network resilience by applying traffic shaping
        (latency, packet loss, bandwidth limitation) to different Ceph component pods
        in both egress and ingress directions and verifying that the system can handle
        these network impairments gracefully.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario files
            workload_ops: Workload operations fixture
            ceph_component_label: Parameterized Ceph component label
            instance_count: Number of pods to target for chaos injection
            execution_type: Execution mode ('serial' or 'parallel')
            traffic_direction: Traffic direction ('egress' or 'ingress')
            network_params: Network impairment parameters
        """
        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        log.info(
            f"Testing pod {traffic_direction} shaping for Ceph component: {ceph_component_label} "
            f"with instance_count={instance_count}, execution_type={execution_type}, "
            f"network_params={network_params}"
        )

        # WORKLOAD SETUP - Start workloads and background cluster operations
        log.info("Setting up workloads for pod traffic shaping testing")
        workload_ops.setup_workloads()

        # Create pod traffic shaping scenarios based on direction
        if traffic_direction == "egress":
            # Initialize NetworkScenarioHelper for intelligent scenario generation
            network_helper = NetworkScenarioHelper(
                scenario_dir=scenario_dir, namespace=openshift_storage_ns
            )

            # Determine component criticality for intelligent scenario generation
            component_name = network_helper.extract_component_name(ceph_component_label)
            is_critical = network_helper.is_critical_component(component_name)

            scenarios = [
                # LEVEL 1: Primary egress shaping scenario
                network_helper.create_pod_egress_shaping(
                    label_selector=ceph_component_label,
                    instance_count=instance_count,
                    test_duration=300,
                ),
                # LEVEL 2: High-intensity egress capacity stress
                network_helper.create_pod_egress_shaping(
                    label_selector=ceph_component_label,
                    instance_count=(
                        min(instance_count * 2, 6)
                        if not is_critical
                        else instance_count
                    ),
                    test_duration=300,
                ),
                # LEVEL 3: Extended duration capacity test
                network_helper.create_pod_egress_shaping(
                    label_selector=ceph_component_label,
                    instance_count=(
                        max(2, instance_count) if not is_critical else instance_count
                    ),
                    test_duration=300,  # Reduced duration for faster testing
                ),
            ]

            # Add extreme scenarios only for resilient components
            if not is_critical:
                extreme_scenarios = [
                    # LEVEL 4: Extreme egress chaos
                    network_helper.create_pod_egress_shaping(
                        label_selector=ceph_component_label,
                        instance_count=min(instance_count * 3, 6),
                        test_duration=300,  # Reduced duration for faster testing
                    ),
                    # LEVEL 5: Maximum egress stress
                    network_helper.create_pod_egress_shaping(
                        label_selector=ceph_component_label,
                        instance_count=min(instance_count * 2, 3),  # Max 3 worker nodes
                        test_duration=300,  # Reduced duration for faster testing
                    ),
                ]
                scenarios.extend(extreme_scenarios)
        else:  # ingress
            # Initialize NetworkScenarioHelper for intelligent scenario generation
            network_helper = NetworkScenarioHelper(
                scenario_dir=scenario_dir, namespace=openshift_storage_ns
            )

            # Determine component criticality for intelligent scenario generation
            component_name = network_helper.extract_component_name(ceph_component_label)
            is_critical = network_helper.is_critical_component(component_name)

            scenarios = [
                # LEVEL 1: Primary ingress shaping scenario
                network_helper.create_pod_ingress_shaping(
                    label_selector=ceph_component_label,
                    instance_count=instance_count,
                    test_duration=300,
                ),
                # LEVEL 2: High-intensity ingress capacity stress
                network_helper.create_pod_ingress_shaping(
                    label_selector=ceph_component_label,
                    instance_count=(
                        min(instance_count * 2, 6)
                        if not is_critical
                        else instance_count
                    ),
                    test_duration=300,
                ),
                # LEVEL 3: Sustained capacity stress test
                network_helper.create_pod_ingress_shaping(
                    label_selector=ceph_component_label,
                    instance_count=(
                        max(2, instance_count) if not is_critical else instance_count
                    ),
                    test_duration=300,  # Reduced duration for faster testing
                ),
            ]

            # Add extreme scenarios only for resilient components
            if not is_critical:
                extreme_scenarios = [
                    # LEVEL 4: Extreme ingress chaos
                    network_helper.create_pod_ingress_shaping(
                        label_selector=ceph_component_label,
                        instance_count=min(instance_count * 3, 6),
                        test_duration=300,  # Reduced duration for faster testing
                    ),
                    # LEVEL 5: Maximum ingress stress
                    network_helper.create_pod_ingress_shaping(
                        label_selector=ceph_component_label,
                        instance_count=min(instance_count * 2, 3),  # Max 3 worker nodes
                        test_duration=300,  # Reduced duration for faster testing
                    ),
                ]
                scenarios.extend(extreme_scenarios)

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("pod_network_scenarios", scenario)
        config.set_tunings(wait_duration=60, iterations=1)
        config.write_to_file(location=scenario_dir)

        # Execute Krkn chaos scenarios
        krkn = KrKnRunner(config.global_config)
        try:
            log.info(
                f"Starting pod {traffic_direction} shaping test for {ceph_component_label}"
            )
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(
                f"Pod {traffic_direction} shaping test completed for {ceph_component_label}"
            )
        except CommandFailed as e:
            validator = ValidationHelper()
            validator.handle_krkn_command_failure(
                e, ceph_component_label, "network chaos"
            )

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

        assert not failing_scenarios, (
            f"Pod {traffic_direction} shaping scenarios failed for {ceph_component_label} "
            f"with pod errors: {failing_scenarios}"
        )

        # Check for Ceph crashes after pod traffic shaping chaos injection
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            ceph_component_label, f"pod {traffic_direction} shaping chaos"
        )
        assert no_crashes, crash_details

        log.info(
            f"Pod {traffic_direction} shaping test for {ceph_component_label} completed successfully"
        )

    @pytest.mark.parametrize(
        "target_component,service_type,test_description",
        [
            (
                MON_APP_LABEL,
                "mon",
                "MON communication ports - cluster coordination",
            ),
            (
                MGR_APP_LABEL,
                "mgr",
                "MGR management ports - cluster management operations",
            ),
            (
                RGW_APP_LABEL,
                "rgw",
                "RGW S3 API ports - object storage access",
            ),
        ],
        ids=[
            "mon-dynamic-ports",
            "mgr-dynamic-ports",
            "rgw-dynamic-ports",
        ],
    )
    def test_krkn_targeted_port_network_outage(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        target_component,
        service_type,
        test_description,
    ):
        """
        Test targeted network outage scenarios focusing on dynamically discovered Ceph service ports.

        This test validates network resilience by blocking specific ports used by
        Ceph components and verifying that the system can handle these targeted
        network disruptions gracefully. Ports are discovered dynamically from the cluster.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario files
            workload_ops: Workload operations fixture
            target_component: Ceph component label selector
            service_type: Type of Ceph service (mon, mgr, rgw, etc.)
            test_description: Description of what is being tested
        """
        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        # WORKLOAD SETUP - Start workloads and background cluster operations
        log.info("Setting up workloads for targeted port network outage testing")
        workload_ops.setup_workloads()

        # Dynamically discover ports for the target service
        log.info(f"Discovering ports for {service_type} service...")
        port_helper = NetworkPortHelper(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        service_ports_map = port_helper.get_ceph_service_ports(service_type)
        target_ports = service_ports_map.get(service_type, [])
        if not target_ports:
            pytest.skip(
                f"No ports discovered for {service_type} service - skipping test"
            )

        log.info(
            f"Testing targeted port network outage for: {test_description} "
            f"targeting dynamically discovered ports {target_ports} on component {target_component}"
        )

        # Initialize NetworkScenarioHelper for intelligent scenario generation
        network_helper = NetworkScenarioHelper(
            scenario_dir=scenario_dir, namespace=openshift_storage_ns
        )

        # Create targeted port network outage scenarios using helper
        scenarios = [
            # LEVEL 1: Ingress port blocking scenario
            network_helper.create_pod_network_outage(
                label_selector=target_component,
                direction=["ingress"],
                ingress_ports=target_ports,
                instance_count=1,  # Safe: target only 1 pod
                test_duration=120,
            ),
            # LEVEL 2: Egress port blocking scenario (for bidirectional testing)
            network_helper.create_pod_network_outage(
                label_selector=target_component,
                direction=["egress"],
                egress_ports=target_ports,
                instance_count=1,
                test_duration=120,
            ),
            # LEVEL 3: High-intensity bidirectional port blocking
            network_helper.create_pod_network_outage(
                label_selector=target_component,
                direction=["ingress", "egress"],  # Block both directions simultaneously
                ingress_ports=target_ports,
                egress_ports=target_ports,
                instance_count=2,  # Target multiple pods for capacity stress
                test_duration=300,  # Extended test duration
            ),
            # LEVEL 4: Sustained capacity stress with extended port blocking
            network_helper.create_pod_network_outage(
                label_selector=target_component,
                direction=["egress"],
                egress_ports=target_ports,
                instance_count=2,  # Target multiple pods
                test_duration=300,  # Reduced duration for faster testing
            ),
            # LEVEL 5: Burst capacity test - rapid sequential port disruptions
            network_helper.create_pod_network_outage(
                label_selector=target_component,
                direction=["ingress"],
                ingress_ports=target_ports,
                instance_count=1,
                test_duration=60,  # Short bursts to test burst handling capacity
            ),
            # LEVEL 6: Maximum capacity stress test - all available pods
            network_helper.create_pod_network_outage(
                label_selector=target_component,
                direction=["ingress", "egress"],
                ingress_ports=target_ports,
                egress_ports=target_ports,
                instance_count=3,  # Target maximum safe number of pods
                test_duration=300,  # Reduced duration for faster testing
            ),
        ]

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("pod_network_scenarios", scenario)

        config_file = config.write_to_file(scenario_dir)

        # Run chaos scenarios
        log.info(f"Starting targeted port chaos scenarios for {target_component}...")
        krkn = KrKnRunner(config_file)
        krkn.run_async()
        krkn.wait_for_completion(check_interval=30)

        # Validate workload operations after chaos
        log.info("Validating workload operations after targeted port chaos...")
        try:
            workload_ops.validate_workload_operations()
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(
                f"Workload validation issue for {target_component} port test: {str(e)}"
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
                f"Failed scenarios for {target_component} port test: {failing_scenarios}"
            )

        assert (
            not failing_scenarios
        ), f"Targeted port network outage scenarios failed for {target_component} with errors: {failing_scenarios}"

        log.info(
            f"Targeted port network outage test for {target_component} completed successfully"
        )

    @pytest.mark.parametrize(
        "port_discovery_method,test_scenario",
        [
            (
                "service_ports",
                "OSD service-exposed ports",
            ),
            (
                "container_ports",
                "OSD container-level ports",
            ),
        ],
        ids=[
            "osd-service-ports",
            "osd-container-ports",
        ],
    )
    @polarion_id("OCS-7344")
    def test_krkn_osd_replication_port_chaos(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        port_discovery_method,
        test_scenario,
    ):
        """
        Test OSD replication port chaos scenarios using dynamically discovered ports.

        This test specifically targets OSD communication ports discovered from
        the actual cluster to validate data replication resilience under network disruptions.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario files
            workload_ops: Workload operations fixture
            port_discovery_method: Method to discover OSD ports (service_ports or container_ports)
            test_scenario: Description of the test scenario
        """
        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        # WORKLOAD SETUP - Start workloads and background cluster operations
        log.info("Setting up workloads for OSD replication port chaos testing")
        workload_ops.setup_workloads()

        # Dynamically discover OSD ports based on method
        port_helper = NetworkPortHelper(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        if port_discovery_method == "service_ports":
            service_ports_map = port_helper.get_ceph_service_ports("osd")
            osd_port_range = service_ports_map.get("osd", [])
        else:  # container_ports
            osd_port_range = port_helper.get_pod_container_ports(OSD_APP_LABEL)
        if not osd_port_range:
            pytest.skip(
                f"No OSD ports discovered using {port_discovery_method} - skipping test"
            )

        log.info(
            f"Testing OSD replication port chaos: {test_scenario} "
            f"targeting dynamically discovered ports {osd_port_range}"
        )

        # Initialize NetworkScenarioHelper for OSD port-specific scenarios
        network_helper = NetworkScenarioHelper(
            scenario_dir=scenario_dir, namespace=openshift_storage_ns
        )

        # Create OSD port-specific scenarios using helper
        scenarios = [
            # LEVEL 1: Target OSD replication ports - egress disruption
            network_helper.create_pod_network_outage(
                label_selector=OSD_APP_LABEL,
                direction=["egress"],
                egress_ports=osd_port_range,
                instance_count=1,  # Safe: target only 1 OSD
                test_duration=300,
            ),
            # LEVEL 2: Target OSD replication ports - ingress disruption
            network_helper.create_pod_network_outage(
                label_selector=OSD_APP_LABEL,
                direction=["ingress"],
                ingress_ports=osd_port_range,
                instance_count=1,
                test_duration=300,
            ),
        ]

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("pod_network_scenarios", scenario)

        config_file = config.write_to_file(scenario_dir)

        # Run chaos scenarios
        log.info("Starting OSD replication port chaos scenarios...")
        krkn = KrKnRunner(config_file)
        krkn.run_async()
        krkn.wait_for_completion(check_interval=30)

        # Validate workload operations after chaos
        log.info("Validating workload operations after OSD port chaos...")
        try:
            workload_ops.validate_workload_operations()
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(f"Workload validation issue for OSD port test: {str(e)}")

        # Check Ceph cluster health after OSD port chaos
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "OSD ports", "OSD port chaos"
        )
        assert no_crashes, crash_details

        # Analyze chaos run results
        chaos_run_output = krkn.get_chaos_data()
        failing_scenarios = [
            scenario
            for scenario in chaos_run_output["telemetry"]["scenarios"]
            if scenario.get("affected_pods", {}).get("error") is not None
        ]

        if failing_scenarios:
            log.error(f"Failed OSD port chaos scenarios: {failing_scenarios}")

        assert (
            not failing_scenarios
        ), f"OSD replication port chaos scenarios failed with errors: {failing_scenarios}"

        log.info("OSD replication port chaos test completed successfully")

    @polarion_id("OCS-7345")
    def test_krkn_interface_specific_network_chaos(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
    ):
        """
        Test interface-specific network chaos using detected physical interfaces.

        This test uses the actual physical network interface (e.g., ens192 on vSphere)
        instead of assuming eth0, and applies network impairments at the interface level.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario files
            workload_ops: Workload operations fixture
        """
        scenario_dir = krkn_scenario_directory

        log.info("Testing interface-specific network chaos with detected interfaces")

        # WORKLOAD SETUP - Start workloads and background cluster operations
        log.info("Setting up workloads for interface-specific network chaos testing")
        workload_ops.setup_workloads()

        # Get actual network interfaces from the cluster
        port_helper = NetworkPortHelper()
        worker_interfaces = port_helper.get_default_network_interfaces(
            node_type="worker"
        )
        log.info(f"Detected worker node interfaces: {worker_interfaces}")

        # Initialize NetworkScenarioHelper for interface-level scenarios
        network_helper = NetworkScenarioHelper(
            scenario_dir=scenario_dir, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )

        # Create interface-level network chaos scenarios using helper
        scenarios = [
            # LEVEL 1: Mild latency scenario
            network_helper.create_pod_network_chaos(
                label_selector="node-role.kubernetes.io/worker",
                instance_count=1,  # Target only 1 worker node
                interfaces=worker_interfaces,
                duration=240,
            ),
            # LEVEL 2: Moderate network impairment scenario
            network_helper.create_pod_network_chaos(
                label_selector="node-role.kubernetes.io/worker",
                instance_count=1,
                interfaces=worker_interfaces,
                duration=180,
            ),
            # LEVEL 3: High-intensity interface chaos (worker nodes only)
            network_helper.create_pod_network_chaos(
                label_selector="node-role.kubernetes.io/worker",
                instance_count=min(
                    3, 3
                ),  # Target multiple worker nodes (max 3 available)
                interfaces=worker_interfaces,
                duration=600,  # Extended duration
            ),
            # LEVEL 4: Extended interface stress (worker nodes only)
            network_helper.create_pod_network_chaos(
                label_selector="node-role.kubernetes.io/worker",
                instance_count=min(3, 3),  # Max 3 worker nodes available
                interfaces=worker_interfaces,
                duration=900,  # Longer stress duration
            ),
            # LEVEL 5: Maximum interface chaos (worker nodes only)
            network_helper.create_pod_network_chaos(
                label_selector="node-role.kubernetes.io/worker",
                instance_count=min(6, 8),  # Maximum safe worker nodes
                interfaces=worker_interfaces,
                duration=300,  # Reduced duration for faster testing
            ),
        ]

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("pod_network_scenarios", scenario)

        config_file = config.write_to_file(scenario_dir)

        # Run chaos scenarios
        log.info("Starting interface-specific network chaos scenarios...")
        krkn = KrKnRunner(config_file)
        krkn.run_async()
        krkn.wait_for_completion(check_interval=30)

        # Validate workload operations after chaos
        log.info("Validating workload operations after interface chaos...")
        try:
            workload_ops.validate_workload_operations()
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(f"Workload validation issue for interface chaos: {str(e)}")

        # Analyze chaos run results
        chaos_run_output = krkn.get_chaos_data()
        failing_scenarios = [
            scenario
            for scenario in chaos_run_output["telemetry"]["scenarios"]
            if scenario.get("affected_pods", {}).get("error") is not None
        ]

        if failing_scenarios:
            log.error(f"Failed interface chaos scenarios: {failing_scenarios}")

        assert (
            not failing_scenarios
        ), f"Interface-specific network chaos scenarios failed with errors: {failing_scenarios}"

        # Check Ceph cluster health after interface chaos
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "network interfaces", "interface chaos"
        )
        assert no_crashes, crash_details

        log.info("Interface-specific network chaos test completed successfully")
