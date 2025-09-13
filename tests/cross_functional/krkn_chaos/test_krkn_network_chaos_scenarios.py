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
    check_ceph_crashes,
    handle_krkn_command_failure,
)
from ocs_ci.krkn_chaos.krkn_helpers import (
    get_default_network_interfaces,
    get_ceph_service_ports,
    get_pod_container_ports,
)

log = logging.getLogger(__name__)


@green_squad
@chaos
@polarion_id("OCS-1235")
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

        # Dynamically discover all Ceph service ports for chaos testing
        try:
            all_service_ports = get_ceph_service_ports("all")
            # Combine all discovered ports into a single list
            dynamic_ceph_ports = []
            for service_type, ports in all_service_ports.items():
                if isinstance(ports, list):
                    dynamic_ceph_ports.extend(ports)
            dynamic_ceph_ports = sorted(
                list(set(dynamic_ceph_ports))
            )  # Remove duplicates
            log.info(
                f"Dynamically discovered Ceph ports for chaos testing: {dynamic_ceph_ports}"
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

        # Create network outage scenarios targeting the specific Ceph component
        scenarios = [
            # Test egress and ingress blocking
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["egress", "ingress"],
                label_selector=ceph_component_label,
                instance_count=instance_count,
                wait_duration=600,
                test_duration=300,
            ),
            # Test egress-only blocking with dynamically discovered Ceph ports
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["egress"],
                egress_ports=dynamic_ceph_ports,
                label_selector=ceph_component_label,
                instance_count=instance_count,
                wait_duration=360,
                test_duration=180,
            ),
            # Test ingress-only blocking
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["ingress"],
                label_selector=ceph_component_label,
                instance_count=instance_count,
                wait_duration=240,
                test_duration=120,
            ),
            # 🔥 MAXIMUM OUTAGE CHAOS: Complete network isolation
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["egress", "ingress"],  # Block EVERYTHING
                label_selector=ceph_component_label,
                instance_count=min(instance_count * 4, 8),  # Target many pods
                wait_duration=1200,  # Extended wait for maximum chaos buildup
                test_duration=900,  # 15 minutes of complete isolation
            ),
            # 💥 TOTAL COMMUNICATION BLACKOUT: Apocalyptic isolation
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["egress", "ingress"],
                egress_ports=list(range(1, 65536)),  # Block ALL ports!
                ingress_ports=list(range(1, 65536)),  # Block ALL ports!
                label_selector=ceph_component_label,
                instance_count=min(instance_count * 6, 10),  # Maximum pods
                wait_duration=1800,  # 30 minutes buildup
                test_duration=1200,  # 20 minutes of total blackout
            ),
            # ⚡ CHAOS STORM: Rapid outage cycles
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["egress"],
                label_selector=ceph_component_label,
                instance_count=max(instance_count * 2, 4),
                wait_duration=180,  # Short wait for rapid cycles
                test_duration=1800,  # 30 minutes of rapid chaos
            ),
            # 🌪️ NETWORK APOCALYPSE: Ultimate outage scenario
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["egress", "ingress"],
                label_selector=ceph_component_label,
                instance_count=min(instance_count * 8, 12),  # Absolute maximum
                wait_duration=2400,  # 40 minutes buildup for apocalypse
                test_duration=1800,  # 30 minutes of apocalyptic outage
            ),
            # 💀 CLUSTER ANNIHILATOR: Beyond maximum outage
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["egress", "ingress"],
                egress_ports=dynamic_ceph_ports
                + [80, 443, 8080, 8443, 9090, 9443],  # Ceph + common ports
                ingress_ports=dynamic_ceph_ports
                + [80, 443, 8080, 8443, 9090, 9443],  # Ceph + common ports
                label_selector=ceph_component_label,
                instance_count=min(instance_count * 10, 15),  # Push absolute limits
                wait_duration=3600,  # 1 HOUR buildup for maximum chaos
                test_duration=2400,  # 40 minutes of cluster annihilation
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
            handle_krkn_command_failure(e, ceph_component_label, "network chaos")

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
        assert check_ceph_crashes(ceph_component_label, "network outage chaos")

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
                {"latency": "50ms", "loss": 0.02},
            ),  # OSDs with network latency and packet loss
            (
                MGR_APP_LABEL,
                1,
                240,
                "serial",
                {"latency": "100ms", "loss": 0.05},
            ),  # MGR with higher latency and loss
            (
                MON_APP_LABEL,
                1,
                180,
                "serial",
                {"latency": "25ms", "loss": 0.01},
            ),  # MON with minimal disruption (critical component)
            (
                MDS_APP_LABEL,
                1,
                240,
                "serial",
                {"latency": "75ms", "loss": 0.03},
            ),  # MDS with moderate network impairment
            (
                RGW_APP_LABEL,
                2,
                300,
                "parallel",
                {"latency": "60ms", "loss": 0.04},
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

        # Get active network interfaces from worker nodes
        worker_interfaces = get_default_network_interfaces(node_type="worker")

        # Get active network interfaces from all nodes (worker + master)
        all_interfaces = get_default_network_interfaces(node_type="all")

        # Create network chaos scenarios targeting the specific Ceph component
        scenarios = [
            # Network chaos with latency and packet loss on worker node interfaces
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=duration,
                label_selector=ceph_component_label,
                instance_count=instance_count,
                interfaces=worker_interfaces,
                execution=execution,
                egress=egress_config,
            ),
            # Network chaos with multiple interface targeting (all cluster interfaces)
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=duration // 2,  # Shorter duration for second scenario
                label_selector=ceph_component_label,
                instance_count=max(
                    1, instance_count // 2
                ),  # Fewer instances for second scenario
                interfaces=all_interfaces,
                execution=(
                    "serial" if execution == "parallel" else "parallel"
                ),  # Alternate execution
                egress={
                    "latency": "30ms",
                    "loss": 0.015,
                },  # Milder impairment for second scenario
            ),
            # 🔥 CATASTROPHIC NETWORK CHAOS: Maximum interface disruption
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=duration * 3,  # 3x longer duration for maximum chaos
                label_selector=ceph_component_label,
                instance_count=min(instance_count * 4, 8),  # Target maximum pods
                interfaces=all_interfaces,  # Hit ALL interfaces
                execution="parallel",  # All at once for maximum chaos
                egress={
                    "latency": "800ms",  # Nearly 1 second latency!
                    "loss": 0.18,  # 18% packet loss - catastrophic!
                },
            ),
            # 💥 TOTAL INTERFACE MELTDOWN: Apocalyptic network conditions
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=duration * 4,  # 4x longer - sustained apocalypse
                label_selector=ceph_component_label,
                instance_count=min(instance_count * 6, 10),  # Maximum possible pods
                interfaces=worker_interfaces + all_interfaces,  # ALL interfaces
                execution="parallel",
                egress={
                    "latency": "1200ms",  # 1.2 SECOND delays!
                    "loss": 0.25,  # 25% packet loss - chaos mode!
                },
            ),
            # ⚡ CHAOS STORM: Rapid-fire interface disruptions
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=duration * 5,  # 5x longer for sustained storm
                label_selector=ceph_component_label,
                instance_count=max(instance_count * 2, 6),
                interfaces=all_interfaces,
                execution="parallel",
                egress={
                    "latency": "600ms",  # Half-second delays
                    "loss": 0.15,  # 15% packet loss
                },
            ),
            # 🌪️ NETWORK APOCALYPSE: Ultimate chaos scenario
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=duration * 6,  # 6x longer - ultimate endurance test
                label_selector=ceph_component_label,
                instance_count=min(instance_count * 8, 12),  # Absolute maximum
                interfaces=worker_interfaces + all_interfaces,  # Every interface
                execution="parallel",
                egress={
                    "latency": "1500ms",  # 1.5 SECOND delays - network nightmare!
                    "loss": 0.30,  # 30% packet loss - apocalyptic!
                },
            ),
            # 💀 CLUSTER DESTROYER: Beyond maximum chaos
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=duration * 8,  # 8x longer - extreme endurance
                label_selector=ceph_component_label,
                instance_count=min(instance_count * 10, 15),  # Push absolute limits
                interfaces=all_interfaces,
                execution="parallel",
                egress={
                    "latency": "2000ms",  # 2 FULL SECONDS per request!
                    "loss": 0.35,  # 35% packet loss - cluster destroyer!
                },
            ),
        ]

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
            handle_krkn_command_failure(e, ceph_component_label, "network chaos")

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
        assert check_ceph_crashes(ceph_component_label, "network chaos")

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
                {"latency": "75ms", "loss": "0.03", "bandwidth": "50mbit"},
            ),  # Worker nodes with network ingress chaos
            (
                constants.MASTER_LABEL,
                1,
                "serial",
                {"latency": "25ms", "loss": "0.01", "bandwidth": "100mbit"},
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

        # Warning for master node testing
        if node_type == "master":
            log.warning(
                "Running network chaos on master nodes - this could affect cluster stability!"
            )

        # Create network chaos ingress scenarios targeting nodes
        scenarios = [
            # Primary ingress chaos scenario
            NetworkOutageScenarios.network_chaos_ingress(
                scenario_dir,
                label_selector=node_label_selector,
                instance_count=instance_count,
                execution_type=execution_type,
                network_params=network_params,
                wait_duration=600,
                test_duration=300,
            ),
            # Secondary scenario with different parameters
            NetworkOutageScenarios.network_chaos_ingress(
                scenario_dir,
                label_selector=node_label_selector,
                instance_count=max(1, instance_count // 2),
                execution_type="serial" if execution_type == "parallel" else "parallel",
                network_params={
                    "latency": "40ms",
                    "loss": "0.015",
                    "bandwidth": "80mbit",
                },
                wait_duration=480,
                test_duration=180,
            ),
            # High-intensity cluster capacity stress scenario
            NetworkOutageScenarios.network_chaos_ingress(
                scenario_dir,
                label_selector=node_label_selector,
                instance_count=min(3, instance_count * 2),  # Target more nodes
                execution_type="parallel",
                network_params={
                    "latency": "100ms",  # Higher latency
                    "loss": "0.05",  # Higher packet loss (5%)
                    "bandwidth": "20mbit",  # Severe bandwidth limitation
                },
                wait_duration=900,  # Longer wait
                test_duration=600,  # Extended test duration
            ),
            # Extreme network degradation scenario
            NetworkOutageScenarios.network_chaos_ingress(
                scenario_dir,
                label_selector=node_label_selector,
                instance_count=instance_count,
                execution_type="parallel",
                network_params={
                    "latency": "200ms",  # Very high latency
                    "loss": "0.08",  # 8% packet loss
                    "bandwidth": "10mbit",  # Very limited bandwidth
                },
                wait_duration=720,
                test_duration=480,
            ),
            # Burst capacity test scenario
            NetworkOutageScenarios.network_chaos_ingress(
                scenario_dir,
                label_selector=node_label_selector,
                instance_count=max(2, instance_count),
                execution_type="serial",
                network_params={
                    "latency": "150ms",
                    "loss": "0.06",  # 6% packet loss
                    "bandwidth": "15mbit",
                },
                wait_duration=600,
                test_duration=900,  # Very long duration to test sustained load
            ),
            # Multi-phase capacity stress scenario
            NetworkOutageScenarios.network_chaos_ingress(
                scenario_dir,
                label_selector=node_label_selector,
                instance_count=min(4, instance_count + 1),  # Target additional nodes
                execution_type="parallel",
                network_params={
                    "latency": "80ms",
                    "loss": "0.04",  # 4% packet loss
                    "bandwidth": "30mbit",
                },
                wait_duration=1200,  # Extended wait for capacity buildup
                test_duration=720,  # Long test duration
            ),
            # 🔥 MAXIMUM CHAOS: Catastrophic network degradation
            NetworkOutageScenarios.network_chaos_ingress(
                scenario_dir,
                label_selector=node_label_selector,
                instance_count=min(6, instance_count * 3),  # Target MANY more nodes
                execution_type="parallel",
                network_params={
                    "latency": "500ms",  # EXTREME latency - half a second!
                    "loss": "0.15",  # 15% packet loss - catastrophic!
                    "bandwidth": "5mbit",  # Barely functional bandwidth
                },
                wait_duration=1200,  # Extended wait for maximum chaos buildup
                test_duration=900,  # 15 minutes of pure chaos
            ),
            # 💥 TOTAL NETWORK MELTDOWN scenario
            NetworkOutageScenarios.network_chaos_ingress(
                scenario_dir,
                label_selector=node_label_selector,
                instance_count=min(
                    8, instance_count * 4
                ),  # Hit as many nodes as possible
                execution_type="parallel",
                network_params={
                    "latency": "1000ms",  # 1 SECOND latency - network nightmare!
                    "loss": "0.20",  # 20% packet loss - chaos mode!
                    "bandwidth": "2mbit",  # Dial-up era bandwidth
                },
                wait_duration=900,
                test_duration=1200,  # 20 minutes of total chaos
            ),
            # ⚡ CHAOS STORM: Rapid-fire network disruptions
            NetworkOutageScenarios.network_chaos_ingress(
                scenario_dir,
                label_selector=node_label_selector,
                instance_count=max(4, instance_count * 2),
                execution_type="parallel",  # All at once for maximum chaos
                network_params={
                    "latency": "300ms",
                    "loss": "0.12",  # 12% packet loss
                    "bandwidth": "8mbit",
                },
                wait_duration=300,  # Short wait - rapid chaos injection
                test_duration=1800,  # 30 minutes of sustained chaos storm
            ),
            # 🌪️ APOCALYPTIC STRESS: Push cluster to breaking point
            NetworkOutageScenarios.network_chaos_ingress(
                scenario_dir,
                label_selector=node_label_selector,
                instance_count=min(10, instance_count * 5),  # Maximum possible nodes
                execution_type="parallel",
                network_params={
                    "latency": "750ms",  # Three-quarter second delays
                    "loss": "0.25",  # 25% packet loss - quarter of packets gone!
                    "bandwidth": "1mbit",  # Practically unusable bandwidth
                },
                wait_duration=1800,  # 30 minutes buildup for maximum chaos
                test_duration=1500,  # 25 minutes of apocalyptic stress
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
        assert check_ceph_crashes(f"{node_type} nodes", "network ingress chaos")

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
                {"latency": "60ms", "loss": "'0.025%'", "bandwidth": "80mbit"},
            ),  # OSDs with egress traffic shaping
            (
                OSD_APP_LABEL,
                2,
                "parallel",
                "ingress",
                {"latency": "55ms", "loss": "'0.022%'", "bandwidth": "85mbit"},
            ),  # OSDs with ingress traffic shaping
            (
                MGR_APP_LABEL,
                1,
                "serial",
                "egress",
                {"latency": "40ms", "loss": "'0.015%'", "bandwidth": "100mbit"},
            ),  # MGR with moderate egress shaping
            (
                MGR_APP_LABEL,
                1,
                "serial",
                "ingress",
                {"latency": "35ms", "loss": "'0.012%'", "bandwidth": "110mbit"},
            ),  # MGR with conservative ingress shaping
            (
                MON_APP_LABEL,
                1,
                "serial",
                "egress",
                {"latency": "30ms", "loss": "'0.01%'", "bandwidth": "120mbit"},
            ),  # MON with minimal egress disruption
            (
                MON_APP_LABEL,
                1,
                "serial",
                "ingress",
                {"latency": "25ms", "loss": "'0.008%'", "bandwidth": "130mbit"},
            ),  # MON with minimal ingress disruption (critical component)
            (
                RGW_APP_LABEL,
                2,
                "parallel",
                "egress",
                {"latency": "70ms", "loss": "'0.03%'", "bandwidth": "60mbit"},
            ),  # RGW with egress bandwidth constraints
            (
                RGW_APP_LABEL,
                2,
                "parallel",
                "ingress",
                {"latency": "65ms", "loss": "'0.028%'", "bandwidth": "70mbit"},
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

        # Create pod traffic shaping scenarios based on direction
        if traffic_direction == "egress":
            scenarios = [
                # Primary egress shaping scenario
                NetworkOutageScenarios.pod_egress_shaping(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=instance_count,
                    execution_type=execution_type,
                    network_params=network_params,
                    wait_duration=600,
                    test_duration=300,
                ),
                # High-intensity egress capacity stress
                NetworkOutageScenarios.pod_egress_shaping(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=min(instance_count * 2, 6),  # Target more pods
                    execution_type="parallel",
                    network_params={
                        "latency": "120ms",
                        "loss": "'0.07%'",  # 7% packet loss
                        "bandwidth": "25mbit",
                    },
                    wait_duration=900,
                    test_duration=600,
                ),
                # Extended duration capacity test
                NetworkOutageScenarios.pod_egress_shaping(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=max(2, instance_count),
                    execution_type=execution_type,
                    network_params={
                        "latency": "90ms",
                        "loss": "'0.05%'",
                        "bandwidth": "40mbit",
                    },
                    wait_duration=720,
                    test_duration=900,  # Extended test duration
                ),
                # 🔥 EXTREME EGRESS CHAOS: Pod-level network apocalypse
                NetworkOutageScenarios.pod_egress_shaping(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=min(instance_count * 4, 8),  # Target maximum pods
                    execution_type="parallel",
                    network_params={
                        "latency": "800ms",  # Nearly 1 second latency!
                        "loss": "'0.18%'",  # 18% packet loss - extreme!
                        "bandwidth": "3mbit",  # Practically unusable
                    },
                    wait_duration=1500,  # Extended buildup
                    test_duration=1800,  # 30 minutes of chaos
                ),
                # 💥 TOTAL EGRESS MELTDOWN: Maximum pod chaos
                NetworkOutageScenarios.pod_egress_shaping(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=min(instance_count * 6, 10),  # Hit ALL possible pods
                    execution_type="parallel",
                    network_params={
                        "latency": "1200ms",  # 1.2 second delays!
                        "loss": "'0.25%'",  # 25% packet loss - chaos mode!
                        "bandwidth": "1mbit",  # Dial-up speed
                    },
                    wait_duration=2400,  # 40 minutes buildup for maximum chaos
                    test_duration=2100,  # 35 minutes of total chaos
                ),
            ]
        else:  # ingress
            scenarios = [
                # Primary ingress shaping scenario
                NetworkOutageScenarios.pod_ingress_shaping(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=instance_count,
                    execution_type=execution_type,
                    network_params=network_params,
                    wait_duration=600,
                    test_duration=300,
                ),
                # High-intensity ingress capacity stress
                NetworkOutageScenarios.pod_ingress_shaping(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=min(instance_count * 2, 6),  # Target more pods
                    execution_type="parallel",
                    network_params={
                        "latency": "110ms",
                        "loss": "'0.06%'",  # 6% packet loss
                        "bandwidth": "30mbit",
                    },
                    wait_duration=900,
                    test_duration=600,
                ),
                # Sustained capacity stress test
                NetworkOutageScenarios.pod_ingress_shaping(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=max(2, instance_count),
                    execution_type=execution_type,
                    network_params={
                        "latency": "85ms",
                        "loss": "'0.045%'",
                        "bandwidth": "35mbit",
                    },
                    wait_duration=720,
                    test_duration=900,  # Extended test duration
                ),
                # 🔥 EXTREME INGRESS CHAOS: Pod-level network apocalypse
                NetworkOutageScenarios.pod_ingress_shaping(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=min(instance_count * 4, 8),  # Target maximum pods
                    execution_type="parallel",
                    network_params={
                        "latency": "750ms",  # Three-quarter second latency!
                        "loss": "'0.17%'",  # 17% packet loss - extreme!
                        "bandwidth": "4mbit",  # Practically unusable
                    },
                    wait_duration=1500,  # Extended buildup
                    test_duration=1800,  # 30 minutes of chaos
                ),
                # 💥 TOTAL INGRESS MELTDOWN: Maximum pod chaos
                NetworkOutageScenarios.pod_ingress_shaping(
                    scenario_dir,
                    namespace=openshift_storage_ns,
                    label_selector=ceph_component_label,
                    instance_count=min(instance_count * 6, 10),  # Hit ALL possible pods
                    execution_type="parallel",
                    network_params={
                        "latency": "1100ms",  # 1.1 second delays!
                        "loss": "'0.23%'",  # 23% packet loss - chaos mode!
                        "bandwidth": "1.5mbit",  # Barely functional
                    },
                    wait_duration=2400,  # 40 minutes buildup for maximum chaos
                    test_duration=2100,  # 35 minutes of total chaos
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
            log.info(
                f"Starting pod {traffic_direction} shaping test for {ceph_component_label}"
            )
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(
                f"Pod {traffic_direction} shaping test completed for {ceph_component_label}"
            )
        except CommandFailed as e:
            handle_krkn_command_failure(e, ceph_component_label, "network chaos")

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
        assert check_ceph_crashes(
            ceph_component_label, f"pod {traffic_direction} shaping chaos"
        )

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

        # Dynamically discover ports for the target service
        log.info(f"Discovering ports for {service_type} service...")
        service_ports_map = get_ceph_service_ports(service_type)
        target_ports = service_ports_map.get(service_type, [])
        if not target_ports:
            pytest.skip(
                f"No ports discovered for {service_type} service - skipping test"
            )

        log.info(
            f"Testing targeted port network outage for: {test_description} "
            f"targeting dynamically discovered ports {target_ports} on component {target_component}"
        )

        # Create targeted port network outage scenarios
        scenarios = [
            # Ingress port blocking scenario
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=target_component,
                direction=["ingress"],
                ingress_ports=target_ports,
                instance_count=1,  # Safe: target only 1 pod
                wait_duration=300,
                test_duration=120,
            ),
            # Egress port blocking scenario (for bidirectional testing)
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=target_component,
                direction=["egress"],
                egress_ports=target_ports,
                instance_count=1,
                wait_duration=300,
                test_duration=120,
            ),
            # High-intensity bidirectional port blocking capacity stress
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=target_component,
                direction=["ingress", "egress"],  # Block both directions simultaneously
                ingress_ports=target_ports,
                egress_ports=target_ports,
                instance_count=min(2, 3),  # Target multiple pods for capacity stress
                wait_duration=600,  # Extended wait for capacity buildup
                test_duration=300,  # Extended test duration
            ),
            # Sustained capacity stress with extended port blocking
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=target_component,
                direction=["egress"],
                egress_ports=target_ports,
                instance_count=2,  # Target multiple pods
                wait_duration=900,  # Very long preparation time
                test_duration=600,  # Very long duration for sustained capacity testing
            ),
            # Burst capacity test - rapid sequential port disruptions
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=target_component,
                direction=["ingress"],
                ingress_ports=target_ports,
                instance_count=1,
                wait_duration=120,  # Short wait for rapid succession
                test_duration=60,  # Short bursts to test burst handling capacity
            ),
            # Maximum capacity stress test - all available pods
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=target_component,
                direction=["ingress", "egress"],
                ingress_ports=target_ports,
                egress_ports=target_ports,
                instance_count=3,  # Target maximum safe number of pods
                wait_duration=1200,  # Very extended wait for maximum capacity buildup
                test_duration=900,  # Very long duration for maximum stress testing
            ),
        ]

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("pod_network_scenarios", scenario)

        config_file = config.generate_config(scenario_dir)

        # Run chaos scenarios
        log.info(f"Starting targeted port chaos scenarios for {target_component}...")
        krkn = KrKnRunner(config_file)
        krkn.run()

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

        # Dynamically discover OSD ports based on method
        if port_discovery_method == "service_ports":
            service_ports_map = get_ceph_service_ports("osd")
            osd_port_range = service_ports_map.get("osd", [])
        else:  # container_ports
            osd_port_range = get_pod_container_ports(OSD_APP_LABEL)
        if not osd_port_range:
            pytest.skip(
                f"No OSD ports discovered using {port_discovery_method} - skipping test"
            )

        log.info(
            f"Testing OSD replication port chaos: {test_scenario} "
            f"targeting dynamically discovered ports {osd_port_range}"
        )

        # Create OSD port-specific scenarios
        scenarios = [
            # Target OSD replication ports - egress disruption
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=OSD_APP_LABEL,
                direction=["egress"],
                egress_ports=osd_port_range,
                instance_count=1,  # Safe: target only 1 OSD
                wait_duration=600,  # Longer for replication testing
                test_duration=300,
            ),
            # Target OSD replication ports - ingress disruption
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=OSD_APP_LABEL,
                direction=["ingress"],
                ingress_ports=osd_port_range,
                instance_count=1,
                wait_duration=600,
                test_duration=300,
            ),
        ]

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("pod_network_scenarios", scenario)

        config_file = config.generate_config(scenario_dir)

        # Run chaos scenarios
        log.info("Starting OSD replication port chaos scenarios...")
        krkn = KrKnRunner(config_file)
        krkn.run()

        # Validate workload operations after chaos
        log.info("Validating workload operations after OSD port chaos...")
        try:
            workload_ops.validate_workload_operations()
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(f"Workload validation issue for OSD port test: {str(e)}")

        # Check Ceph cluster health after OSD port chaos
        assert check_ceph_crashes("OSD ports", "OSD port chaos")

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

        # Get actual network interfaces from the cluster
        from ocs_ci.krkn_chaos.krkn_helpers import get_default_network_interfaces

        worker_interfaces = get_default_network_interfaces(node_type="worker")
        log.info(f"Detected worker node interfaces: {worker_interfaces}")

        # Create interface-level network chaos scenarios
        scenarios = [
            # Mild latency scenario
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=240,
                label_selector="node-role.kubernetes.io/worker",
                instance_count=1,  # Target only 1 worker node
                interfaces=worker_interfaces,
                execution="serial",
                egress={
                    "latency": "25ms",  # Mild latency
                    "loss": 0.0,  # No packet loss for safety
                },
            ),
            # Moderate network impairment scenario
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=180,
                label_selector="node-role.kubernetes.io/worker",
                instance_count=1,
                interfaces=worker_interfaces,
                execution="serial",
                egress={
                    "latency": "50ms",
                    "loss": 0.01,  # 1% packet loss
                },
            ),
            # 🔥 EXTREME INTERFACE CHAOS: Maximum interface disruption
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=1200,  # 20 minutes of interface chaos
                label_selector="node-role.kubernetes.io/worker",
                instance_count=min(4, 6),  # Target multiple worker nodes
                interfaces=worker_interfaces,
                execution="parallel",  # All at once for maximum chaos
                egress={
                    "latency": "900ms",  # Nearly 1 second latency!
                    "loss": 0.20,  # 20% packet loss - extreme!
                },
            ),
            # 💥 INTERFACE APOCALYPSE: Total interface meltdown
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=1800,  # 30 minutes of apocalyptic conditions
                label_selector="node-role.kubernetes.io/worker",
                instance_count=min(6, 8),  # Maximum worker nodes
                interfaces=worker_interfaces,
                execution="parallel",
                egress={
                    "latency": "1500ms",  # 1.5 SECOND delays!
                    "loss": 0.30,  # 30% packet loss - apocalyptic!
                },
            ),
            # ⚡ INTERFACE CHAOS STORM: Sustained interface destruction
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=2400,  # 40 minutes of sustained chaos storm
                label_selector="node-role.kubernetes.io/worker",
                instance_count=min(8, 10),  # Push interface limits
                interfaces=worker_interfaces,
                execution="parallel",
                egress={
                    "latency": "1200ms",  # 1.2 second delays
                    "loss": 0.25,  # 25% packet loss
                },
            ),
            # 🌪️ INTERFACE ANNIHILATOR: Ultimate interface chaos
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=3000,  # 50 minutes of ultimate chaos
                label_selector="node-role.kubernetes.io/worker",
                instance_count=min(10, 12),  # Absolute maximum nodes
                interfaces=worker_interfaces,
                execution="parallel",
                egress={
                    "latency": "2000ms",  # 2 FULL SECONDS per request!
                    "loss": 0.35,  # 35% packet loss - interface annihilator!
                },
            ),
        ]

        # Generate Krkn configuration
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("pod_network_scenarios", scenario)

        config_file = config.generate_config(scenario_dir)

        # Run chaos scenarios
        log.info("Starting interface-specific network chaos scenarios...")
        krkn = KrKnRunner(config_file)
        krkn.run()

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
        assert check_ceph_crashes("network interfaces", "interface chaos")

        log.info("Interface-specific network chaos test completed successfully")
