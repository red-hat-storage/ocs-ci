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
            # Test egress-only blocking with specific ports
            NetworkOutageScenarios.pod_network_outage(
                scenario_dir,
                namespace=openshift_storage_ns,
                direction=["egress"],
                egress_ports=[6789, 6800, 6801, 6802, 6803, 6804, 6805],  # Ceph ports
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

        # Create network chaos scenarios targeting the specific Ceph component
        scenarios = [
            # Network chaos with latency and packet loss on eth0 interface
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=duration,
                label_selector=ceph_component_label,
                instance_count=instance_count,
                interfaces=["eth0"],
                execution=execution,
                egress=egress_config,
            ),
            # Network chaos with different interface targeting (enp1s0 - common alternative)
            NetworkOutageScenarios.pod_network_chaos(
                scenario_dir,
                duration=duration // 2,  # Shorter duration for second scenario
                label_selector=ceph_component_label,
                instance_count=max(
                    1, instance_count // 2
                ),  # Fewer instances for second scenario
                interfaces=["enp1s0", "eth0"],
                execution=(
                    "serial" if execution == "parallel" else "parallel"
                ),  # Alternate execution
                egress={
                    "latency": "30ms",
                    "loss": 0.015,
                },  # Milder impairment for second scenario
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
        ), f"Network chaos scenarios failed for {ceph_component_label} with pod errors: {failing_scenarios}"

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

        log.info(
            f"Network ingress chaos test for {node_type} nodes completed successfully"
        )

    @pytest.mark.parametrize(
        "ceph_component_label,instance_count,execution_type,network_params",
        [
            (
                OSD_APP_LABEL,
                2,
                "parallel",
                {"latency": "60ms", "loss": "'0.025%'", "bandwidth": "80mbit"},
            ),  # OSDs with egress traffic shaping
            (
                MGR_APP_LABEL,
                1,
                "serial",
                {"latency": "40ms", "loss": "'0.015%'", "bandwidth": "100mbit"},
            ),  # MGR with moderate egress shaping
            (
                MON_APP_LABEL,
                1,
                "serial",
                {"latency": "30ms", "loss": "'0.01%'", "bandwidth": "120mbit"},
            ),  # MON with minimal egress disruption
            (
                RGW_APP_LABEL,
                2,
                "parallel",
                {"latency": "70ms", "loss": "'0.03%'", "bandwidth": "60mbit"},
            ),  # RGW with egress bandwidth constraints
        ],
        ids=[
            "osd-egress-shaping",
            "mgr-egress-shaping",
            "mon-egress-shaping",
            "rgw-egress-shaping",
        ],
    )
    def test_krkn_pod_egress_shaping(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        instance_count,
        execution_type,
        network_params,
    ):
        """
        Test pod egress shaping scenarios using Krkn pod_egress_shaping template.

        This test validates network resilience by applying egress traffic shaping
        (latency, packet loss, bandwidth limitation) to different Ceph component pods
        and verifying that the system can handle these network impairments gracefully.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario files
            workload_ops: Workload operations fixture
            ceph_component_label: Parameterized Ceph component label
            instance_count: Number of pods to target for chaos injection
            execution_type: Execution mode ('serial' or 'parallel')
            network_params: Network impairment configuration
        """
        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        log.info(
            f"Testing pod egress shaping for Ceph component: {ceph_component_label} "
            f"with instance_count={instance_count}, execution_type={execution_type}, "
            f"network_params={network_params}"
        )

        # Create pod egress shaping scenarios targeting the specific Ceph component
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
            # Secondary scenario with different shaping parameters
            NetworkOutageScenarios.pod_egress_shaping(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=ceph_component_label,
                instance_count=max(1, instance_count // 2),
                execution_type="serial" if execution_type == "parallel" else "parallel",
                network_params={
                    "latency": "35ms",
                    "loss": "'0.018%'",
                    "bandwidth": "90mbit",
                },
                wait_duration=480,
                test_duration=180,
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
            log.info(f"Starting pod egress shaping test for {ceph_component_label}")
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(f"Pod egress shaping test completed for {ceph_component_label}")
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
        ), f"Pod egress shaping scenarios failed for {ceph_component_label} with pod errors: {failing_scenarios}"

        log.info(
            f"Pod egress shaping test for {ceph_component_label} completed successfully"
        )

    @pytest.mark.parametrize(
        "ceph_component_label,instance_count,execution_type,network_params",
        [
            (
                OSD_APP_LABEL,
                2,
                "parallel",
                {"latency": "55ms", "loss": "'0.022%'", "bandwidth": "85mbit"},
            ),  # OSDs with ingress traffic shaping
            (
                MGR_APP_LABEL,
                1,
                "serial",
                {"latency": "35ms", "loss": "'0.012%'", "bandwidth": "110mbit"},
            ),  # MGR with conservative ingress shaping
            (
                MON_APP_LABEL,
                1,
                "serial",
                {"latency": "25ms", "loss": "'0.008%'", "bandwidth": "130mbit"},
            ),  # MON with minimal ingress disruption (critical component)
            (
                RGW_APP_LABEL,
                2,
                "parallel",
                {"latency": "65ms", "loss": "'0.028%'", "bandwidth": "70mbit"},
            ),  # RGW with ingress bandwidth constraints
        ],
        ids=[
            "osd-ingress-shaping",
            "mgr-ingress-shaping",
            "mon-ingress-shaping",
            "rgw-ingress-shaping",
        ],
    )
    def test_krkn_pod_ingress_shaping(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        ceph_component_label,
        instance_count,
        execution_type,
        network_params,
    ):
        """
        Test pod ingress shaping scenarios using Krkn pod_ingress_shaping template.

        This test validates network resilience by applying ingress traffic shaping
        (latency, packet loss, bandwidth limitation) to different Ceph component pods
        and verifying that the system can handle these network impairments gracefully.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario files
            workload_ops: Workload operations fixture
            ceph_component_label: Parameterized Ceph component label
            instance_count: Number of pods to target for chaos injection
            execution_type: Execution mode ('serial' or 'parallel')
            network_params: Network impairment configuration
        """
        scenario_dir = krkn_scenario_directory
        openshift_storage_ns = constants.OPENSHIFT_STORAGE_NAMESPACE

        log.info(
            f"Testing pod ingress shaping for Ceph component: {ceph_component_label} "
            f"with instance_count={instance_count}, execution_type={execution_type}, "
            f"network_params={network_params}"
        )

        # Create pod ingress shaping scenarios targeting the specific Ceph component
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
            # Secondary scenario with different shaping parameters
            NetworkOutageScenarios.pod_ingress_shaping(
                scenario_dir,
                namespace=openshift_storage_ns,
                label_selector=ceph_component_label,
                instance_count=max(1, instance_count // 2),
                execution_type="serial" if execution_type == "parallel" else "parallel",
                network_params={
                    "latency": "32ms",
                    "loss": "'0.016%'",
                    "bandwidth": "95mbit",
                },
                wait_duration=480,
                test_duration=180,
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
            log.info(f"Starting pod ingress shaping test for {ceph_component_label}")
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            log.info(f"Pod ingress shaping test completed for {ceph_component_label}")
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
        ), f"Pod ingress shaping scenarios failed for {ceph_component_label} with pod errors: {failing_scenarios}"

        log.info(
            f"Pod ingress shaping test for {ceph_component_label} completed successfully"
        )
