import time
import random
import logging
import subprocess
from ocs_ci.ocs import ocp
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    CephHealthException,
    NoRunningCephToolBoxException,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.platform_nodes import PlatformNodesFactory

log = logging.getLogger(__name__)


class NetworkFaults(PlatformNodesFactory):
    """
    A class to inject and remove various network faults on OpenShift cluster nodes
    using the 'tc' Linux command via 'oc debug'. Supports simulation of network issues
    such as packet loss, latency, duplication, and corruption.
    """

    def __init__(
        self, nodes, interface_types=["default"], duration=30, iterations=4, pause=15
    ):
        """
        Initializes the NetworkFaults object.

        Args:
            nodes (list): List of node objects.
            interface_types (list): List of interface types to use (e.g., "default", "ovn").
            duration (int): Time in seconds to hold the fault per iteration.
            iterations (int): Number of iterations to apply the fault.
            pause (int): Pause duration in seconds between fault iterations.
        """
        super().__init__()
        self.nodes = nodes
        self.duration = duration
        self.iterations = iterations
        self.pause = pause
        self.ocp_obj = ocp.OCP()
        self.platform_node_obj = self.get_nodes_platform()
        self.node_interfaces = self._get_all_node_network_interfaces(interface_types)

        log.info(
            f"Initialized NetworkFaults class with nodes: {','.join([node.name for node in self.nodes])}, "
            f"interfaces per node: {self.node_interfaces}, duration: {self.duration}s, iterations: {self.iterations}"
        )

    def _get_all_node_network_interfaces(self, interface_types):
        """
        Gathers the network interface names on each node for the given interface types.

        Args:
            interface_types (list): Types of interfaces to collect (e.g., "default", "ovn").

        Returns:
            dict: Mapping of node names to a list of interface names.
        """
        node_interfaces = {}
        for node in self.nodes:
            interfaces = []
            for interface_type in interface_types:
                if interface_type == "default":
                    cmd = "ip route | awk '/^default/ {print $5}'"
                elif interface_type == "ovn":
                    cmd = "ip -o link show | grep ovn | awk -F: '{print $2}'"
                else:
                    raise ValueError(f"Unsupported interface type: {interface_type}")

                log.debug(
                    f"Fetching '{interface_type}' interface(s) from node {node.name}"
                )
                try:
                    output = self.ocp_obj.exec_oc_debug_cmd(
                        node=node.name, cmd_list=[cmd]
                    )
                    ifaces = [
                        iface.strip() for iface in output.splitlines() if iface.strip()
                    ]
                    interfaces.extend(ifaces)
                except CommandFailed as e:
                    log.error(f"Error retrieving interfaces from node {node.name}: {e}")
                    continue

            interfaces = list(set(interfaces))
            if not interfaces:
                log.warning(f"No interfaces found for node {node.name}")
            node_interfaces[node.name] = interfaces
        return node_interfaces

    def _apply_fault(self, description, netem_command):
        """
        Applies a specified tc netem fault in looped iterations across all interfaces.

        Examples of tc netem commands:
        - tc qdisc add dev <interface> root netem delay 100ms
        # Introduces 100ms constant latency

        - tc qdisc add dev <interface> root netem delay 100ms 20ms
        # 100ms average latency with ±20ms jitter

        - tc qdisc add dev <interface> root netem loss 10%
        # Simulates 10% packet loss

        - tc qdisc add dev <interface> root netem duplicate 5%
        # Duplicates 5% of the packets

        - tc qdisc add dev <interface> root netem corrupt 0.1%
        # Introduces bit-level corruption to 0.1% of packets

        - tc qdisc add dev <interface> root netem reorder 25% 50%
        # Reorders 25% of packets with a 50% correlation

        - tc qdisc add dev <interface> root netem delay 80ms 10ms loss 5%
        # 80ms latency ±10ms jitter and 5% packet loss

        - tc qdisc add dev <interface> root netem delay 50ms duplicate 2% corrupt 0.2%
        # Adds 50ms latency, duplicates 2% packets, and corrupts 0.2%

        Args:
            description (str): Description of the fault type for logs.
            netem_command (str): Netem command string to simulate the fault.
        """
        covered_nodes = set()

        for i in range(self.iterations):
            remaining_nodes = [
                node for node in self.nodes if node.name not in covered_nodes
            ]
            if not remaining_nodes:
                remaining_nodes = self.nodes.copy()

            count = min(len(remaining_nodes), random.randint(1, len(self.nodes)))
            selected_nodes = random.sample(remaining_nodes, count)

            for node in selected_nodes:
                interfaces = self.node_interfaces.get(node.name, [])
                for iface in interfaces:
                    log.info(
                        f"[Iteration {i+1}] Applying {description} on {node.name}/{iface}"
                    )
                    cmd = f"tc qdisc replace dev {iface} root netem {netem_command}"
                    try:
                        self.ocp_obj.exec_oc_debug_cmd(node=node.name, cmd_list=[cmd])
                        covered_nodes.add(node.name)
                    except (CommandFailed, subprocess.TimeoutExpired) as e:
                        log.error(f"Failed to apply fault on {node.name}/{iface}: {e}")

            log.info(f"[Iteration {i+1}] Holding fault for {self.duration}s")
            time.sleep(self.duration)

            for node in selected_nodes:
                interfaces = self.node_interfaces.get(node.name, [])
                for iface in interfaces:
                    log.info(
                        f"[Iteration {i+1}] Removing fault from {node.name}/{iface}"
                    )
                    cmd = f"tc qdisc del dev {iface} root"
                    try:
                        self.ocp_obj.exec_oc_debug_cmd(node=node.name, cmd_list=[cmd])
                    except CommandFailed as e:
                        log.error(
                            f"Failed to remove fault from {node.name}/{iface}: {e}"
                        )

            if i < self.iterations - 1:
                log.info(
                    f"[Iteration {i+1}] Pausing for {self.pause}s before next iteration"
                )
                time.sleep(self.pause)

        log.info("All iterations completed. Clearing any residual faults.")
        self._remove_faults_all_nodes()

    def _remove_faults_all_nodes(self):
        """
        Removes all netem qdiscs from all interfaces on all nodes,
        and verifies that the faults have been successfully cleared.
        """
        log.info("Performing cleanup of all interfaces on all nodes")

        for node in self.nodes:
            interfaces = self.node_interfaces.get(node.name, [])
            for iface in interfaces:
                cmd_del = f"tc qdisc del dev {iface} root || true"
                try:
                    self.ocp_obj.exec_oc_debug_cmd(node=node.name, cmd_list=[cmd_del])
                    log.debug(f"Deleted qdisc on {node.name}/{iface}")
                except CommandFailed as e:
                    log.warning(f"Could not delete qdisc on {node.name}/{iface}: {e}")
                    continue

                # Verify removal
                cmd_verify = f"tc qdisc show dev {iface}"
                try:
                    output = self.ocp_obj.exec_oc_debug_cmd(
                        node=node.name, cmd_list=[cmd_verify]
                    )
                    if "netem" in output:
                        log.error(
                            f"Verification failed: netem still active on {node.name}/{iface}"
                        )
                    else:
                        log.info(
                            f"Verified: netem successfully removed from {node.name}/{iface}"
                        )
                except CommandFailed as e:
                    log.warning(
                        f"Could not verify qdisc status on {node.name}/{iface}: {e}"
                    )

        time.sleep(5)
        log.info("All fault configurations attempted and verified.")

    def network_packet_loss(self, percentage=25):
        """Simulates packet loss on all nodes.

        Args:
            percentage (int): Packet loss percentage.
        """
        self._apply_fault(f"{percentage}% packet loss", f"loss {percentage}%")

    def add_network_latency(self, delay_ms=100, jitter_ms=20):
        """Simulates latency and jitter on all nodes.

        Args:
            delay_ms (int): Base delay in milliseconds.
            jitter_ms (int): Jitter in milliseconds.
        """
        self._apply_fault(
            f"{delay_ms}ms latency ±{jitter_ms}ms jitter",
            f"delay {delay_ms}ms {jitter_ms}ms",
        )

    def add_packet_duplication(self, duplicate_percent=5):
        """Simulates packet duplication on all nodes.

        Args:
            duplicate_percent (int): Packet duplication percentage.
        """
        self._apply_fault(
            f"{duplicate_percent}% packet duplication",
            f"duplicate {duplicate_percent}%",
        )

    def add_packet_corruption(self, corruption_percent=1):
        """Simulates packet corruption on all nodes.

        Args:
            corruption_percent (int): Packet corruption percentage.
        """
        self._apply_fault(
            f"{corruption_percent}% packet corruption", f"corrupt {corruption_percent}%"
        )

    def add_network_combine_faults(
        self, delay_ms=80, jitter_ms=20, loss_percent=10, duplicate_percent=1
    ):
        """Applies a combination of network faults.

        Args:
            delay_ms (int): Base latency in milliseconds.
            jitter_ms (int): Jitter in milliseconds.
            loss_percent (int): Packet loss percentage.
            duplicate_percent (int): Packet duplication percentage.
        """
        netem_cmd = (
            f"delay {delay_ms}ms {jitter_ms}ms "
            f"loss {loss_percent}% "
            f"duplicate {duplicate_percent}%"
        )
        desc = f"combined faults (delay={delay_ms}ms±{jitter_ms}ms, loss={loss_percent}%, dup={duplicate_percent}%)"
        self._apply_fault(desc, netem_cmd)

    def pre_fault_injection_checks(self):
        """Perform any pre-fault sanity checks such as node readiness."""
        log.info("Performing pre-fault injection checks (placeholder)")

    def post_fault_injection_checks(self):
        """Verifies Ceph cluster health and recovers from node failures if needed."""
        log.info("Verifying post-fault Ceph cluster health")
        try:
            if ceph_health_check(tries=3, delay=20):
                log.info("Ceph cluster is healthy post-fault")
                return
        except (CephHealthException, CommandFailed, subprocess.TimeoutExpired) as e:
            log.error(f"Initial post-fault check failed: {e}")

        log.warning("Ceph cluster unhealthy, initiating node restart")
        self.platform_node_obj.restart_nodes_by_stop_and_start(self.nodes)

        try:
            if ceph_health_check(tries=5, delay=30):
                log.info("Ceph cluster recovered after reboot")
            else:
                log.error("Ceph cluster still unhealthy after node reboot")
        except (
            CephHealthException,
            CommandFailed,
            subprocess.TimeoutExpired,
            NoRunningCephToolBoxException,
        ) as e:
            log.error(f"Final post-reboot health check failed: {e}")

    def run(self):
        """
        Executes all network fault types in randomized order.
        Performs pre and post fault injection checks.
        """
        fault_methods = [
            self.network_packet_loss,
            self.add_network_latency,
            self.add_packet_duplication,
            self.add_packet_corruption,
            self.add_network_combine_faults,
        ]
        random.shuffle(fault_methods)

        log.info("Beginning full network fault injection sequence")
        for method in fault_methods:
            log.info(f"Invoking fault scenario: {method.__name__}")
            self.pre_fault_injection_checks()
            method()
            self.post_fault_injection_checks()
        log.info("Completed all fault injection scenarios")
