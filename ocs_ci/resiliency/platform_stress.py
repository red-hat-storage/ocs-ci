import time
import subprocess
import random
import threading
import logging
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    NoRunningCephToolBoxException,
)
from ocs_ci.ocs import ocp
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


class PlatformStress:
    """A class to perform stress testing on OpenShift cluster nodes using stress-ng."""

    def __init__(self, nodes):
        """Initializes PlatformStress with the given nodes.

        Args:
            nodes (list): List of node objects to perform stress testing on.
        """
        self.nodes = nodes
        self.ocp_obj = ocp.OCP()
        self.run_status = False  # Flag to control stress test execution
        self.active_threads = []  # To keep track of active threads
        self.stop_event = threading.Event()  # Event to signal threads to stop
        log.info("Initialized PlatformStress with nodes: %s", [n.name for n in nodes])

    def _apply_stress(self, node_obj, cmd_args, timeout=60, wait=True):
        """Applies stress to a node using stress-ng with given arguments.

        Args:
            node_obj (object): The node object to apply stress to.
            cmd_args (str): Arguments to pass to stress-ng.
            timeout (int, optional): Timeout in seconds for the stress command. Defaults to 60.
            wait (bool, optional): Whether to wait for command completion. Defaults to True.

        Returns:
            bool: True if stress command succeeded, False otherwise.
        """
        success = True
        full_cmd = f"stress-ng {cmd_args} --timeout {timeout}s"
        log.info("[%s] Executing: %s", node_obj.name, full_cmd)
        try:
            self.ocp_obj.exec_oc_debug_cmd(
                node=node_obj.name,
                cmd_list=[full_cmd],
                use_root=False,
                timeout=timeout + 20,
            )
        except (
            CommandFailed,
            subprocess.TimeoutExpired,
            NoRunningCephToolBoxException,
        ) as e:
            log.error("[%s] Stress command failed: %s", node_obj.name, e)
            success = False

        if wait:
            log.debug("Waiting %s seconds for stress command to complete...", timeout)
            # Check stop_event periodically instead of just sleeping
            for _ in range(timeout):
                if self.stop_event.is_set():
                    log.info(
                        "[%s] Stress test interrupted by stop event", node_obj.name
                    )
                    return False
                time.sleep(1)
        return success

    def cpu_stress(self, node_obj, load_percentage=95, timeout=60):
        """Applies CPU stress to a node.

        Args:
            node_obj (object): The node object to apply CPU stress to.
            load_percentage (int, optional): CPU load percentage. Defaults to 95.
            timeout (int, optional): Timeout in seconds. Defaults to 60.

        Returns:
            bool: True if stress command succeeded, False otherwise.
        """
        if self.stop_event.is_set():
            return False
        cpu_count = int(node_obj.data["status"]["capacity"]["cpu"])
        log.info(
            "Running CPU stress test: %d processes at %d%% load",
            cpu_count,
            load_percentage,
        )
        return self._apply_stress(
            node_obj,
            f"--cpu {cpu_count} --cpu-load {load_percentage} --cpu-method matrixprod",
            timeout,
        )

    def memory_stress(self, node_obj, process=1, load_percentage=95, timeout=120):
        """Applies memory stress to a node.

        Args:
            node_obj (object): The node object to apply memory stress to.
            process (int, optional): Number of processes. Defaults to 1.
            load_percentage (int, optional): Memory load percentage. Defaults to 95.
            timeout (int, optional): Timeout in seconds. Defaults to 120.

        Returns:
            bool: True if stress command succeeded, False otherwise.
        """
        if self.stop_event.is_set():
            return False
        log.info(
            "Running memory stress test: %d processes at %d%% memory",
            process,
            load_percentage,
        )
        return self._apply_stress(
            node_obj, f"--vm {process} --vm-bytes {load_percentage}%", timeout
        )

    def io_stress(self, node_obj, process=8, timeout=60):
        """Applies I/O stress to a node.

        Args:
            node_obj (object): The node object to apply I/O stress to.
            process (int, optional): Number of processes. Defaults to 8.
            timeout (int, optional): Timeout in seconds. Defaults to 60.

        Returns:
            bool: True if stress command succeeded, False otherwise.
        """
        if self.stop_event.is_set():
            return False
        log.info("Running I/O stress test: %d processes", process)
        return self._apply_stress(
            node_obj,
            f"--hdd {process} --hdd-bytes 4G --temp-path /mnt/disk1 --hdd-opts sync",
            timeout,
        )

    def network_stress(self, node_obj, timeout=60):
        """Applies network stress to a node.

        Args:
            node_obj (object): The node object to apply network stress to.
            timeout (int, optional): Timeout in seconds. Defaults to 60.

        Returns:
            bool: True if stress command succeeded, False otherwise.
        """
        cpu_count = int(node_obj.data["status"]["capacity"]["cpu"])
        socket = cpu_count

        if self.stop_event.is_set():
            return False
        log.info("Running network stress test: %d socket stressors", socket)
        return self._apply_stress(node_obj, f"--sock {socket}", timeout)

    def all_stress(self, node_obj, vm=1, io=4, timeout=60):
        """Applies combined stress (CPU, memory, I/O, network) to a node.

        Args:
            node_obj (object): The node object to apply stress to.
            vm (int, optional): Number of VM stressors. Defaults to 1.
            io (int, optional): Number of I/O stressors. Defaults to 4.
            timeout (int, optional): Timeout in seconds. Defaults to 60.

        Returns:
            bool: True if stress command succeeded, False otherwise.
        """
        if self.stop_event.is_set():
            return False
        cpu_count = int(node_obj.data["status"]["capacity"]["cpu"])
        sock = cpu_count
        log.info(
            "Running combined stress test: CPU=%d, VM=%d, IO=%d, SOCK=%d",
            cpu_count,
            vm,
            io,
            sock,
        )
        return self._apply_stress(
            node_obj,
            f" --cpu {cpu_count} --vm {vm} --iomix {io} --sock {sock} --temp-path /mnt/disk1 ",
            timeout,
        )

    def _run_random_stress_loop(self, timeout=0, node_selection="ALL"):
        """Internal method that runs in a loop to apply random stress tests.

        Args:
            timeout (int, optional): Timeout for each stress test. 0 means random.
            node_selection (str, optional): "ALL" for all nodes or "RANDOM" for subset.
        """
        stress_funcs = [
            self.cpu_stress,
            self.memory_stress,
            self.io_stress,
            self.network_stress,
            self.all_stress,
        ]

        while not self.stop_event.is_set():
            if not self.nodes:
                log.warning("No nodes available for stress test.")
                break

            subset_nodes = (
                self.nodes
                if node_selection.upper() == "ALL"
                else random.sample(self.nodes, min(2, len(self.nodes)))
            )
            current_timeout = timeout or random.randint(30, 120)
            selected = random.choice(stress_funcs)
            log.info(
                "Randomly selected %s for nodes: %s",
                selected.__name__,
                [n.name for n in subset_nodes],
            )

            for node in subset_nodes:
                if self.stop_event.is_set():
                    break
                thread = threading.Thread(
                    target=selected, args=(node,), kwargs={"timeout": current_timeout}
                )
                thread.start()
                self.active_threads.append(thread)
                log.info(
                    "Started thread for %s on node: %s", selected.__name__, node.name
                )

            # Wait for current stress tests to complete or stop event
            for thread in list(self.active_threads):
                if self.stop_event.is_set():
                    break
                thread.join(timeout=1)
                if not thread.is_alive():
                    self.active_threads.remove(thread)

            if not ceph_health_check(fix_ceph_health=True):
                log.error("Ceph health check failed after scenario execution.")

    def start_random_stress(self, timeout=0, node_selection="ALL"):
        """Starts random stress tests in the background.

        Args:
            timeout (int, optional): Timeout for each stress test. 0 means random.
            node_selection (str, optional): "ALL" for all nodes or "RANDOM" for subset.

        Returns:
            bool: True if stress test started successfully, False otherwise.
        """
        if self.run_status:
            log.warning("Random stress test is already running")
            return False

        self.stop_event.clear()
        self.run_status = True
        self.background_thread = threading.Thread(
            target=self._run_random_stress_loop,
            args=(timeout, node_selection),
            daemon=True,
        )
        self.background_thread.start()
        log.info("Started random stress test in background")
        return True

    def stop(self):
        """Stops all running stress tests.

        Returns:
            bool: True if stress tests were stopped successfully, False otherwise.
        """
        if not self.run_status:
            log.warning("No stress test is currently running")
            return False

        self.stop_event.set()
        self.run_status = False

        # Wait for background thread to finish
        if hasattr(self, "background_thread") and self.background_thread.is_alive():
            self.background_thread.join(timeout=10)

        # Wait for active stress threads to finish
        for thread in list(self.active_threads):
            thread.join(timeout=5)
            if not thread.is_alive():
                self.active_threads.remove(thread)

        log.info("All stress tests have been stopped")
        return True

    def __del__(self):
        """Cleans up when the object is deleted by stopping any running stress tests."""
        if self.run_status:
            self.stop()
