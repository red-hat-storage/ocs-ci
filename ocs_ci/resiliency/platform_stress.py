import time
import subprocess
import random
import threading
import logging
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_nodes
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    NoRunningCephToolBoxException,
)
from ocs_ci.ocs import ocp

log = logging.getLogger(__name__)


class PlatformStress:
    """
    A class to perform stress testing on OpenShift cluster nodes using stress-ng.
    """

    def __init__(self):
        """Initializes the PlatformStress instance with all worker and master nodes."""
        self.nodes = [
            node
            for nt in [constants.WORKER_MACHINE, constants.MASTER_MACHINE]
            for node in get_nodes(nt)
        ]
        self.ocp_obj = ocp.OCP()
        self.run_status = True

    def _apply_stress(self, nodes, cmd_args, timeout=30, wait=True):
        """
        Apply a stress-ng command to a list of nodes.

        Args:
            nodes (list): List of node objects.
            cmd_args (str): Arguments to pass to stress-ng.
            timeout (int, optional): Duration to run the stress command in seconds. Defaults to 30.
            wait (bool, optional): Whether to wait after executing the command. Defaults to True.

        Returns:
            bool: True if command executed successfully on all nodes, False otherwise.
        """
        success = True
        for node in nodes:
            full_cmd = f"stress-ng {cmd_args} --timeout {timeout}s"
            log.info(f"[{node.name}] Executing: {full_cmd}")
            try:
                self.ocp_obj.exec_oc_debug_cmd(node=node.name, cmd_list=[full_cmd])
            except (
                CommandFailed,
                subprocess.TimeoutExpired,
                NoRunningCephToolBoxException,
            ) as e:
                log.error(f"[{node.name}] Stress command failed: {e}")
                success = False

        if wait:
            log.debug("Waiting for stress tests to complete...")
            time.sleep(timeout)

        return success

    def cpu_stress(self, nodes, process=4, load_percentage=70, timeout=60):
        """
        Run a CPU stress test on the specified nodes.

        Args:
            nodes (list): List of node objects.
            process (int): Number of CPU stressors.
            load_percentage (int): Target CPU load percentage.
            timeout (int): Duration of the test in seconds.

        Returns:
            bool: True if test ran successfully.
        """
        cmd = f"--cpu {process} --cpu-load {load_percentage}"
        return self._apply_stress(nodes, cmd, timeout)

    def memory_stress(self, nodes, process=4, load_percentage=70, timeout=60):
        """
        Run a memory stress test on the specified nodes.

        Args:
            nodes (list): List of node objects.
            process (int): Number of memory stressors.
            load_percentage (int): Target memory usage percentage.
            timeout (int): Duration of the test in seconds.

        Returns:
            bool: True if test ran successfully.
        """
        cmd = f"--vm {process} --vm-bytes {load_percentage}%"
        return self._apply_stress(nodes, cmd, timeout)

    def io_stress(self, nodes, process=4, timeout=60):
        """
        Run an IO (disk) stress test on the specified nodes.

        Args:
            nodes (list): List of node objects.
            process (int): Number of IO stressors.
            timeout (int): Duration of the test in seconds.

        Returns:
            bool: True if test ran successfully.
        """
        cmd = f"--hdd {process} --hdd-bytes 2G"
        return self._apply_stress(nodes, cmd, timeout)

    def network_stress(self, nodes, socket=4, timeout=60):
        """
        Run a network stress test on the specified nodes.

        Args:
            nodes (list): List of node objects.
            socket (int): Number of socket stressors.
            timeout (int): Duration of the test in seconds.

        Returns:
            bool: True if test ran successfully.
        """
        cmd = f"--sock {socket}"
        return self._apply_stress(nodes, cmd, timeout)

    def all_stress(self, nodes, cpu=1, vm=1, io=1, sock=1, timeout=60):
        """
        Run all types of stress tests on the specified nodes.

        Args:
            nodes (list): List of node objects.
            cpu (int): Number of CPU stressors.
            vm (int): Number of memory stressors.
            io (int): Number of IO stressors.
            sock (int): Number of socket stressors.
            timeout (int): Duration of the test in seconds.

        Returns:
            bool: True if all stress tests ran successfully.
        """
        cmd = f"--cpu {cpu} --vm {vm} --io {io} --sock {sock}"
        return self._apply_stress(nodes, cmd, timeout)

    def run_random_stress(self, timeout=0):
        """
        Run a randomly selected stress test on a random subset of nodes.

        Args:
            timeout (int, optional): Duration of the test in seconds. Defaults to 60.

        Returns:
            bool: True if test ran successfully.
        """
        if not self.nodes:
            log.warning("No available nodes to run stress.")
            return False

        stress_tests = [
            self.cpu_stress,
            self.memory_stress,
            self.io_stress,
            self.network_stress,
            self.all_stress,
        ]
        nodes = random.sample(self.nodes, min(2, len(self.nodes)))
        timeout = timeout or random.randint(30, 60)
        selected_test = random.choice(stress_tests)
        log.info(f"Running {selected_test.__name__} on {[n.name for n in nodes]}")
        return selected_test(nodes, timeout=timeout)

    def run(self, interval=10):
        """
        Start the stress test in a background thread.

        Args:
            interval (int, optional): Wait time between test cycles in seconds. Defaults to 10.
        """
        if hasattr(self, "_thread") and self._thread and self._thread.is_alive():
            log.warning("Stress test is already running.")
            return

        self.run_status = True
        self._thread = threading.Thread(
            target=self._thread_target, args=(interval,), daemon=True
        )
        self._thread.start()
        log.info("Stress test started in background thread.")

    def _thread_target(self, interval):
        """
        Target method for background thread to run random stress tests.

        Args:
            interval (int): Interval between tests.
        """
        log.info("Background stress thread running...")
        while self.run_status:
            try:
                self.run_random_stress()
                if interval:
                    log.info(f"Sleeping for {interval} seconds before next test.")
                    time.sleep(interval)
            except (RuntimeError, TimeoutError, subprocess.TimeoutExpired) as e:
                log.exception(f"Unexpected error in stress loop: {e}")
                self.run_status = False
                break
        log.info("Background stress thread stopped.")

    def stop(self):
        """
        Stop the background stress test thread.

        Returns:
            bool: True if successfully stopped.
        """
        if not getattr(self, "run_status", False):
            log.info("Stress test is not running.")
            return True

        log.info("Stopping stress test...")
        self.run_status = False

        if hasattr(self, "_thread") and self._thread:
            self._thread.join()
            log.info("Stress test thread joined successfully.")

        return True
