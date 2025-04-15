import logging
import json
from ocs_ci.ocs import ocp
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


class NodeStats:
    """
    Class to retrieve and manage OpenShift node statistics such as CPU, memory,
    disk, and network metrics from a given node using `oc debug`.
    """

    @staticmethod
    def cpu_stats(node_obj, interval=1, count=2, format="json"):
        """
        Get CPU statistics for a given node using `mpstat`.

        Args:
            node_obj (OCSNode): The node object to fetch stats from.
            interval (int): Interval in seconds between samples. Default 1.
            count (int): Number of samples to take. Default 2.
            format (str): Output format - "json" or "text". Default "json".

        Returns:
            list or str or None: Parsed statistics list (JSON), raw output (text),
                                or None on failure.
        """
        log.info(
            f"Running mpstat on node '{node_obj.name}' with interval={interval}, count={count}, format={format}"
        )

        if format not in ("json", "text"):
            log.error(f"Unsupported format '{format}'. Use 'json' or 'text'.")
            return None

        cmd = f"mpstat {interval} {count}"
        if format == "json":
            cmd += " -o JSON"
        elif format == "text":
            cmd += " > /tmp/mpstat.txt && cat /tmp/mpstat.txt"
            log.warning("Text format selected. Output will be raw and unparsed.")

        ocp_obj = ocp.OCP(kind="node")
        try:
            cmd_output = ocp_obj.exec_oc_debug_cmd(
                node=node_obj.name, cmd_list=[cmd], use_root=False
            )

            if not cmd_output:
                log.warning("Empty response received from mpstat command")
                return None

            if format == "json":
                try:
                    output = json.loads(cmd_output)
                    return (
                        output.get("sysstat", {})
                        .get("hosts", [{}])[0]
                        .get("statistics", [])
                    )
                except json.JSONDecodeError as e:
                    log.error(
                        f"Failed to parse JSON from mpstat on node '{node_obj.name}': {e}"
                    )
                    return None
            return cmd_output.splitlines()

        except CommandFailed as e:
            log.error(f"Failed to fetch CPU stats from node '{node_obj.name}': {e}")
            return None

    @staticmethod
    def memory_usage_percent(node_obj):
        """
        Calculate memory usage percentage from /proc/meminfo.

        Args:
            node_obj (OCSNode): Node object to query.

        Returns:
            float: Used memory percentage.
        """
        ocp_obj = ocp.OCP(kind="node")
        cmd = "cat /proc/meminfo"

        try:
            output = ocp_obj.exec_oc_debug_cmd(
                node=node_obj.name, cmd_list=[cmd], use_root=False, timeout=30
            )

            meminfo = {}
            for line in output.splitlines():
                try:
                    key, value = line.strip().split(":", 1)
                    meminfo[key] = int(value.strip().split()[0])  # in KB
                except (ValueError, IndexError):
                    continue

            mem_total = meminfo.get("MemTotal")
            mem_available = meminfo.get("MemAvailable")

            if mem_total and mem_available:
                used_percent = ((mem_total - mem_available) / mem_total) * 100
                log.info(f"Memory usage on node {node_obj.name}: {used_percent:.2f}%")
                return round(used_percent, 2)

            log.warning("Missing MemTotal or MemAvailable in /proc/meminfo")
            return 0.0

        except CommandFailed as e:
            log.error(
                f"Failed to compute memory usage on node {node_obj.name}:  {str(e)}"
            )
            return 0.0

    @staticmethod
    def disk_stats(node_obj, format="json", interval=1, count=2):
        """
        Get disk I/O statistics using `iostat`.

        Args:
            node_obj (OCSNode): Node object to query.
            format (str): Output format ("json" or "text"). Default "json".
            interval (int): Interval in seconds between samples. Default 1.
            count (int): Number of samples to take. Default 2.

        Returns:
            dict or list: Latest disk statistics (dict for JSON, list for text).
        """
        ocp_obj = ocp.OCP(kind="node")
        if format not in ("json", "text"):
            log.error(f"Unsupported format '{format}'. Use 'json' or 'text'.")
            return {}

        cmd = f"iostat -xt {interval} {count}"
        if format == "json":
            cmd += " -o JSON"

        try:
            output = ocp_obj.exec_oc_debug_cmd(
                node=node_obj.name, cmd_list=[cmd], use_root=False, timeout=30
            )

            if format == "json":
                try:
                    output = json.loads(output)
                    stats = (
                        output.get("sysstat", {})
                        .get("hosts", [{}])[0]
                        .get("statistics", [])
                    )
                    return stats[-1] if stats else {}
                except json.JSONDecodeError as e:
                    log.error(
                        f"Failed to parse JSON from iostat on node '{node_obj.name}': {e}"
                    )
                    return {}

            return output.splitlines()

        except CommandFailed as e:
            log.error(f"Failed to fetch disk stats from node '{node_obj.name}': {e}")
            return {}

    @staticmethod
    def network_stats(node_obj, interface="ovn-k8s-mp0", interval=1, count=2):
        """
        Get network interface statistics using `sar`.

        Args:
            node_obj (OCSNode): Node object to query.
            interface (str): Network interface to monitor. Default "ovn-k8s-mp0".
            interval (int): Interval in seconds between samples. Default 1.
            count (int): Number of samples to take. Default 2.

        Returns:
            list: Network interface statistics as text lines.
        """
        ocp_obj = ocp.OCP(kind="node")
        cmd = f"sar -n DEV {interval} {count}"

        try:
            output = ocp_obj.exec_oc_debug_cmd(
                node=node_obj.name, cmd_list=[cmd], use_root=False
            )
            log.info(f"Running network stats command on node: {node_obj.name}")
            return output.splitlines()
        except CommandFailed as e:
            log.error(f"Failed to fetch network stats for node {node_obj.name}: {e}")
            return []
