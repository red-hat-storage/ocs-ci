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
    def cpu_stats(node_obj, format="json", interval=1, count=2):
        """
        Get CPU statistics for a given node using `mpstat`.

        Args:
            node_obj (OCSNode): Node object to query.
            format (str): Output format ('json' or 'text').
            interval (int): Interval between samples in seconds.
            count (int): Number of samples to collect.

        Returns:
            list or None: List of CPU stats dictionaries (if format is 'json'),
            or None for unsupported format.
        """
        ocp_obj = ocp.OCP(kind="node")

        if format == "json":
            cmd = f"debug nodes/{node_obj.name} -- mpstat {interval} {count} -o JSON"
        elif format == "text":
            cmd = f"debug nodes/{node_obj.name} -- mpstat {interval} {count} > /tmp/mpstat.txt"
            log.warning(
                "Text format selected. This method currently does not return parsed text output."
            )
            return None
        else:
            log.error(f"Unsupported format '{format}'. Use 'json' or 'text'.")
            return None

        try:
            log.info(
                f"Running mpstat on node '{node_obj.name}' with interval={interval}, count={count}"
            )
            cmd_output = ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
            if format == "json":
                output = json.loads(cmd_output)
                return (
                    output.get("sysstat", {})
                    .get("hosts", [{}])[0]
                    .get("statistics", [])
                )
        except CommandFailed as e:
            log.error(f"Failed to fetch CPU stats for node '{node_obj.name}': {e}")
        except json.JSONDecodeError as e:
            log.error(
                f"Failed to parse JSON output from mpstat on node '{node_obj.name}': {e}"
            )

        return []

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
        cmd = f"debug nodes/{node_obj.name} -- cat /proc/meminfo"

        try:
            output = ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)

            meminfo = {}
            for line in output.splitlines():
                key, value = line.strip().split(":", 1)
                meminfo[key] = int(value.strip().split()[0])  # in KB

            mem_total = meminfo.get("MemTotal")
            mem_available = meminfo.get("MemAvailable")

            if mem_total and mem_available:
                used_percent = ((mem_total - mem_available) / mem_total) * 100
                log.info(f"Memory usage on node {node_obj.name}: {used_percent:.2f}%")
                return round(used_percent, 2)
            else:
                log.warning("Missing MemTotal or MemAvailable in /proc/meminfo")
                return 0.0

        except CommandFailed as e:
            log.error(f"Failed to compute memory usage on node {node_obj.name}: {e}")
            return 0.0

    @staticmethod
    def disk_stats(node_obj):
        """
        Get disk I/O statistics using `iostat`.

        Args:
            node_obj (OCSNode): Node object to query.

        Returns:
            dict: Latest disk statistics.
        """
        ocp_obj = ocp.OCP(kind="node")
        cmd = f"debug nodes/{node_obj.name} -- iostat -xt -o JSON 1 2"

        try:
            log.info(f"Running disk stats command on node: {node_obj.name}")
            cmd_output = ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
            output = json.loads(cmd_output)
            stats = (
                output.get("sysstat", {}).get("hosts", [{}])[0].get("statistics", [])
            )
            return stats[-1] if stats else {}
        except CommandFailed as e:
            log.error(f"Failed to fetch disk stats for node {node_obj.name}: {e}")
            return {}

    @staticmethod
    def network_stats(node_obj, interface="ovn-k8s-mp0"):
        """
        Get network interface statistics using `sar`.

        Args:
            node_obj (OCSNode): Node object to query.

        Returns:
            str: Network interface statistics as text.
        """
        ocp_obj = ocp.OCP(kind="node")
        cmd = f"debug nodes/{node_obj.name} -- sar -n DEV 1 1"

        try:
            log.info(f"Running network stats command on node: {node_obj.name}")
            return ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
        except CommandFailed as e:
            log.error(f"Failed to fetch network stats for node {node_obj.name}: {e}")
            return ""
