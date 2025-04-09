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
    def cpu_stats(node_obj):
        """
        Get CPU statistics for a given node using `mpstat`.

        Args:
            node_obj (OCSNode): Node object to query.

        Returns:
            list: List of CPU stats dictionaries parsed from mpstat JSON output.
        """
        ocp_obj = ocp.OCP(kind="node")
        cmd = f"debug nodes/{node_obj.name} -- mpstat 1 2 -o JSON"

        try:
            log.info(f"Running mpstat on node: {node_obj.name}")
            cmd_output = ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
            output = json.loads(cmd_output)
            return output.get("sysstat", {}).get("hosts", [{}])[0].get("statistics", [])
        except CommandFailed as e:
            log.error(f"Failed to fetch CPU stats for node {node_obj.name}: {e}")
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
