import ipaddress
import logging
import re
import pytest

from ocs_ci.ocs.resources.pod import get_mon_pods
from ocs_ci.framework.testlib import (
    tier1,
    brown_squad,
    polarion_id,
    skipif_ocs_version,
    skipif_external_mode,
    runs_on_provider,
)

log = logging.getLogger(__name__)


@tier1
@brown_squad
@skipif_ocs_version("<4.15")
class TestGetCommands:
    @pytest.fixture(autouse=True)
    def setup(self, odf_cli_setup):
        self.odf_cli_runner = odf_cli_setup

    @skipif_external_mode
    @runs_on_provider
    @polarion_id("OCS-6237")
    def test_get_health(self):
        output = self.odf_cli_runner.run_get_health()
        self.validate_mon_pods(output)
        self.validate_mon_quorum_and_health(output)
        self.validate_osd_pods(output)
        self.validate_running_pods(output)
        self.validate_pg_status(output)
        self.validate_mgr_pods(output)
        self.validate_cluster_capacity(output)
        self.validate_node_pressure(output)
        self.validate_noobaa(output)

    @skipif_external_mode
    @runs_on_provider
    @polarion_id("OCS-6238")
    def test_get_mon_endpoint(self):
        result = self.odf_cli_runner.run_get_mon_endpoint()
        output = result.stdout.decode().strip()
        assert output, "Mon endpoint not found in output"
        # Validate the format of the mon endpoint output
        # Supports both IPv4 (ip:port) and IPv6 ([ip]:port) formats
        ipv4_ep = r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+"
        ipv6_ep = r"\[[0-9a-fA-F:]+\]:\d+"
        endpoint_pattern = rf"^(({ipv4_ep}|{ipv6_ep}),?)+$"
        assert re.match(
            endpoint_pattern, output
        ), f"Invalid mon endpoint format: {output}"

        # Get the number of monitor pods
        mon_pods = get_mon_pods()
        expected_mon_count = len(mon_pods)

        # Split endpoints — handle both IPv4 (a.b.c.d:port,) and
        # IPv6 ([addr]:port,) by splitting on the pattern "],"|","
        endpoints = re.split(r"(?<=\d),", output.strip())
        assert (
            len(endpoints) == expected_mon_count
        ), f"Expected {expected_mon_count} mon endpoints, but found {len(endpoints)}"

        # Validate each endpoint
        for endpoint in endpoints:
            if endpoint.startswith("["):
                # IPv6 format: [addr]:port
                addr_str, port_str = endpoint.rsplit(":", 1)
                addr_str = addr_str.strip("[]")
                ipaddress.IPv6Address(addr_str)
            else:
                # IPv4 format: addr:port
                addr_str, port_str = endpoint.rsplit(":", 1)
                ipaddress.IPv4Address(addr_str)
            assert (
                1 <= int(port_str) <= 65535
            ), f"Invalid port number in endpoint: {endpoint}"

    def validate_mon_pods(self, output):
        mon_status = re.search(
            r"(\d+) mon pods running on (\d+) different nodes",
            output.stdout.decode(),
        )
        assert mon_status, "Mon distribution status not found in output"
        pod_count, node_count = int(mon_status.group(1)), int(mon_status.group(2))
        assert pod_count >= 3, f"Expected at least 3 mon pods, found {pod_count}"
        assert (
            node_count >= 3
        ), f"Mon pods should be on at least 3 different nodes, found {node_count}"

    def validate_mon_quorum_and_health(self, output):
        health_ok = "HEALTH_OK" in output.stdout.decode()
        assert health_ok, "Ceph health is not OK"

    def validate_osd_pods(self, output):
        osd_status = re.search(
            r"(\d+) osd pods running on (\d+) different nodes",
            output.stdout.decode(),
        )
        assert osd_status, "OSD distribution status not found in output"
        pod_count, node_count = int(osd_status.group(1)), int(osd_status.group(2))
        assert pod_count >= 3, f"Expected at least 3 OSD pods, found {pod_count}"
        assert (
            node_count >= 3
        ), f"OSD pods should be on at least 3 different nodes, found {node_count}"

    def validate_running_pods(self, output):
        pods_status = re.search(
            r"All (\d+) pods are Running/Succeeded",
            output.stdout.decode(),
        )
        assert pods_status, "Running pods status not found in output"
        pod_count = int(pods_status.group(1))
        assert pod_count > 0, f"Expected positive pod count, found {pod_count}"
        log.info(f"Found {pod_count} running or succeeded pods")

    def validate_pg_status(self, output):
        pg_status = re.search(
            r"PgState: (.*?), PgCount: (\d+)",
            output.stdout.decode(),
        )
        assert pg_status, "Placement group status not found in output"
        pg_state, pg_count = pg_status.groups()
        assert (
            pg_state == "active+clean"
        ), f"Expected PG state to be 'active+clean', found '{pg_state}'"
        assert int(pg_count) > 0, f"Expected positive PG count, found {pg_count}"

    def validate_mgr_pods(self, output):
        mgr_status = re.search(
            r"(\d+) mgr pod\(s\) running",
            output.stdout.decode(),
        )
        assert mgr_status, "MGR status not found in output"
        mgr_count = int(mgr_status.group(1))
        assert mgr_count >= 1, f"Expected at least 1 MGR pod, found {mgr_count}"
        log.info(f"Found {mgr_count} running MGR pods")

    def validate_cluster_capacity(self, output):
        capacity_match = re.search(
            r"Cluster capacity [\d.]+% used",
            output.stdout.decode(),
        )
        assert capacity_match, "Cluster capacity not found in output"

    def validate_node_pressure(self, output):
        assert (
            "[OK] Node Resource Pressure [OK]" in output.stdout.decode()
        ), "Node resource pressure check did not pass"

    def validate_noobaa(self, output):
        stdout = output.stdout.decode()
        if "NooBaa" not in stdout:
            log.info("NooBaa not present in health output, skipping validation")
            return
        assert "[OK] NooBaa" in stdout, "NooBaa health check did not pass"
