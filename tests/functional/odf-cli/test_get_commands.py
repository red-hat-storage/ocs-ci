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
)

log = logging.getLogger(__name__)


@tier1
@brown_squad
@skipif_ocs_version("<4.15")
class TestGetCommands:
    @pytest.fixture(autouse=True)
    def setup(self, odf_cli_setup):
        self.odf_cli_runner = odf_cli_setup

    @polarion_id("OCS-6237")
    def test_get_health(self):
        output = self.odf_cli_runner.run_get_health()
        self.validate_mon_pods(output)
        self.validate_mon_quorum_and_health(output)
        self.validate_osd_pods(output)
        self.validate_running_pods(output)
        self.validate_pg_status(output)
        self.validate_mgr_pods(output)

    @skipif_external_mode
    @polarion_id("OCS-6238")
    def test_get_mon_endpoint(self):
        result = self.odf_cli_runner.run_get_mon_endpoint()
        output = result.stdout.decode().strip()
        assert output, "Mon endpoint not found in output"
        # Validate the format of the mon endpoint output
        endpoint_pattern = r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+,?)+$"
        assert re.match(
            endpoint_pattern, output
        ), f"Invalid mon endpoint format: {output}"

        # Get the number of monitor pods
        mon_pods = get_mon_pods()
        expected_mon_count = len(mon_pods)

        # Check that we have the correct number of endpoints
        endpoints = output.strip().split(",")
        assert (
            len(endpoints) == expected_mon_count
        ), f"Expected {expected_mon_count} mon endpoints, but found {len(endpoints)}"

        # Validate each endpoint
        for endpoint in endpoints:
            ip, port = endpoint.split(":")
            assert (
                1 <= int(port) <= 65535
            ), f"Invalid port number in endpoint: {endpoint}"
            octets = ip.split(".")
            assert len(octets) == 4, f"Invalid IP address in endpoint: {endpoint}"
            assert all(
                0 <= int(octet) <= 255 for octet in octets
            ), f"Invalid IP address in endpoint: {endpoint}"

    def validate_mon_pods(self, output):
        mon_pods = [
            line
            for line in output.stdout.decode().split("\n")
            if "rook-ceph-mon-" in line
        ]
        assert (
            len(mon_pods) >= 3
        ), f"Expected at least 3 mon pods, found {len(mon_pods)}"
        nodes = set()
        for pod in mon_pods:
            assert "Running" in pod, f"Mon pod not in Running state: {pod}"
            node = pod.split()[-1]
            nodes.add(node)
        assert (
            len(nodes) >= 3
        ), f"Mon pods should be on at least 3 different nodes, found {len(nodes)}"

    def validate_mon_quorum_and_health(self, output):
        health_ok = "Info: HEALTH_OK" in output.stderr.decode()
        assert health_ok, "Ceph health is not OK"

    def validate_osd_pods(self, output):
        osd_pods = [
            line
            for line in output.stdout.decode().split("\n")
            if "rook-ceph-osd-" in line and "prepare" not in line
        ]
        assert (
            len(osd_pods) >= 3
        ), f"Expected at least 3 OSD pods, found {len(osd_pods)}"
        nodes = set()
        for pod in osd_pods:
            assert "Running" in pod, f"OSD pod not in Running state: {pod}"
            node = pod.split()[-1]
            nodes.add(node)
        assert (
            len(nodes) >= 3
        ), f"OSD pods should be on at least 3 different nodes, found {len(nodes)}"

    def validate_running_pods(self, output):
        pod_lines = output.stdout.decode().strip().split("\n")
        running_pods = [
            line
            for line in pod_lines
            if "\tRunning\t" in line or "\tSucceeded\t" in line
        ]

        assert running_pods, "No running or succeeded pods found in output"

        for pod in running_pods:
            pod_name, status, namespace, node = pod.split("\t")
            assert status in [
                "Running",
                "Succeeded",
            ], f"Pod {pod_name} not in Running or Succeeded state: {status}"

        log.info(f"Found {len(running_pods)} running or succeeded pods")

    def validate_pg_status(self, output):
        pg_status = re.search(
            r"Info: Checking placement group status\nInfo:\s+PgState: (.*?), PgCount: (\d+)",
            output.stderr.decode(),
        )
        assert pg_status, "Placement group status not found in output"
        pg_state, pg_count = pg_status.groups()
        assert (
            pg_state == "active+clean"
        ), f"Expected PG state to be 'active+clean', found '{pg_state}'"
        assert int(pg_count) > 0, f"Expected positive PG count, found {pg_count}"

    def validate_mgr_pods(self, output):
        mgr_pods = [
            line
            for line in output.stdout.decode().split("\n")
            if "rook-ceph-mgr-" in line
        ]

        assert mgr_pods, "No MGR pods found in output"

        for pod in mgr_pods:
            assert "Running" in pod, f"MGR pod not in Running state: {pod}"

        nodes = set(pod.split()[-1] for pod in mgr_pods)

        log.info(f"Found {len(mgr_pods)} running MGR pods on {len(nodes)} nodes")

        mgr_section = re.search(
            r"Info: Checking if at least one mgr pod is running\n(.*?)$",
            output.stderr.decode(),
            re.DOTALL,
        )
        if mgr_section:
            log.info("MGR pod check found in stderr")
        else:
            log.warning("MGR pod check not found in stderr")
