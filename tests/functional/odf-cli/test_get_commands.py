import logging
import re
import pytest

from ocs_ci.ocs.resources.pod import get_mon_pods
from ocs_ci.helpers.odf_cli import ODFCLIRetriever, ODFCliRunner
from ocs_ci.framework.testlib import tier1, brown_squad, polarion_id

logger = logging.getLogger(__name__)


@tier1
@brown_squad
class TestGetCommands:
    @pytest.fixture(scope="function", autouse=True)
    def odf_cli_setup(self):
        odf_cli_retriever = ODFCLIRetriever()

        # Check and download ODF CLI binary if needed
        try:
            assert odf_cli_retriever.odf_cli_binary
        except AssertionError:
            logger.warning("ODF CLI binary not found. Attempting to download...")
            odf_cli_retriever.retrieve_odf_cli_binary()
            if not odf_cli_retriever.odf_cli_binary:
                pytest.fail("Failed to download ODF CLI binary")

        # Check and initialize ODFCliRunner if needed
        try:
            self.odf_cli_runner = ODFCliRunner()
            assert self.odf_cli_runner
        except AssertionError:
            logger.warning("ODFCliRunner not initialized. Attempting to initialize...")
            self.odf_cli_runner = ODFCliRunner()
            if not self.odf_cli_runner:
                pytest.fail("Failed to initialize ODFCliRunner")

        logger.info(
            "ODF CLI binary downloaded and ODFCliRunner initialized successfully"
        )

    @polarion_id("OCS-6237")
    def test_get_health(self):
        output = self.odf_cli_runner.run_get_health()
        self.validate_mon_pods(output)
        self.validate_mon_quorum_and_health(output)
        self.validate_osd_pods(output)
        self.validate_running_pods(output)
        self.validate_pg_status(output)
        self.validate_mgr_pods(output)

    @polarion_id("OCS-6238")
    def test_get_mon_endpoint(self):
        output = self.odf_cli_runner.run_get_mon_endpoint()
        assert output, "Mon endpoint not found in output"
        # Validate the format of the mon endpoint output
        endpoint_pattern = r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+,?)+$"
        assert re.match(
            endpoint_pattern, output.strip()
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
        mon_section = re.search(
            r"Info: Checking if at least three mon pods are running on different nodes\n(.*?)\n\n",
            output,
            re.DOTALL,
        )
        assert mon_section, "Mon pods section not found in output"
        mon_pods = mon_section.group(1).split("\n")
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
        health_ok = "Info: HEALTH_OK" in output
        assert health_ok, "Ceph health is not OK"

    def validate_osd_pods(self, output):
        osd_section = re.search(
            r"Info: Checking if at least three osd pods are running on different nodes\n(.*?)\n\n",
            output,
            re.DOTALL,
        )
        assert osd_section, "OSD pods section not found in output"
        osd_pods = osd_section.group(1).split("\n")
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
        running_pods_section = re.search(
            r"Info: Pods that are in 'Running' or `Succeeded` status\n(.*?)\n\nWarning:",
            output,
            re.DOTALL,
        )
        assert running_pods_section, "Running pods section not found in output"
        running_pods = running_pods_section.group(1).split("\n")
        assert len(running_pods) > 0, "No running pods found"
        for pod in running_pods:
            assert (
                "Running" in pod or "Succeeded" in pod
            ), f"Pod not in Running or Succeeded state: {pod}"

    def validate_pg_status(self, output):
        pg_status = re.search(
            r"Info: Checking placement group status\nInfo:\s+PgState: (.*?), PgCount: (\d+)",
            output,
        )
        assert pg_status, "Placement group status not found in output"
        pg_state, pg_count = pg_status.groups()
        assert (
            pg_state == "active+clean"
        ), f"Expected PG state to be 'active+clean', found '{pg_state}'"
        assert int(pg_count) > 0, f"Expected positive PG count, found {pg_count}"

    def validate_mgr_pods(self, output):
        mgr_section = re.search(
            r"Info: Checking if at least one mgr pod is running\n(.*?)$",
            output,
            re.DOTALL,
        )
        assert mgr_section, "MGR pods section not found in output"
        mgr_pods = mgr_section.group(1).split("\n")
        assert len(mgr_pods) >= 1, f"Expected at least 1 MGR pod, found {len(mgr_pods)}"
        for pod in mgr_pods:
            assert "Running" in pod, f"MGR pod not in Running state: {pod}"
