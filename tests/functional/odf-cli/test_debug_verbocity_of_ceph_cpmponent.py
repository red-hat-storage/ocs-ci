import pytest
import logging

from ocs_ci.helpers.helpers import run_odf_cli
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod


log = logging.getLogger(__name__)


@green_squad
class TestDebugVerbocityOfCephComponents:
    @pytest.mark.parametrize(
        argnames=["service", "log_level"],
        argvalues=[("osd", 10), ("mds", 10)],
        # argvalues=[("mon", 10), ("osd", 10), ("mds", 10)],
    )
    def test_debug_verbocity_of_ceph_conponents(self, service, log_level):
        """_summary_"""
        odf_cmd = f"odf set ceph log-level {service} {log_level}"

        assert run_odf_cli(cmd=odf_cmd)

        ceph_cmd = f"ceph config get {service}"

        toolbox = get_ceph_tools_pod()
        ceph_output = toolbox.exec_ceph_cmd(ceph_cmd)

        ceph_log_level = ceph_output.get("debug_mon", {}).get("value", None)
        assert ceph_log_level, f"No Debug value found in the {ceph_cmd} output."

        memory_value, log_value = ceph_log_level.split("/")

        assert int(log_value) == int(
            log_level
        ), f"Actual value set by odf-cli is {log_level} is not" \
            "matching with the value reported by {ceph_cmd} : {log_value}"
