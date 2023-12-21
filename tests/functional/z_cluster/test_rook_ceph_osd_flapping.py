import logging
import pytest
import time


from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.cluster import ceph_health_check
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    bugzilla,
    skipif_ocs_version,
    skipif_external_mode,
    ignore_leftovers,
)

log = logging.getLogger(__name__)


@brown_squad
@tier2
@ignore_leftovers
@bugzilla("2223959")
@skipif_external_mode
@skipif_ocs_version("<4.14")
@pytest.mark.polarion_id("OCS-XXXX")
class TestRookCephOsdFlapping(ManageTest):
    """
    Test Rook Ceph OSD Flapping

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            self.osd_pod_obj.delete()
            ceph_health_check(tries=40, delay=30)

        request.addfinalizer(finalizer)

    def test_rook_ceph_osd_flapping(self):
        """
        Test Process:

        1.Get osd id
        2.Mark an osd down manually 6 times
        3.Verify osd is down with "csph -s" command ["1 osds down"]
        4.Verify osd log contain "osd_max_markdown_count 5 in last" string
        5.Reset osd pod [oc delete pod]
        6.Verify ceph status is OK
        """
        log.info("Get One OSD ID")
        osd_pod_objs = pod.get_osd_pods()
        self.osd_pod_obj = osd_pod_objs[0]
        osd_pod_id = pod.get_osd_pod_id(self.osd_pod_obj)
        ct_pod = pod.get_ceph_tools_pod()
        log.info(f"Mark an osd {osd_pod_id} down manually")
        for _ in range(6):
            time.sleep(5)
            ct_pod.exec_ceph_cmd(f"ceph osd down osd.{osd_pod_id}")

        log.info(f"Verify osd {osd_pod_id} is down")
        sample = TimeoutSampler(
            timeout=100,
            sleep=5,
            func=self.verify_output_ceph_tool_pod,
            command="ceph health",
            expected_string="1 osds down",
        )
        if not sample.wait_for_func_status(result=True):
            raise ValueError("OSD DEBUG Log does not exist")

        log.info("Verify osd logs")
        expected_string = (
            "The OSD pod will sleep for 24 hours. "
            "Restart the pod manually once the flapping issue is fixed"
        )
        osd_pod_log = pod.get_pod_logs(
            pod_name=self.osd_pod_obj.name, all_containers=True
        )
        assert (
            expected_string in osd_pod_log
        ), f"The expected log {expected_string} is not found in osd log"

    def verify_output_ceph_tool_pod(self, command, expected_string):
        """

        Args:
            command (str): the command we run in ceph tool pod
            expected_string (str): the expected string in the output

        Returns:
            bool: True if the output contain the expected_string otherwise False

        """
        ct_pod = pod.get_ceph_tools_pod()
        output_ceph_command = ct_pod.exec_ceph_cmd(
            ceph_cmd=command, out_yaml_format=False
        )
        return expected_string in output_ceph_command
