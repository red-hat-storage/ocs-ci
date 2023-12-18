import logging
import pytest


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
            ceph_health_check()

        request.addfinalizer(finalizer)

    def test_rook_ceph_osd_flapping(self):
        """
        Test Process:

        1.Get osd id
        2.Mark an osd down manually
        3.Verify osd is down
        4.Verify osd log contain "osd_max_markdown_count 5 in last" string
        5.Scale up osd
        6.Verify ceph status is OK
        """
        log.info("Get One OSD ID")
        osd_pod_objs = pod.get_osd_pods()
        self.osd_pod_id = pod.get_osd_pod_id(osd_pod_objs[0])
        ct_pod = pod.get_ceph_tools_pod()
        log.info(f"Mark an osd {self.osd_pod_id} down manually")
        ct_pod.exec_ceph_cmd(f"ceph osd down osd.{self.osd_pod_id}")

        log.info(f"Verify osd {self.osd_pod_id} is down")
        sample = TimeoutSampler(
            timeout=10,
            sleep=1,
            func=self.verify_output_ceph_tool_pod,
            command="ceph -s",
            expected_strings="1 osds down",
        )
        if not sample.wait_for_func_status(result=True):
            raise ValueError("OSD DEBUG Log does not exist")

        log.info("Verify osd logs")
        osd_pod_log = pod.get_pod_logs(
            pod_name=osd_pod_objs[0].name, all_containers=True
        )
        assert (
            "osd_max_markdown_count 5 in last" in osd_pod_log
        ), "The expected log 'osd_max_markdown_count 5 in last' is not found in osd log"

    def verify_output_ceph_tool_pod(self, command, expected_strings):
        """

        Args:
            command:
            expected_strings:

        Returns:


        """
        ct_pod = pod.get_ceph_tools_pod()
        output_ceph_command = ct_pod.exec_ceph_cmd(command)
        return expected_strings in output_ceph_command
