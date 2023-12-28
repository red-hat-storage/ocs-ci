import logging
import pytest
import time
import random


from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.helpers import run_cmd_verify_cli_output
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
@pytest.mark.polarion_id("OCS-5404")
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
        4.Verify osd log contains "The OSD pod will sleep for 24 hours." string
        5.Reset osd pod [oc delete pod]
        6.Verify ceph status is OK
        """
        log.info("Get One OSD ID")
        osd_pod_objs = pod.get_osd_pods()
        self.osd_pod_obj = osd_pod_objs[random.randint(0, len(osd_pod_objs) - 1)]
        log.info(f"Get osd pod {self.osd_pod_obj.name}")
        osd_pod_id = pod.get_osd_pod_id(self.osd_pod_obj)
        ct_pod = pod.get_ceph_tools_pod()
        log.info(
            f"Mark an osd {osd_pod_id} down manually. Running 'ceph osd down osd.{osd_pod_id}' 6 times"
        )
        for _ in range(6):
            time.sleep(5)
            ct_pod.exec_ceph_cmd(f"ceph osd down osd.{osd_pod_id}")

        log.info(f"Verify osd {osd_pod_id} is down")
        sample = TimeoutSampler(
            timeout=300,
            sleep=5,
            func=run_cmd_verify_cli_output,
            cmd="ceph health",
            cephtool_cmd=True,
            expected_output_lst=["1 osds down"],
        )
        if not sample.wait_for_func_status(result=True):
            raise ValueError(f"OSD {osd_pod_id} is not down after 300 sec")

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
