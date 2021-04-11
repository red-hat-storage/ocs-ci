import logging
import pytest
import time

from ocs_ci.framework.testlib import ManageTest, tier1, bugzilla, skipif_ocs_version
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_osd_pods, get_osd_pod_id

log = logging.getLogger(__name__)


@tier1
@skipif_ocs_version("<4.7")
@bugzilla("1938049")
@pytest.mark.polarion_id("OCS-2512")
class TestOSDHeapProfile(ManageTest):
    """
    Test osd heap profile created on '/var/log/ceph/'.

    """

    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        """
        Archive a crash report so that it is no longer considered for the
        RECENT_CRASH health check and does not appear in the
        crash ls-new output

        """

        def finalizer():
            logging.info("Silence the ceph warnings by “archiving” the crash")
            tool_pod = get_ceph_tools_pod()
            tool_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all", format=None)
            logging.info(
                "Perform Ceph and cluster health checks after silencing the ceph warnings"
            )
            ceph_health_check()

        request.addfinalizer(finalizer)

    def test_osd_heap_profile(self):
        """
        Test osd heap profile created on '/var/log/ceph/'.

        """
        log.info("Start heap profiler for osd-0")
        pod_tool = get_ceph_tools_pod()
        pod_tool.exec_sh_cmd_on_pod(command="ceph tell osd.0 heap start_profiler")

        time.sleep(10)

        log.info("Dump heap profile")
        pod_tool.exec_sh_cmd_on_pod(command="ceph tell osd.0 heap dump")

        log.info("Get osd-0 pod object")
        osd_pods = get_osd_pods()
        for osd_pod in osd_pods:
            if get_osd_pod_id(osd_pod) == "0":
                osd_pod_0 = osd_pod

        log.info("Verify osd.0.profile log exist on /var/log/ceph/")
        out = osd_pod_0.exec_cmd_on_pod(command="ls -ltr /var/log/ceph/")
        if "osd.0.profile" not in out:
            raise Exception("osd.0.profile log does not exist on /var/log/ceph/")
