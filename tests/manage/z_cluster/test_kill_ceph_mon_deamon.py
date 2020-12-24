import logging
import pytest
import time

from ocs_ci.framework.testlib import ManageTest, tier4a, bugzilla
from ocs_ci.ocs.resources.pod import get_pod_node, get_mon_pods, get_ceph_tools_pod
from ocs_ci.utility.utils import run_cmd

log = logging.getLogger(__name__)


@tier4a
@bugzilla("1904917")
class TestKillCephMonDaemon(ManageTest):
    """
    Verify coredump getting generated for ceph mon daemon crash

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Archive a crash report so that it is no longer considered for the
        RECENT_CRASH health check and does not appear in the
        crash ls-new output

        """

        def finalizer():
            tool_pod = get_ceph_tools_pod()
            tool_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all", format=None)

        request.addfinalizer(finalizer)

    def test_kill_ceph_mon_process(self):
        """
        Kill ceph mon daemon

        """
        log.info("Get Node name where mon pod running")
        mon_pods = get_mon_pods()
        mon_pod = mon_pods[0]
        node_obj = get_pod_node(mon_pod)
        node_name = node_obj.name
        cmd_gen = "oc debug node/" + node_name + " -- chroot /host "

        log.info("find ceph-mon process-id")
        cmd_ps = "ps -ef"
        cmd = cmd_gen + cmd_ps
        out = run_cmd(cmd=cmd)
        for line in out.split("\n"):
            if ("setuser-match-path" in line) and ("167" in line):
                pid = line.split()[1]

        log.info(f"Kill ceph-mon process-id {pid}")
        cmd_kill = f"kill -11 {pid}"
        cmd = cmd_gen + cmd_kill
        run_cmd(cmd=cmd)

        # Waiting to updated crash list
        time.sleep(600)

        log.info("Verify that we have a crash event for ceph-mon crash")
        tool_pod = get_ceph_tools_pod()
        crash_ls = tool_pod.exec_ceph_cmd(ceph_cmd="ceph crash ls-new", format=None)
        if "mon." not in crash_ls:
            raise Exception("ceph mon process does not killed")

        log.info("Check coredump log ")
        coredump_ls = tool_pod.exec_ceph_cmd(ceph_cmd="coredumpctl list", format=None)
        if "mon." not in coredump_ls:
            raise Exception("coredump not getting generated for ceph mon daemon crash")
