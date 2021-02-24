import logging
import pytest

from ocs_ci.framework.testlib import ManageTest, tier1, bugzilla
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.helpers import verify_cli_cmd_output
from ocs_ci.ocs.resources.pod import (
    get_pod_node,
    get_mon_pods,
    get_ceph_tools_pod,
    get_mgr_pods,
    get_osd_pods,
)

log = logging.getLogger(__name__)


@tier1
@bugzilla("1904917")
class TestKillCephDaemon(ManageTest):
    """
    Verify coredump getting generated for ceph daemon crash

    """

    @pytest.fixture(scope="function", autouse=True)
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

    @pytest.mark.parametrize(
        argnames=["daemon_type"],
        argvalues=[
            pytest.param(*["mon"], marks=pytest.mark.polarion_id("OCS-2491")),
            pytest.param(*["osd"], marks=pytest.mark.polarion_id("OCS-2492")),
            pytest.param(*["mgr"], marks=pytest.mark.polarion_id("OCS-2493")),
        ],
    )
    def test_coredump_check_for_ceph_daemon_crash(self, daemon_type):
        """
        Verify coredumpctl list updated after killing daemon

        """
        log.info(f"Get Node name where {daemon_type} pod running")
        if daemon_type == "mon":
            mon_pod_nodes = [get_pod_node(pod) for pod in get_mon_pods()]
            node_obj = mon_pod_nodes[0]
        elif daemon_type == "mgr":
            mgr_pod_nodes = [get_pod_node(pod) for pod in get_mgr_pods()]
            node_obj = mgr_pod_nodes[0]
        elif daemon_type == "osd":
            osd_pod_nodes = [get_pod_node(pod) for pod in get_osd_pods()]
            node_obj = osd_pod_nodes[0]

        node_name = node_obj.name
        cmd_gen = "oc debug node/" + node_name + " -- chroot /host "

        log.info(
            "Delete 'posted' directory /var/lib/rook/openshift-storage/crash/posted/"
        )
        cmd_bash = f"oc debug nodes/{node_name} -- chroot /host /bin/bash -c "
        cmd_delete_files = '"rm -rf /var/lib/rook/openshift-storage/crash/posted/*"'
        cmd = cmd_bash + cmd_delete_files
        run_cmd(cmd=cmd)

        log.info(f"find ceph-{daemon_type} process-id")
        cmd_pid = f"pidof ceph-{daemon_type}"
        cmd = cmd_gen + cmd_pid
        out = run_cmd(cmd=cmd)
        pid = out.strip()
        if not pid.isnumeric():
            raise Exception(f"The ceph-{daemon_type} process-id was not found.")

        log.info(f"Kill ceph-{daemon_type} process-id {pid}")
        cmd_kill = f"kill -11 {pid}"
        cmd = cmd_gen + cmd_kill
        run_cmd(cmd=cmd)

        log.info(f"Verify that we have a crash event for ceph-{daemon_type} crash")
        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=verify_cli_cmd_output,
            cmd="ceph crash ls",
            expected_output_lst=[daemon_type],
            cephtool_cmd=True,
        )
        if not sample.wait_for_func_status(True):
            raise Exception(
                f"ceph-{daemon_type} process does not exist on crash list (tool pod)"
            )

        log.info(
            f"Verify coredumpctl list updated after killing ceph-{daemon_type} daemon"
        )
        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=verify_cli_cmd_output,
            cmd="coredumpctl list",
            expected_output_lst=[daemon_type],
            debug_node=node_name,
        )
        if not sample.wait_for_func_status(True):
            raise Exception(
                f"coredump not getting generated for ceph-{daemon_type} daemon crash"
            )

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=verify_cli_cmd_output,
            cmd="ls -ltr /var/lib/rook/openshift-storage/crash/posted/",
            expected_output_lst=[":"],
            debug_node=node_name,
        )
        if not sample.wait_for_func_status(True):
            raise Exception(
                f"coredump not getting generated for ceph-{daemon_type} daemon crash"
            )
