import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.disruption_helpers import Disruptions
from ocs_ci.helpers.helpers import run_cmd_verify_cli_output
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.framework.pytest_customization.marks import skipif_rhel_os, brown_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    bugzilla,
    skipif_ocs_version,
    skipif_external_mode,
    runs_on_provider,
)
from ocs_ci.ocs.resources.pod import (
    get_pod_node,
    get_mon_pods,
    get_ceph_tools_pod,
    get_mgr_pods,
    get_osd_pods,
)

log = logging.getLogger(__name__)


@runs_on_provider
@brown_squad
@tier2
@skipif_external_mode
@skipif_ocs_version("<4.7")
@bugzilla("1904917")
@pytest.mark.polarion_id("OCS-2491")
@pytest.mark.polarion_id("OCS-2492")
@pytest.mark.polarion_id("OCS-2493")
@skipif_rhel_os
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
            log.info("Silence the ceph warnings by “archiving” the crash")
            tool_pod = get_ceph_tools_pod()
            tool_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all", format=None)
            log.info(
                "Perform Ceph and cluster health checks after silencing the ceph warnings"
            )
            ceph_health_check()

        request.addfinalizer(finalizer)

    def test_coredump_check_for_ceph_daemon_crash(self):
        """
        Verify coredumpctl list updated after killing daemon

        """
        log.info("Get Node name where mon pod running")
        mon_pod_nodes = [get_pod_node(pod) for pod in get_mon_pods()]
        mon_pod_node_names = [node.name for node in mon_pod_nodes]

        log.info("Get Node name where mgr pod running")
        mgr_pod_nodes = [get_pod_node(pod) for pod in get_mgr_pods()]
        mgr_pod_node_names = [node.name for node in mgr_pod_nodes]

        log.info("Get Node name where osd pod running")
        osd_pod_nodes = [get_pod_node(pod) for pod in get_osd_pods()]
        osd_pod_node_names = [node.name for node in osd_pod_nodes]

        node_mgr_mon_osd_names = set(mgr_pod_node_names).intersection(
            osd_pod_node_names, mon_pod_node_names
        )
        node_mgr_osd_names = set(mgr_pod_node_names).intersection(osd_pod_node_names)
        node_mgr_mon_names = set(mgr_pod_node_names).intersection(mon_pod_node_names)

        if len(node_mgr_mon_osd_names) > 0:
            daemon_types = ["mgr", "osd", "mon"]
            node_name = list(node_mgr_mon_osd_names)[0]
        elif len(node_mgr_osd_names) > 0:
            daemon_types = ["mgr", "osd"]
            node_name = list(node_mgr_osd_names)[0]
        elif len(node_mgr_mon_names) > 0:
            daemon_types = ["mgr", "mon"]
            node_name = list(node_mgr_mon_names)[0]
        else:
            daemon_types = ["mgr"]
            node_name = mgr_pod_node_names[0]
        log.info(f"Test the daemon_types {daemon_types} on node {node_name}")

        log.info(
            "Delete the contents of 'posted' directory "
            "`/var/lib/rook/openshift-storage/crash/posted/`"
        )
        cmd_bash = (
            f"oc debug nodes/{node_name} --to-namespace={config.ENV_DATA['cluster_namespace']} "
            "-- chroot /host /bin/bash -c "
        )
        cmd_delete_files = '"rm -rf /var/lib/rook/openshift-storage/crash/posted/*"'
        cmd = cmd_bash + cmd_delete_files
        run_cmd(cmd=cmd)

        for daemon_type in daemon_types:
            log.info(f"find ceph-{daemon_type} process-id")
            cmd_pid = f"pidof ceph-{daemon_type}"
            cmd_gen = (
                "oc debug node/"
                + node_name
                + f" --to-namespace={config.ENV_DATA['cluster_namespace']} -- chroot /host "
            )
            cmd = cmd_gen + cmd_pid
            out = run_cmd(cmd=cmd)
            pids = out.strip().split()
            pid = pids[0]
            if not pid.isnumeric():
                raise Exception(f"The ceph-{daemon_type} process-id was not found.")

            log.info(f"Kill ceph-{daemon_type} process-id {pid}")
            disruptions_obj = Disruptions()
            disruptions_obj.daemon_pid = pid
            disruptions_obj.kill_daemon(
                node_name=node_name, check_new_pid=False, kill_signal="11"
            )

        log.info(
            f"Verify that we have a crash event for ceph-{daemon_types} crash (tool pod)"
        )
        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=run_cmd_verify_cli_output,
            cmd="ceph crash ls",
            expected_output_lst=daemon_types,
            cephtool_cmd=True,
        )
        if not sample.wait_for_func_status(True):
            raise Exception(
                f"ceph-{daemon_types} process does not exist on crash list (tool pod)"
            )

        log.info(
            f"Verify coredumpctl list updated after killing {daemon_types} daemons on {node_name}"
        )
        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=run_cmd_verify_cli_output,
            cmd="coredumpctl list",
            expected_output_lst=daemon_types,
            debug_node=node_name,
        )
        if not sample.wait_for_func_status(True):
            raise Exception(
                f"coredump not getting generated for ceph-{daemon_types} daemon crash"
            )

        log.info(f"Verify the directory postedcoredumpctl is not empty on {node_name}")
        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=run_cmd_verify_cli_output,
            cmd="ls -ltr /var/lib/rook/openshift-storage/crash/posted/",
            expected_output_lst=[":"],
            debug_node=node_name,
        )
        if not sample.wait_for_func_status(True):
            raise Exception(
                f"coredump not getting generated for {daemon_types} daemons crash"
            )

        log.info(
            "Verify ceph status moved to HEALTH_WARN state with the relevant "
            "information (daemons have recently crashed)"
        )
        sample = TimeoutSampler(
            timeout=20,
            sleep=5,
            func=run_cmd_verify_cli_output,
            cmd="ceph health detail",
            expected_output_lst=daemon_types
            + ["HEALTH_WARN", "daemons have recently crashed"],
            cephtool_cmd=True,
        )
        if not sample.wait_for_func_status(True):
            raise Exception(
                "The output of command ceph health detail did not show "
                "warning 'daemons have recently crashed'"
            )
