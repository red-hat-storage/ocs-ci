import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_ocs_version,
    skipif_external_mode,
    tier2,
    brown_squad,
    jira,
)
from ocs_ci.helpers.helpers import run_cmd_verify_cli_output
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@tier2
@brown_squad
@skipif_external_mode
@skipif_ocs_version("<4.15 or or >=4.18")
@jira("DFBUGS-1284")
@pytest.mark.polarion_id("OCS-6240")
class TestMgrRookModule(ManageTest):
    """
    Test class for enabling rook module on mgr

    """

    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        """
        Disables rook module on mgr and sets orchestrator backend to original(null) state.
        Archives crash so that if they are raised during the test as part of teardown

        """

        def finalizer():
            toolbox = pod.get_ceph_tools_pod()
            log.info("Setting orch backend to none and disabling rook module on mgr")
            toolbox.exec_ceph_cmd('ceph orch set backend ""')
            toolbox.exec_ceph_cmd("ceph mgr module disable rook")
            mgr_pods = pod.get_mgr_pods()
            for mgr_pod in mgr_pods:
                log.info(
                    f"Restarting mgr pod:{mgr_pod.name} post disabling rook module"
                )
                mgr_pod.delete(wait=True)
            log.info("Validating orch status after disabling rook module")
            try:
                toolbox.exec_ceph_cmd(ceph_cmd="ceph orch status", format=None)
            except CommandFailed as ecf:
                if "No orchestrator configured" in str(ecf):
                    log.info("Backend Mgr rook module has been successfully disabled")
            else:
                raise Exception("Mgr rook module is not disabled during teardown")
            finally:
                log.info("Archive crash if they are raised during the test")
                crash_check = run_cmd_verify_cli_output(
                    cmd="ceph health detail",
                    expected_output_lst={
                        "HEALTH_WARN",
                        "daemons have recently crashed",
                    },
                    cephtool_cmd=True,
                )
                if crash_check:
                    toolbox.exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
                else:
                    log.info("There are no daemon crashes during teardown")

        request.addfinalizer(finalizer)

    def test_mgr_enable_rook_backend_module(self):
        """
        Test to verify there are no crashes post enabling rook module on mgr and
        setting rook as an orchestrator backend.
        BZ: https://bugzilla.redhat.com/show_bug.cgi?id=2274165

        """
        toolbox = pod.get_ceph_tools_pod()
        log.info("Enabling rook module on mgr")
        toolbox.exec_ceph_cmd("ceph mgr module enable rook")
        log.info("Setting orchestrator backend to rook")
        toolbox.exec_ceph_cmd("ceph orch set backend rook")
        log.info("Validating orch status to verify backend: rook is Available")
        sample = TimeoutSampler(
            timeout=120,
            sleep=3,
            func=run_cmd_verify_cli_output,
            cmd="ceph orch status",
            expected_output_lst={"rook", "Yes"},
            cephtool_cmd=True,
        )
        if not sample.wait_for_func_status(True):
            raise Exception("Rook backend module is not enabled")

        mgr_pods = pod.get_mgr_pods()
        for mgr_pod in mgr_pods:
            log.info(f"Restarting mgr pod:{mgr_pod.name} post enabling rook module")
            mgr_pod.delete(wait=True)

        log.info(
            "Verify that there are no crash events for any of the ceph-mgr daemons by waiting upto 600s"
        )
        sample = TimeoutSampler(
            timeout=600,
            sleep=30,
            func=run_cmd_verify_cli_output,
            cmd="ceph health detail",
            expected_output_lst={
                "HEALTH_WARN",
                "daemons have recently crashed",
            },
            cephtool_cmd=True,
        )
        if sample.wait_for_func_status(True):
            cr_ls = toolbox.exec_ceph_cmd("ceph crash ls-new")
            raise Exception(
                f"Daemons other than mgr has crashed: {cr_ls}, failing test and archiving all crashes at teardown."
                "Please check the archived crash report for details"
            )
