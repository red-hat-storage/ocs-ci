"""
Testing MON disk low threshold
Automating BZ#1964055
"""
import logging
import time
import random
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    E2ETest,
    tier2,
    bugzilla,
    skipif_ocs_version,
    skipif_external_mode,
)
from ocs_ci.ocs import node
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.utility import utils
from ocs_ci.helpers.helpers import run_cmd_verify_cli_output
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)

SLEEP_TIMEOUT = 20
TEMP_FILE = "tmp_f"
DD_BLOCK_SIZE = 16
DD_COUNT = 64


@brown_squad
@tier2
@bugzilla("1964055")
@pytest.mark.polarion_id("OCS-2733")
@skipif_ocs_version("<4.9")
class TestMonDataAvailWarn(E2ETest):
    """
    Testing MON disk low threshold.
    Ceph health enters '' state once mon disk reaches >= 85%

    """

    mon_pod = None
    worker_node = None
    oc_cmd = None
    mon_suffix = None
    workloads_dir = None
    dd_seek_count = 0

    @pytest.fixture()
    def workloads_dir_setup(self, request):
        """
        Setting up the environment for the test

        """
        if config.DEPLOYMENT.get("local_storage"):
            self.worker_node = node.get_worker_nodes()[0]
            self.oc_cmd = OCP(namespace=config.ENV_DATA["cluster_namespace"])
            mon_pod_name = self.oc_cmd.exec_oc_debug_cmd(
                node=self.worker_node,
                cmd_list=["ls /var/lib/rook/ | grep mon"],
            )
            mon_pod_id = mon_pod_name.split("-")[1].replace("\n", "")

            mon_pods_info = pod.get_pods_having_label(
                label=f"ceph_daemon_id={mon_pod_id}",
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            self.mon_pod = pod.get_pod_obj(
                name=mon_pods_info[0]["metadata"]["name"],
                namespace=config.ENV_DATA["cluster_namespace"],
            )
        else:
            self.mon_pod = random.choice(pod.get_mon_pods())
        self.mon_suffix = self.mon_pod.get().get("metadata").get("labels").get("mon")

        self.workloads_dir = f"/var/lib/ceph/mon/ceph-{self.mon_suffix}/workloads"
        log.info(f"Selected mon '{self.mon_pod.name}'")
        self.mon_pod.exec_cmd_on_pod(f"mkdir {self.workloads_dir}")
        self.mon_pod.exec_cmd_on_pod(f"touch {self.workloads_dir}/{TEMP_FILE}")

        def finalizer():
            self.mon_pod.exec_cmd_on_pod(f"rm -rf {self.workloads_dir}")
            time.sleep(SLEEP_TIMEOUT)
            utils.ceph_health_check()

        request.addfinalizer(finalizer)

    def get_used_percentage(self):
        """
        Get used percentage on /var/lib/ceph/mon/ceph-[a/b/c]

        Returns:
            int: Used space percentage

        """
        path = f"/var/lib/ceph/mon/ceph-{self.mon_suffix}"
        if config.DEPLOYMENT.get("local_storage"):
            path = "/etc/hosts"
        cmd = f"df -Th | grep {path}"
        mount_details = self.mon_pod.exec_sh_cmd_on_pod(command=cmd, sh="sh")
        used_percent = mount_details.split()[5].replace("%", "")
        return int(used_percent)

    def exec_dd_cmd(self):
        """
        Append 1G to tmp file using dd command
        """
        of_path = f"/var/lib/ceph/mon/ceph-{self.mon_suffix}/workloads"
        if config.DEPLOYMENT.get("local_storage"):
            of_path = f"/var/lib/rook/mon-{self.mon_suffix}/data/workloads"

        write_cmd = f"dd if=/dev/urandom of={of_path}/{TEMP_FILE} "

        write_cmd += f"bs={DD_BLOCK_SIZE}M count={DD_COUNT} "
        write_cmd += f"seek={self.dd_seek_count * DD_BLOCK_SIZE * DD_COUNT}"

        if config.DEPLOYMENT.get("local_storage"):
            self.oc_cmd.exec_oc_debug_cmd(
                node=self.worker_node,
                cmd_list=[write_cmd],
            )
        else:
            self.mon_pod.exec_sh_cmd_on_pod(command=write_cmd, sh="sh")
        self.dd_seek_count += 1

    @skipif_external_mode
    @pytest.mark.usefixtures(workloads_dir_setup.__name__)
    def test_mon_data_avail_warn(self):
        """
        Test mon disk threshold

        Steps:
          - Write to temp file using dd until reaches >= 85% (1G each)
          - Check ceph health from 80% and above
          - From 85% and above, ceph health status should be
            'HEALTH_WARN' with warning message regarding low space
        """

        used_percent = self.get_used_percentage()
        log.info(f"Used percentage on {self.workloads_dir}: {used_percent}%")

        should_keep_writing = True
        while should_keep_writing:
            self.exec_dd_cmd()
            used_percent = self.get_used_percentage()
            log.info(f"Used percentage on {self.workloads_dir}: {used_percent}%")
            if used_percent >= 80:
                time.sleep(SLEEP_TIMEOUT)
                if used_percent >= 85:
                    time.sleep(SLEEP_TIMEOUT)
                    ceph_status = CephCluster().get_ceph_health()
                    log.info(f"Ceph status is: {ceph_status}")
                    assert run_cmd_verify_cli_output(
                        cmd="ceph health detail",
                        expected_output_lst={"HEALTH_WARN", "low on available space"},
                        cephtool_cmd=True,
                    ), "Ceph status should be HEALTH_WARN containing 'low on available space'"
                    should_keep_writing = False
                else:
                    utils.ceph_health_check()
