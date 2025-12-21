import time
import logging
import os
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import BaseTest
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    tier2,
    post_upgrade,
    skipif_ocs_version,
)
from ocs_ci.ocs.resources.pod import get_pods_having_label, Pod
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError

log = logging.getLogger(__name__)

WAIT_FOR_ROTATION_TIME = 1500  # seconds
SLEEP_BETWEEN_TRIES = 300  # seconds


@brown_squad
@post_upgrade
@skipif_ocs_version("<4.17")
@tier2
class TestPodsCsiLogRotation(BaseTest):
    def check_for_successful_log_rotation(
        self, pod_obj, gz_logs_num, current_log_file_size, logs_dir, log_file_name
    ):
        """
        Checks if the logs were rotated successfully

        Args:
            pod_obj (obj): Pod which log files should be investigated
            gz_logs_num (int): Last known number of compressed log files
            current_log_file_size (int) Last known size of the current log file
            logs_dir (str): Logs directory on this pod
            log_file_name (str): Current log file name

        Returns:
            bool: True if the log files were rotated
        """
        new_gz_logs_num, new_current_log_file_size = pod_obj.get_csi_pod_log_details(
            logs_dir, log_file_name
        )
        log.info(
            f"New number of gz = {new_gz_logs_num}, new current log file size = {new_current_log_file_size}"
        )

        if gz_logs_num < 7:  # max number of gz logs
            # test that new compressed file was added
            return new_gz_logs_num == gz_logs_num + 1
        else:
            # the number of compressed file was already at maximum, so test that the old current log was compressed
            # and the new one has smaller size
            return new_current_log_file_size < current_log_file_size

    def pump_logs_and_wait_for_rotation(self, pod_obj, logs_dir, log_file_name):
        """
        Tests that the log files on pod are rotated correctly

        Args:
            pod_obj (str): Pod object which is tested
            logs_dir (str): Logs directory on this pod
            log_file_name (str) Current log file name

        """
        log.info(f"Testing log {log_file_name} rotation on pod {pod_obj.name}")
        gz_logs_num, current_log_file_size = pod_obj.get_csi_pod_log_details(
            logs_dir, log_file_name
        )
        log.info(
            f"Number of compressed logs = {gz_logs_num}, current log file size = {current_log_file_size}"
        )

        # Delete log file first to clear any logrotate state
        pod_obj.exec_cmd_on_pod(
            command=f"rm -f {logs_dir + log_file_name}",
            container_name="log-rotator",
            out_yaml_format=False,
            shell=True,
        )

        # pump current log file size - truncate now creates a NEW file
        pod_obj.exec_cmd_on_pod(
            command=f"truncate -s 560M {logs_dir + log_file_name}",
            container_name="log-rotator",
            out_yaml_format=False,
            shell=True,
        )

        time.sleep(10)  # wait to make sure that the truncate had its effect
        current_log_file_size = pod_obj.get_csi_pod_log_details(
            logs_dir, log_file_name
        )[1]
        log.info(f"Current log file size after truncate is = {current_log_file_size}")

        try:
            for result in TimeoutSampler(
                WAIT_FOR_ROTATION_TIME,
                SLEEP_BETWEEN_TRIES,
                self.check_for_successful_log_rotation,
                pod_obj=pod_obj,
                gz_logs_num=gz_logs_num,
                current_log_file_size=current_log_file_size,
                logs_dir=logs_dir,
                log_file_name=log_file_name,
            ):
                if result:
                    break
            log.info("The logs were rotated correctly")
        except TimeoutExpiredError:
            assert False, "The logs were not rotated"

    @pytest.mark.parametrize(
        argnames=[
            "pod_selector",
            "log_file_name",
            "additional_log_file_name",
        ],
        argvalues=[
            pytest.param(
                *[
                    constants.CSI_CEPHFSPLUGIN_LABEL_419,
                    "csi-cephfsplugin.log",
                    "",
                ],
            ),
            pytest.param(
                *[
                    constants.CSI_RBDPLUGIN_LABEL_419,
                    "csi-rbdplugin.log",
                    "",
                ],
            ),
            pytest.param(
                *[
                    constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL_419,
                    "csi-cephfsplugin.log",
                    "csi-addons.log",
                ],
            ),
            pytest.param(
                *[
                    constants.CSI_RBDPLUGIN_PROVISIONER_LABEL_419,
                    "csi-rbdplugin.log",
                    "csi-addons.log",
                ],
            ),
        ],
    )
    def test_pods_csi_log_rotation(
        self, pod_selector, log_file_name, additional_log_file_name
    ):
        """
        Tests that the both log files on provisioner pod are rotated correctly.

        Args:
            pod_selector (str): Pod selector according to the interface
            log_file_name (str) Current log file name
            additional_log_file_name (str) Additional log file name; empty string if is not relevant

        """
        base_dir = "/csi-logs/"
        suffix_dir = ""
        if pod_selector == "csi-cephfsplugin":
            suffix_dir = "openshift-storage.cephfs.csi.ceph.com/log/node-plugin/"
        elif pod_selector == "csi-rbdplugin":
            suffix_dir = "openshift-storage.rbd.csi.ceph.com/log/node-plugin/"
        elif pod_selector == "csi-cephfsplugin-provisioner":
            suffix_dir = "openshift-storage.cephfs.csi.ceph.com/log/controller-plugin/"
        elif pod_selector == "csi-rbdplugin-provisioner":
            suffix_dir = "openshift-storage.rbd.csi.ceph.com/log/controller-plugin/"

        logs_dir = os.path.join(base_dir, suffix_dir)
        log.debug(f"logs dir: {logs_dir}")

        pod_list = get_pods_having_label(
            namespace=config.ENV_DATA["cluster_namespace"], label=pod_selector
        )
        csi_interface_plugin_pod_objs = [Pod(**pod) for pod in pod_list]

        # check on the first pod
        pod_obj = csi_interface_plugin_pod_objs[0]
        self.pump_logs_and_wait_for_rotation(pod_obj, logs_dir, log_file_name)
        if additional_log_file_name:
            self.pump_logs_and_wait_for_rotation(
                pod_obj, logs_dir, additional_log_file_name
            )
