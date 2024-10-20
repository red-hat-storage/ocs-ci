import time
import logging
import pytest

from ocs_ci.framework.testlib import BaseTest
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.constants import OPENSHIFT_STORAGE_NAMESPACE
from ocs_ci.framework.pytest_customization.marks import brown_squad, tier2
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError

log = logging.getLogger(__name__)

WAIT_FOR_ROTATION_TIME = 1200  # seconds
SLEEP_BETWEEN_TRIES = 300  # seconds


@brown_squad
@tier2
class TestPodsCsiLogRotation(BaseTest):
    def check_for_log_rotation_successful(
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
        new_gz_logs_num, new_current_log_file_size = pod_obj.get_logs_details_on_pod(
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
        gz_logs_num, current_log_file_size = pod_obj.get_logs_details_on_pod(
            logs_dir, log_file_name
        )
        log.info(
            f"Number of compressed logs = {gz_logs_num}, current log file size = {current_log_file_size}"
        )

        # pump current log file size
        pod_obj.exec_cmd_on_pod(
            command=f"-- truncate -s 560M {logs_dir + log_file_name}",
            container_name="log-collector",
            out_yaml_format=False,
            shell=True,
        )

        time.sleep(10)  # wait fo make sure that the truncate had its effect
        current_log_file_size = pod_obj.get_logs_details_on_pod(
            logs_dir, log_file_name
        )[1]
        log.info(f"Current log file size after truncate is = {current_log_file_size}")

        try:
            for result in TimeoutSampler(
                WAIT_FOR_ROTATION_TIME,
                SLEEP_BETWEEN_TRIES,
                self.check_for_log_rotation_successful,
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
        argnames=["pod_selector", "logs_dir", "log_file_name"],
        argvalues=[
            pytest.param(
                *[
                    "csi-cephfsplugin",
                    "/var/lib/rook/openshift-storage.cephfs.csi.ceph.com/log/node-plugin/",
                    "csi-cephfsplugin.log",
                ],
            ),
            pytest.param(
                *[
                    "csi-rbdplugin",
                    "/var/lib/rook/openshift-storage.rbd.csi.ceph.com/log/node-plugin/",
                    "csi-rbdplugin.log",
                ],
            ),
        ],
    )
    def test_pods_csi_log_rotation(self, pod_selector, logs_dir, log_file_name):
        """
        Tests that the log files on pod are rotated correctly

        Args:
            pod_selector (str): Pod selector according to the interface
            logs_dir (str): Logs directory on this pod
            log_file_name (str) Current log file name

        """

        csi_interface_plugin_pod_objs = pod.get_all_pods(
            namespace=OPENSHIFT_STORAGE_NAMESPACE, selector=[pod_selector]
        )

        # check on the first pod
        pod_obj = csi_interface_plugin_pod_objs[0]
        self.pump_logs_and_wait_for_rotation(pod_obj, logs_dir, log_file_name)

    @pytest.mark.parametrize(
        argnames=["pod_selector", "logs_dir", "log_file_name"],
        argvalues=[
            pytest.param(
                *[
                    "csi-cephfsplugin-provisioner",
                    "/var/lib/rook/openshift-storage.cephfs.csi.ceph.com/log/controller-plugin/",
                    "csi-cephfsplugin.log",
                ],
            ),
            pytest.param(
                *[
                    "csi-rbdplugin-provisioner",
                    "/var/lib/rook/openshift-storage.rbd.csi.ceph.com/log/controller-plugin/",
                    "csi-rbdplugin.log",
                ],
            ),
        ],
    )
    def test_provisioner_pods_csi_log_rotation(
        self, pod_selector, logs_dir, log_file_name
    ):
        """
        Tests that the both log files on provisioner pod are rotated correctly.

        Args:
            pod_selector (str): Pod selector according to the interface
            logs_dir (str): Logs directory on this pod
            log_file_name (str) Current log file name

        """
        ADDTIONAL_CSI_LOG_FILE = "csi-addons.log"
        csi_interface_plugin_pod_objs = pod.get_all_pods(
            namespace=OPENSHIFT_STORAGE_NAMESPACE, selector=[pod_selector]
        )

        # check on the first pod
        pod_obj = csi_interface_plugin_pod_objs[0]
        self.pump_logs_and_wait_for_rotation(pod_obj, logs_dir, log_file_name)
        self.pump_logs_and_wait_for_rotation(pod_obj, logs_dir, ADDTIONAL_CSI_LOG_FILE)
