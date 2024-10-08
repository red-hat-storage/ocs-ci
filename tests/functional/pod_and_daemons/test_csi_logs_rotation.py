import time
import logging

from ocs_ci.framework.testlib import BaseTest
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.constants import OPENSHIFT_STORAGE_NAMESPACE
from ocs_ci.framework.pytest_customization.marks import brown_squad, tier1
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError

log = logging.getLogger(__name__)
LOGS_DIR_NAME = "/var/lib/rook/openshift-storage.cephfs.csi.ceph.com/log/node-plugin/"
CURRENT_LOG_NAME = "csi-cephfsplugin.log"
WAIT_FOR_ROTATION_TIME = 1200  # seconds
SLEEP_BETWEEN_TRIES = 300  # seconds


@brown_squad
@tier1
class TestPodsCsiLogRotation(BaseTest):
    def logs_were_rotated(self, pod_obj, gz_logs_num, current_log_file_size):
        """
        Gets csi pod log files details

        Args:
            pod_obj (obj): Pod which log files should be investigated
            gz_logs_num (int): Last known number of compressed log files
            current_log_file_size (int) Last known size of the current log file

        Returns:
            bool: True if the log files were rotated
        """
        new_gz_logs_num, new_current_log_file_size = pod_obj.get_logs_details_on_pod(
            LOGS_DIR_NAME
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

    def test_pods_csi_log_rotation(self):
        """
        Tests that the log files on pod are rotated correctly

        """
        log.info("Testing logs rotation on pod.")

        csi_cephfsplugin_pod_objs = pod.get_all_pods(
            namespace=OPENSHIFT_STORAGE_NAMESPACE, selector=["csi-cephfsplugin"]
        )

        # check on the first pod
        pod_obj = csi_cephfsplugin_pod_objs[0]
        gz_logs_num, current_log_file_size = pod_obj.get_logs_details_on_pod(
            LOGS_DIR_NAME
        )
        log.info(
            f"Number of compressed logs = {gz_logs_num}, current log file size = {current_log_file_size}"
        )

        # pump current log file size
        pod_obj.exec_cmd_on_pod(
            command=f"-- truncate -s 560M {LOGS_DIR_NAME + CURRENT_LOG_NAME}",
            container_name="log-collector",
            out_yaml_format=False,
            shell=True,
        )

        time.sleep(10)  # wait fo make sure that the truncate had its effect
        current_log_file_size = pod_obj.get_logs_details_on_pod(LOGS_DIR_NAME)[1]
        log.info(f"Current log file size after truncate is = {current_log_file_size}")

        try:
            for result in TimeoutSampler(
                WAIT_FOR_ROTATION_TIME,
                SLEEP_BETWEEN_TRIES,
                self.logs_were_rotated,
                pod_obj=pod_obj,
                gz_logs_num=gz_logs_num,
                current_log_file_size=current_log_file_size,
            ):
                if result:
                    break
            log.info("The logs were rotated correctly")
        except TimeoutExpiredError:
            assert False, "The logs were not rotated"
