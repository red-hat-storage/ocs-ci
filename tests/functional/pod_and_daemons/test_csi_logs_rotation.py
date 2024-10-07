import time
import logging

from ocs_ci.framework.testlib import BaseTest
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.constants import OPENSHIFT_STORAGE_NAMESPACE
from ocs_ci.framework.pytest_customization.marks import green_squad, tier1

log = logging.getLogger(__name__)
LOGS_DIR_NAME = "/var/lib/rook/openshift-storage.cephfs.csi.ceph.com/log/node-plugin/"
CURRENT_LOG_NAME = "csi-cephfsplugin.log"
WAIT_FOR_ROTATION_TIME = 1200  # seconds

@green_squad
@tier1
class TestPodsCsiLogRotation(BaseTest):
    def get_logs_details_on_pod(self, pod_obj):
        """
        Gets csi pod log files details

        Args:
            pod_obj (obj): Pod which log files should be investigated

        Returns:
            gz_logs_num (int): Number of compressed log files
            current_log_file_size (int) Size of the current log file
        """

        all_logs = (
            pod_obj.exec_cmd_on_pod(
                command=f"-- ls -l {LOGS_DIR_NAME}",
                container_name="log-collector",
                out_yaml_format=False,
                shell=True,
            )
            .strip()
            .split("\n")
        )
        gz_logs_num = 0
        current_log_file_size = 0

        for log_file in all_logs[1:]:  # ignore 'total' line
            file_details = log_file.split()
            file_name = "".join(file_details[8:])
            if file_name.endswith(".gz"):
                gz_logs_num = gz_logs_num + 1
            if file_name == CURRENT_LOG_NAME:
                current_log_file_size = file_details[4]
        return gz_logs_num, current_log_file_size

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
        gz_logs_num, current_log_file_size = self.get_logs_details_on_pod(pod_obj)
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
        current_log_file_size = self.get_logs_details_on_pod(pod_obj)[1]
        log.info(f"Current log file size after truncate is = {current_log_file_size}")

        time.sleep(WAIT_FOR_ROTATION_TIME)  # sleep for time needed for logs rotation
        new_gz_logs_num, new_current_log_file_size = self.get_logs_details_on_pod(
            pod_obj
        )
        log.info(
            f"New number of gz = {new_gz_logs_num}, new current log file size = {new_current_log_file_size}"
        )

        if gz_logs_num < 7:  # max number of gz logs
            # test that new compressed file was added
            assert new_gz_logs_num == gz_logs_num + 1
        else:
            # the number of compressed file was already at maximum, so test that the old current log was compressed
            # and the new one has smaller size
            assert new_current_log_file_size < current_log_file_size
