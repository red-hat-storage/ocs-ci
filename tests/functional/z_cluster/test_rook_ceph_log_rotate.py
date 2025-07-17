import logging
import time
import pytest
import re

from ocs_ci.ocs.constants import MANAGED_SERVICE_PLATFORMS
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.cluster import ceph_health_check
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_ocs_version,
    skipif_external_mode,
    ignore_leftovers,
    config,
    runs_on_provider,
)

log = logging.getLogger(__name__)


@runs_on_provider
@brown_squad
@tier2
@ignore_leftovers
@skipif_external_mode
@skipif_ocs_version("<4.10")
@pytest.mark.polarion_id("OCS-4684")
class TestRookCephLogRotate(ManageTest):
    """
    Test Rook Ceph Log Rotate

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            log.info("Delete logCollector from storage cluster yaml file")
            storagecluster_obj = OCP(
                resource_name=constants.DEFAULT_CLUSTERNAME,
                namespace=config.ENV_DATA["cluster_namespace"],
                kind=constants.STORAGECLUSTER,
            )

            # Check if logCollector exists before trying to remove it
            sc_data = storagecluster_obj.get()
            if "spec" in sc_data and "logCollector" in sc_data.get("spec", {}):
                log.info("LogCollector found in storagecluster spec, removing it")
                params = '[{"op": "remove", "path": "/spec/logCollector"}]'
                storagecluster_obj.patch(params=params, format_type="json")
                log.info(
                    "It takes time for storagecluster to update after the edit command"
                )
                time.sleep(30)
                log.info("Verify storagecluster on Ready state")
                verify_storage_cluster()
                ceph_health_check()
            else:
                log.info(
                    "LogCollector not found in storagecluster spec, skipping removal"
                )

        request.addfinalizer(finalizer)

    def get_pod_obj_based_on_id(self, pod_type):
        """
        Get Pod Obj based on id.

        Args:
            pod_type (str): The type of pod [osd/mon/mgr/rgw/mds]

        Returns:
            POD Obj: pod obj based pod_type
        """
        pod_objs = self.podtype_id[pod_type][0]()
        for pod_obj in pod_objs:
            if self.podtype_id[pod_type][1] == pod.get_ceph_daemon_id(pod_obj):
                return pod_obj

    def verify_new_log_created(self, pod_type):
        """
        Verify new log created on /var/log/ceph

        Args:
            pod_type (str): The type of pod [osd/mon/mgr/rgw/mds]

        Returns:
            bool: True if a new log created, otherwise False

        """
        pod_obj = self.get_pod_obj_based_on_id(pod_type)
        output_cmd = pod_obj.exec_cmd_on_pod(command="ls -lh /var/log/ceph")
        expected_string = (
            self.podtype_id[pod_type][2]
            if pod_type == "rgw"
            else f"{self.podtype_id[pod_type][2]}{self.podtype_id[pod_type][1]}"
        )
        cnt_logs = len(re.findall(expected_string, output_cmd))
        if cnt_logs != int(self.podtype_id[pod_type][3]) + 1:
            log.info(output_cmd)
            log.error(
                f"pod_type:{pod_type} cnt_logs_before_fill_log:"
                f"{self.podtype_id[pod_type][3]} cnt_logs_after_fill_log:{cnt_logs}"
            )
            # Write additional data in chunks if verification failed
            log.info(
                f"Writing additional 560MB data to {expected_string}.log in chunks"
            )
            chunk_size = 50  # 50MB chunks
            total_size = 560

            for offset in range(0, total_size, chunk_size):
                current_chunk = min(chunk_size, total_size - offset)
                log_file = f"/var/log/ceph/{expected_string}.log"
                cmd = (
                    f"dd if=/dev/urandom of={log_file} bs=1M "
                    f"count={current_chunk} seek={offset} conv=notrunc"
                )
                pod_obj.exec_cmd_on_pod(
                    command=cmd,
                    out_yaml_format=False,
                    container_name="log-collector",
                )
            return False
        return True

    def test_rook_ceph_log_rotate(self):
        """
        Test Process:
            1.Verify the number of MGR,MDS,OSD,MON,RGW logs
            2.Add logCollector to spec section on Storagecluster
            3.Write 500M to MGR,MDS,OSD,MON,RGW
            4.Verify new log created
            5.Delete logCollector from Storagecluster

        """
        self.podtype_id = dict()
        self.podtype_id["mgr"] = [
            pod.get_mgr_pods,
            pod.get_ceph_daemon_id(pod.get_mgr_pods()[0]),
            "ceph-mgr.",
        ]
        self.podtype_id["osd"] = [
            pod.get_osd_pods,
            pod.get_ceph_daemon_id(pod.get_osd_pods()[0]),
            "ceph-osd.",
        ]
        self.podtype_id["mon"] = [
            pod.get_mon_pods,
            pod.get_ceph_daemon_id(pod.get_mon_pods()[0]),
            "ceph-mon.",
        ]
        if config.ENV_DATA["platform"].lower() in (
            *constants.CLOUD_PLATFORMS,
            *MANAGED_SERVICE_PLATFORMS,
        ):
            # Check if RGW pods exist before adding them to the test
            rgw_pods = pod.get_rgw_pods()
            if rgw_pods:
                log.info("RGW pods found, including them in log rotation test")
                self.podtype_id["rgw"] = [
                    pod.get_rgw_pods,
                    pod.get_ceph_daemon_id(rgw_pods[0]),
                    "ceph-client.rgw.ocs.storagecluster.cephobjectstore.a",
                ]
            else:
                log.info("No RGW pods found, skipping RGW log rotation test")
        self.podtype_id["mds"] = [
            pod.get_mds_pods,
            pod.get_ceph_daemon_id(pod.get_mds_pods()[0]),
            "ceph-mds.",
        ]

        for pod_type in self.podtype_id:
            pod_obj = self.get_pod_obj_based_on_id(pod_type)
            output_cmd = pod_obj.exec_cmd_on_pod(command="ls -l /var/log/ceph")
            expected_string = (
                self.podtype_id[pod_type][2]
                if pod_type == "rgw"
                else f"{self.podtype_id[pod_type][2]}{self.podtype_id[pod_type][1]}"
            )
            self.podtype_id[pod_type].append(
                len(re.findall(expected_string, output_cmd))
            )
        storagecluster_obj = OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.STORAGECLUSTER,
        )
        log.info(
            "Add logCollector section on storagecluster, maxLogSize=500M periodicity=hourly"
        )
        params = '{"spec": {"logCollector": {"enabled": true,"maxLogSize":"500M", "periodicity": "hourly"}}}'
        storagecluster_obj.patch(
            params=params,
            format_type="merge",
        )
        log.info("It takes time for storagecluster to update after the edit command")
        time.sleep(30)
        log.info("Verify storagecluster on Ready state")
        verify_storage_cluster()

        log.info("Copy data to /var/log/ceph/<ceph>.log file")
        for pod_type in self.podtype_id:
            pod_obj = self.get_pod_obj_based_on_id(pod_type)
            expected_string = (
                self.podtype_id[pod_type][2]
                if pod_type == "rgw"
                else f"{self.podtype_id[pod_type][2]}{self.podtype_id[pod_type][1]}"
            )

            # Check disk space before writing
            log.info(f"Checking disk space for {pod_type} pod")
            df_output = pod_obj.exec_cmd_on_pod(
                command="df -h /var/log/ceph", container_name="log-collector"
            )
            log.info(f"Disk space before write for {pod_type}: {df_output}")

            # Write data in chunks to avoid memory issues
            log.info(f"Writing 530MB data to {expected_string}.log in chunks")
            chunk_size = 50  # 50MB chunks
            total_size = 530

            for offset in range(0, total_size, chunk_size):
                current_chunk = min(chunk_size, total_size - offset)
                chunk_num = offset // chunk_size + 1
                total_chunks = (total_size + chunk_size - 1) // chunk_size
                log.info(
                    f"Writing chunk {chunk_num}/{total_chunks}: "
                    f"{current_chunk}MB at offset {offset}MB"
                )

                # Use conv=notrunc to not truncate the file, seek to append at the right position
                log_file = f"/var/log/ceph/{expected_string}.log"
                cmd = (
                    f"dd if=/dev/urandom of={log_file} bs=1M "
                    f"count={current_chunk} seek={offset} conv=notrunc"
                )
                pod_obj.exec_cmd_on_pod(
                    command=cmd,
                    out_yaml_format=False,
                    container_name="log-collector",
                )

        for pod_type in self.podtype_id:
            sample = TimeoutSampler(
                timeout=1800,
                sleep=40,
                func=self.verify_new_log_created,
                pod_type=pod_type,
            )
            if not sample.wait_for_func_status(result=True):
                error_log = f"New {pod_type} log is not created after timeout."
                log.error(error_log)
                raise TimeoutExpiredError(error_log)
