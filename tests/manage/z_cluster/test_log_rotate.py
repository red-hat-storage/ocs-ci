import logging
import time
import pytest
import re

from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.resources.pod import get_mgr_pods
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    bugzilla,
    skipif_ocs_version,
    skipif_external_mode,
    ignore_leftovers,
)

log = logging.getLogger(__name__)


@tier2
@ignore_leftovers
@bugzilla("2116416")
@skipif_external_mode
@skipif_ocs_version("<4.10")
@pytest.mark.polarion_id("OCS-XYZ")
class TestLogsRotate(ManageTest):
    """
    Test Logs Rotate
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            log.info("Delete logCollector from storage cluster yaml file")
            storagecluster_obj = OCP(
                resource_name=constants.DEFAULT_CLUSTERNAME,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                kind=constants.STORAGECLUSTER,
            )
            params = '[{"op": "remove", "path": "/spec/logCollector"}]'
            storagecluster_obj.patch(params=params, format_type="json")
            params = '{"spec": {"logCollector":{}}}'
            storagecluster_obj.patch(
                params=params,
                format_type="merge",
            )
            log.info(
                "It takes time for storagecluster to update after the edit command"
            )
            time.sleep(30)
            log.info("Verify storagecluster on Ready state")
            verify_storage_cluster()

        request.addfinalizer(finalizer)

    def test_logs_rotate(self):
        """
        Test Process:
        1.Verify the number of MGR logs
        2.Add logCollector to spec section on Storagecluster
        3.Write 500M to ceph-mgr.a.log
        4.Verify new log created
        5.Delete logCollector from Storagecluster
        """
        pod_mgr_obj = get_mgr_pods()[0]
        self.mgr_id = (
            pod_mgr_obj.get("data").get("metadata").get("labels").get("ceph_daemon_id")
        )
        # cmd = f"ls -l /var/log/ceph"
        output_cmd = pod_mgr_obj.exec_cmd_on_pod(command="ls -l /var/log/ceph")
        self.ceph_mgr_count = len(re.findall(f"ceph-mgr.{self.mgr_id}", output_cmd))
        storagecluster_obj = OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            kind=constants.STORAGECLUSTER,
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

        sample = TimeoutSampler(
            timeout=963,
            sleep=40,
            func=self.verify_new_log_created,
            container_name="log-collector",
            command=f"dd if=/dev/urandom of=/var/log/ceph/ceph-mgr.{self.mgr_id}.log bs=1M count=530",
        )
        if not sample.wait_for_func_status(result=True):
            log.error("New log is not created after timeout")
            raise TimeoutExpiredError("New log is not created after timeout")

    def verify_new_log_created(self, container_name, command):
        try:
            mgr_obj = get_mgr_pods()[0]
            mgr_obj.exec_cmd_on_container(
                container_name=container_name, command=command
            )
            output_cmd = mgr_obj.exec_cmd_on_pod(
                command="ls -l  --block-size=M /var/log/ceph"
            )
            if len(re.findall("ceph-mgr", output_cmd)) != self.ceph_mgr_count + 1:
                logging.info(output_cmd)
                return False
            return True
        except Exception as e:
            logging.error(e)
            return False
