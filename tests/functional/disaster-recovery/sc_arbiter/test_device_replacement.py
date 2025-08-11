import logging
from datetime import datetime, timezone

from ocs_ci.framework.pytest_customization.marks import (
    stretchcluster_required,
    turquoise_squad,
    polarion_id,
    tier4a,
    tier4,
    jira,
)
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import (
    verify_data_loss,
    verify_data_corruption,
    verify_vm_workload,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.osd_operations import osd_device_replacement
from ocs_ci.ocs.resources.stretchcluster import StretchCluster

logger = logging.getLogger(__name__)


@tier4a
@tier4
@stretchcluster_required
@turquoise_squad
@jira("DFBUGS-1273")
class TestDeviceReplacementInStretchCluster:

    @polarion_id("OCS-5047")
    def test_device_replacement(
        self,
        nodes,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        cnv_workload,
        setup_cnv,
    ):
        """
        Test device replacement in stretch cluster while logwriter workload
        for both CephFs and RBD is running

        Steps:
            1) Run logwriter/reader workload for both CephFs and RBD volumes
            2) Perform device replacement procedure
            3) Verify no data loss
            4) Verify no data corruption

        """

        sc_obj = StretchCluster()

        # setup logwriter workloads in the background
        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=0)
        sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
            zone_aware=False
        )

        sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logwriter_reader_pods(label=constants.LOGREADER_CEPHFS_LABEL)
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
        )
        logger.info("All the workloads pods are successfully up and running")

        # setup vm and write some data to the VM instance
        vm_obj = cnv_workload(volume_interface=constants.VM_VOLUME_PVC)
        vm_obj.run_ssh_cmd(command="mkdir /test && sudo chmod -R 777 /test")
        vm_obj.run_ssh_cmd(
            command="< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 > /test/file_1.txt && sync"
        )
        md5sum_before = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
        logger.debug(
            f"This is the file_1.txt content:\n{vm_obj.run_ssh_cmd(command='cat /test/file_1.txt')}"
        )

        start_time = datetime.now(timezone.utc)

        sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

        # run device replacement procedure
        logger.info("Running device replacement procedure now")
        osd_device_replacement(nodes)

        # check I/O for any failures
        end_time = datetime.now(timezone.utc)
        sc_obj.post_failure_checks(start_time, end_time, wait_for_read_completion=False)
        logger.info("Successfully verified with post failure checks for the workloads")

        # check vm data written after the failure for integrity
        verify_vm_workload(vm_obj, md5sum_before)

        # stop the VM
        vm_obj.stop()
        logger.info("Stoped the VM successfully")

        sc_obj.cephfs_logreader_job.delete()
        logger.info(sc_obj.cephfs_logreader_pods)
        for pod in sc_obj.cephfs_logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        logger.info("All old CephFS logreader pods are deleted")

        # check for any data loss through logwriter logs
        verify_data_loss(sc_obj)

        # check for data corruption through logreader logs
        verify_data_corruption(sc_obj, logreader_workload_factory)
