import logging
import time

from ocs_ci.framework.pytest_customization.marks import (
    stretchcluster_required,
    tier1,
    polarion_id,
    turquoise_squad,
)
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import (
    check_for_logwriter_workload_pods,
    verify_vm_workload,
    verify_data_loss,
    verify_data_corruption,
)
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.ocs.node import (
    gracefully_reboot_nodes,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
)
from ocs_ci.utility.retry import retry


log = logging.getLogger(__name__)


@tier1
@turquoise_squad
@stretchcluster_required
class TestGracefulRestart:

    @polarion_id("OCS-5048")
    def test_graceful_restart(
        self,
        node_restart_teardown,
        node_drain_teardown,
        reset_conn_score,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        nodes,
        cnv_workload,
        setup_cnv,
    ):
        """
        Test cluster node graceful restart while the logwriter and VM workloads are Running

        Steps:
        - Deploy the ceph-fs & rbd workloads
        - Deploy VM workload with some data
        - Gracefully reboot the nodes
        - Verify VM workload data integrity
        - Verify ceph-fs & rbd workloads data integrity

        """

        sc_obj = StretchCluster()

        # Run the logwriter cephFs workloads
        log.info("Running logwriter cephFS and RBD workloads")
        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=0)
        sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
            zone_aware=False
        )

        # setup vm and write some data to the VM instance
        vm_obj = cnv_workload(volume_interface=constants.VM_VOLUME_PVC)
        vm_obj.run_ssh_cmd(
            command="dd if=/dev/zero of=/file_1.txt bs=1024 count=102400"
        )
        md5sum_before = cal_md5sum_vm(vm_obj, file_path="/file_1.txt")

        # make sure all the worload pods are running
        check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
        log.info("All logwriter workload pods are running successfully")

        gracefully_reboot_nodes()
        log.info("Gracefully restarted all the cluster nodes")

        log.info("a moment to breathe!")
        time.sleep(60)

        # wait for all storage pods to be running or completed
        retry(CommandFailed, tries=5, delay=10)(wait_for_pods_to_be_running)(
            timeout=600
        )

        # check vm data written before the failure for integrity
        log.info("Waiting for VM SSH connectivity!")
        retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
        retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
            vm_obj, md5sum_before
        )

        # stop the VM
        vm_obj.stop()
        log.info("Stoped the VM successfully")

        # check for any data loss
        check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
        verify_data_loss(sc_obj)

        # check for data corruption
        sc_obj.cephfs_logreader_job.delete()
        log.info(sc_obj.cephfs_logreader_pods)
        for pod in sc_obj.cephfs_logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        log.info("All old CephFS logreader pods are deleted")
        verify_data_corruption(sc_obj, logreader_workload_factory)
