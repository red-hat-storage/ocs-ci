import logging
import random
import time

from datetime import datetime, timezone

from ocs_ci.framework.pytest_customization.marks import (
    stretchcluster_required,
    tier2,
    polarion_id,
    turquoise_squad,
)
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import (
    check_for_logwriter_workload_pods,
    verify_data_loss,
    verify_data_corruption,
    verify_vm_workload,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import drain_nodes, schedule_nodes
from ocs_ci.ocs.resources.stretchcluster import StretchCluster


log = logging.getLogger(__name__)


@tier2
@turquoise_squad
@stretchcluster_required
class TestNodeDrain:

    zones = constants.DATA_ZONE_LABELS

    @polarion_id("OCS-5056")
    def test_zone_node_drain(
        self,
        reset_conn_score,
        node_drain_teardown,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        nodes,
        cnv_workload,
        setup_cnv,
    ):
        """
        Drain the nodes of a data zone while the logwriter and cnv workloads
        are running in background

        Steps:
        - Deploy the ceph-fs & rbd workloads
        - Deploy VM workload with some data
        - Drain the nodes of randomly selected data zone
        - Verify VM workload data integrity
        - Verify ceph-fs & rbd workloads data integrity

        """

        sc_obj = StretchCluster()

        # Run the logwriter cephFs and RBD workloads
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

        # randomly select a zone to drain the nodes from
        zone = random.choice(self.zones)
        nodes_to_drain = sc_obj.get_nodes_in_zone(zone)

        # drain nodes in the selected zone
        start_time = datetime.now(timezone.utc)
        drain_nodes(node_names=[node_obj.name for node_obj in nodes_to_drain])
        time.sleep(300)
        schedule_nodes(node_names=[node_obj.name for node_obj in nodes_to_drain])
        end_time = datetime.now(timezone.utc)

        # verify the io after all the nodes are scheduled
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_CEPHFS_LABEL, exp_num_replicas=0
        )
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGREADER_CEPHFS_LABEL, exp_num_replicas=0
        )
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=0
        )

        sc_obj.post_failure_checks(start_time, end_time, wait_for_read_completion=False)

        # check vm data written before the failure for integrity
        verify_vm_workload(vm_obj, md5sum_before)

        # stop the VM
        vm_obj.stop()
        log.info("Stoped the VM successfully")

        # check for any data loss
        check_for_logwriter_workload_pods(sc_obj, nodes=nodes)

        # check for any data loss through logwriter logs
        verify_data_loss(sc_obj)

        # check for data corruption through logreader logs
        sc_obj.cephfs_logreader_job.delete()
        log.info(sc_obj.cephfs_logreader_pods)
        for pod in sc_obj.cephfs_logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        log.info("All old CephFS logreader pods are deleted")
        verify_data_corruption(sc_obj, logreader_workload_factory)
