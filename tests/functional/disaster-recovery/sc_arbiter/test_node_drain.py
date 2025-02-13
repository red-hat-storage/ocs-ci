import logging
import random
import time

from datetime import datetime, timezone

from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import check_for_logwriter_workload_pods
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import drain_nodes, schedule_nodes
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_in_statuses
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.ocs.resources.stretchcluster import StretchCluster


log = logging.getLogger(__name__)


class TestNodeDrain:

    zones = constants.DATA_ZONE_LABELS

    def test_zone_node_drain(
        self,
        reset_conn_score,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        nodes,
        cnv_workload,
        setup_cnv,
    ):
        """
        Drain the nodes of a zone while the logwriter and cnv workloads
        are running in background

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
        md5sum_after = cal_md5sum_vm(vm_obj, file_path="/file_1.txt")
        assert (
            md5sum_before == md5sum_after
        ), "Data integrity of the file inside VM is not maintained during the failure"
        log.info(
            "Data integrity of the file inside VM is maintained during the failure"
        )

        # check if new data can be created
        vm_obj.run_ssh_cmd(
            command="dd if=/dev/zero of=/file_2.txt bs=1024 count=103600"
        )
        log.info("Successfully created new data inside VM")

        # check if the data can be copied back to local machine
        vm_obj.scp_from_vm(local_path="/tmp", vm_src_path="/file_1.txt")
        log.info("VM data is successfully copied back to local machine")

        # stop the VM
        vm_obj.stop()
        log.info("Stoped the VM successfully")

        # check for any data loss
        check_for_logwriter_workload_pods(sc_obj, nodes=nodes)

        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_CEPHFS_LABEL
        ), "[CephFS] Data is lost"
        log.info("[CephFS] No data loss is seen")
        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_RBD_LABEL
        ), "[RBD] Data is lost"
        log.info("[RBD] No data loss is seen")

        # check for data corruption
        sc_obj.cephfs_logreader_job.delete()
        for pod in sc_obj.cephfs_logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        log.info("All old logreader pods are deleted")
        pvc = get_pvc_objs(
            pvc_names=[
                sc_obj.cephfs_logwriter_dep.get()["spec"]["template"]["spec"][
                    "volumes"
                ][0]["persistentVolumeClaim"]["claimName"]
            ],
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )[0]
        logreader_workload_factory(
            pvc=pvc, logreader_path=constants.LOGWRITER_CEPHFS_READER, duration=5
        )

        sc_obj.get_logwriter_reader_pods(constants.LOGREADER_CEPHFS_LABEL)

        wait_for_pods_to_be_in_statuses(
            expected_statuses=constants.STATUS_COMPLETED,
            pod_names=[pod.name for pod in sc_obj.cephfs_logreader_pods],
            timeout=900,
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )
        log.info("Logreader job pods have reached 'Completed' state!")

        assert sc_obj.check_for_data_corruption(
            label=constants.LOGREADER_CEPHFS_LABEL
        ), "Data is corrupted for cephFS workloads"
        log.info("No data corruption is seen in CephFS workloads")

        assert sc_obj.check_for_data_corruption(
            label=constants.LOGWRITER_RBD_LABEL
        ), "Data is corrupted for RBD workloads"
        log.info("No data corruption is seen in RBD workloads")
