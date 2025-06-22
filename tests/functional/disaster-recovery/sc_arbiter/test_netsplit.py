import pytest
import logging
import time
import ocpnetsplit

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    turquoise_squad,
    tier1,
    stretchcluster_required,
)
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import (
    check_for_logwriter_workload_pods,
    recover_from_ceph_stuck,
)

from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.ocs.exceptions import CephHealthException, CommandFailed

from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_all_nodes
from ocs_ci.helpers.sanity_helpers import Sanity
from datetime import datetime, timedelta, timezone
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_in_statuses,
    get_ceph_tools_pod,
)
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


@tier1
@stretchcluster_required
@turquoise_squad
class TestNetSplit:
    @pytest.fixture()
    def init_sanity(self, request):
        """
        Initial Cluster sanity
        """
        self.sanity_helpers = Sanity()

        def finalizer():
            """
            Make sure the ceph is not ERR state at the end of the test
            """
            try:
                logger.info("Making sure ceph health is OK")
                self.sanity_helpers.health_check(tries=50, cluster_check=False)
            except CephHealthException as e:
                assert (
                    "HEALTH_WARN" in e.args[0]
                ), f"Ceph seems to be in HEALTH_ERR state: {e.args[0]}"
                get_ceph_tools_pod().exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
                logger.info("Archived ceph crash!")

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames="zones, duration",
        argvalues=[
            pytest.param(
                constants.NETSPLIT_DATA_1_DATA_2,
                15,
                marks=[
                    pytest.mark.polarion_id("OCS-5069"),
                    pytest.mark.polarion_id("OCS-5071"),
                ],
            ),
            pytest.param(
                constants.NETSPLIT_ARBITER_DATA_1,
                15,
                marks=[
                    pytest.mark.polarion_id("OCS-5072"),
                    pytest.mark.polarion_id("OCS-5074"),
                ],
            ),
            pytest.param(
                constants.NETSPLIT_ARBITER_DATA_1_AND_ARBITER_DATA_2,
                15,
                marks=[
                    pytest.mark.polarion_id("OCS-5083"),
                    pytest.mark.polarion_id("OCS-5085"),
                ],
            ),
            pytest.param(
                constants.NETSPLIT_ARBITER_DATA_1_AND_DATA_1_DATA_2,
                15,
                marks=[
                    pytest.mark.polarion_id("OCS-5077"),
                    pytest.mark.polarion_id("OCS-5079"),
                ],
            ),
        ],
        ids=[
            "Data-1-Data-2",
            "Arbiter-Data-1",
            "Arbiter-Data-1-and-Arbiter-Data-2",
            "Arbiter-Data-1-and-Data-1-Data-2",
        ],
    )
    @pytest.mark.polarion_id("OCS-5850")
    def test_netsplit(
        self,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        nodes,
        zones,
        duration,
        init_sanity,
        reset_conn_score,
        cnv_workload,
        setup_cnv,
    ):
        """
        This test will test the netsplit scenarios (BC, AB, AB-AC, AB-BC) when CephFS, RBD and VM workloads
        are running.
        Steps:
            1) Run both the logwriter and logreader CephFS and RBD workloads
               CephFS workload uses RWX volume and RBD workload uses RWO volumes
            2) Create VM using standalone PVC. Create some data inside the VM instance
            3) Reset the connection scores for the mons
            4) Induce the network split
            5) Check VM data integrity is maintained post netsplit. Check if New IO is possible in VM and out of VM.
            6) Make sure logreader job pods have Completed state.
               Check if there is any write or read pause. Fail only when neccessary.
            7) For bc/ab-bc netsplit cases, it is expected for logreader/logwriter pods to go CLBO
               Make sure the above pods run fine after the nodes are restarted
            8) Delete the old logreader job and create new logreader job to verify the data corruption
            9) Make sure there is no data loss
            10) Validate the connection scores
            11) Do a complete cluster sanity and make sure there is no issue post recovery

        """
        kubeconfig = config.RUN.get("kubeconfig")
        sc_obj = StretchCluster()

        # run cephfs and rbd workload for both logwriter and logreader
        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=(duration + 10))

        sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
            zone_aware=False
        )

        logger.info("Workloads are running")

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

        # note all the pod names
        check_for_logwriter_workload_pods(sc_obj, nodes=nodes)

        # note the file names created and each file start write time
        # note the file names created
        sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

        # note the start time (UTC)
        target_time = datetime.now() + timedelta(minutes=5)
        start_time = target_time.astimezone(timezone.utc)
        end_time = start_time + timedelta(minutes=duration)
        ocpnetsplit.main.schedule_split(
            nodes=get_all_nodes(),
            split_name=zones,
            target_dt=target_time,
            target_length=duration,
            kubeconfig=kubeconfig,
        )
        logger.info(f"Netsplit induced at {start_time} for zones {zones}")

        # check for ceph accessibility and note the end time (UTC)
        timeout = (end_time - datetime.now(timezone.utc)).total_seconds()
        if not sc_obj.check_ceph_accessibility(timeout=int(timeout)):
            assert recover_from_ceph_stuck(
                sc_obj
            ), "Something went wrong. not expected. please check rook-ceph logs"
        time_now = datetime.now(timezone.utc)
        if time_now < end_time:
            time.sleep((end_time - time_now).total_seconds())

        logger.info(f"Ended netsplit at {end_time}")

        # check vm data written before the failure for integrity
        @retry(CommandFailed, tries=10, delay=10, backoff=1)
        def _validate_vm_workload():
            """
            Validate vm workload post recovery

            """
            logger.info("Waiting for VM SSH connectivity!")
            vm_obj.wait_for_ssh_connectivity()
            md5sum_after = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
            assert (
                md5sum_before == md5sum_after
            ), "Data integrity of the file inside VM is not maintained during the failure"
            logger.info(
                "Data integrity of the file inside VM is maintained during the failure"
            )

            # check if new data can be created
            vm_obj.run_ssh_cmd(
                command="< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 > /test/file_2.txt"
            )
            logger.info("Successfully created new data inside VM")

            # check if the data can be copied back to local machine
            vm_obj.scp_from_vm(local_path="/tmp", vm_src_path="/test/file_1.txt")
            logger.info("VM data is successfully copied back to local machine")

            # stop the VM
            vm_obj.stop()
            logger.info("Stoped the VM successfully")

        _validate_vm_workload()

        # get all the running logwriter pods
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_CEPHFS_LABEL, exp_num_replicas=0
        )
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGREADER_CEPHFS_LABEL, exp_num_replicas=0
        )
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=0
        )

        # check if all the read operations are successful during the failure window, check for every minute
        sc_obj.post_failure_checks(start_time, end_time, wait_for_read_completion=False)

        # check for any data loss
        check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_CEPHFS_LABEL
        ), "[CephFS] Data is lost"
        logger.info("[CephFS] No data loss is seen")
        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_RBD_LABEL
        ), "[RBD] Data is lost"
        logger.info("[RBD] No data loss is seen")

        # check for data corruption
        sc_obj.cephfs_logreader_job.delete()
        logger.info(sc_obj.cephfs_logreader_pods)
        for pod in sc_obj.cephfs_logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        logger.info("All old CephFS logreader pods are deleted")
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
        logger.info("Logreader job pods have reached 'Completed' state!")

        assert sc_obj.check_for_data_corruption(
            label=constants.LOGREADER_CEPHFS_LABEL
        ), "Data is corrupted for cephFS workloads"
        logger.info("No data corruption is seen in CephFS workloads")

        assert sc_obj.check_for_data_corruption(
            label=constants.LOGWRITER_RBD_LABEL
        ), "Data is corrupted for RBD workloads"
        logger.info("No data corruption is seen in RBD workloads")
