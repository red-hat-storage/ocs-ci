import pytest
import logging
import time
import ocpnetsplit

from ocs_ci.utility.utils import wait_for_ceph_health_not_ok
from ocs_ci.utility.retry import retry
from ocs_ci.framework.pytest_customization.marks import (
    turquoise_squad,
    tier1,
    stretchcluster_required,
)
from ocs_ci.helpers.stretchcluster_helper import (
    recover_workload_pods_post_recovery,
    recover_from_ceph_stuck,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour, CommandFailed

from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.ocs.exceptions import CephHealthException

from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_all_nodes
from ocs_ci.helpers.sanity_helpers import Sanity
from datetime import datetime, timedelta, timezone
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_in_statuses,
    get_ceph_tools_pod,
    get_not_running_pods,
)

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
                ), f"Ignoring Ceph health warnings: {e.args[0]}"
                get_ceph_tools_pod().exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
                logger.info("Archived ceph crash!")

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames="zones, duration",
        argvalues=[
            pytest.param(constants.NETSPLIT_DATA_1_DATA_2, 13),
            pytest.param(constants.NETSPLIT_ARBITER_DATA_1, 13),
            pytest.param(constants.NETSPLIT_ARBITER_DATA_1_AND_ARBITER_DATA_2, 13),
            pytest.param(constants.NETSPLIT_ARBITER_DATA_1_AND_DATA_1_DATA_2, 13),
        ],
        ids=[
            "Data-1-Data-2",
            "Arbiter-Data-1",
            "Arbiter-Data-1-and-Arbiter-Data-2",
            "Arbiter-Data-1-and-Data-1-Data-2",
        ],
    )
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
    ):
        """
        This test will test the netsplit scenarios when active-active CephFS and RBD workloads
        is running.
        Steps:
            1) Run both the logwriter and logreader CephFS and RBD workloads
               CephFS workload uses RWX volume and RBD workload uses RWO volumes
            2) Reset the connection scores for the mons
            3) Induce the network split
            4) Make sure logreader job pods have Completed state.
               Check if there is any write or read pause. Fail only when neccessary.
            5) For bc/ab-bc netsplit cases, it is expected for logreader/logwriter pods to go CLBO
               Make sure the above pods run fine after the nodes are restarted
            6) Delete the old logreader job and create new logreader job to verify the data corruption
            7) Make sure there is no data loss
            8) Validate the connection scores
            7) Do a complete cluster sanity and make sure there is no issue post recovery

        """

        sc_obj = StretchCluster()

        # run cephfs workload for both logwriter and logreader
        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=(duration + 10))
        logger.info("Workloads are running")

        # Generate 5 minutes worth of logs before inducing the netsplit
        logger.info("Generating 2 mins worth of log")
        time.sleep(120)

        # note all the pod names
        sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logwriter_reader_pods(label=constants.LOGREADER_CEPHFS_LABEL)
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
        )

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
        )
        logger.info(f"Netsplit induced at {start_time} for zones {zones}")

        # wait for the ceph to be unhealthy
        wait_for_ceph_health_not_ok()

        # get the nodes which are present in the
        # out of quorum zone
        retry(CommandFailed, tries=5, delay=10)(sc_obj.get_out_of_quorum_nodes)()

        # note the end time (UTC)
        if not sc_obj.check_ceph_accessibility(timeout=(duration * 60)):
            assert recover_from_ceph_stuck(
                sc_obj
            ), "Something went wrong. not expected. please check rook-ceph logs"
        time_now = datetime.now(timezone.utc)
        if time_now < end_time:
            time.sleep((end_time - time_now).total_seconds())

        logger.info(f"Ended netsplit at {end_time}")

        # check if all the read operations are successful during the failure window, check for every minute
        sc_obj.post_failure_checks(start_time, end_time, wait_for_read_completion=False)

        # wait for the logreader workload to finish
        try:
            sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGREADER_CEPHFS_LABEL,
                statuses=["Running", "Completed"],
            )
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
            )
        except UnexpectedBehaviour:

            logger.info("some pods are not running, so trying the work-around")
            pods_not_running = get_not_running_pods(
                namespace=constants.STRETCH_CLUSTER_NAMESPACE
            )
            recover_workload_pods_post_recovery(sc_obj, pods_not_running)

        sc_obj.cephfs_logreader_job.delete()
        logger.info(sc_obj.cephfs_logreader_pods)
        for pod in sc_obj.cephfs_logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        logger.info("All old CephFS logreader pods are deleted")

        # check for any data loss
        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_CEPHFS_LABEL
        ), "[CephFS] Data is lost"
        logger.info("[CephFS] No data loss is seen")
        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_RBD_LABEL
        ), "[RBD] Data is lost"
        logger.info("[RBD] No data loss is seen")

        # check for data corruption
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
