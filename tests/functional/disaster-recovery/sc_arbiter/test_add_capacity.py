import pytest
import logging

from datetime import datetime, timezone
from ocs_ci.framework.pytest_customization.marks import (
    turquoise_squad,
    stretchcluster_required,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.resources.pod import (
    get_pod_restarts_count,
    get_ceph_tools_pod,
    wait_for_pods_to_be_in_statuses,
)
from ocs_ci.ocs.resources.stretchcluster import StretchCluster

logger = logging.getLogger(__name__)


@turquoise_squad
@stretchcluster_required
class TestAddCapacityStretchCluster:
    """
    Add capacity to the Stretch cluster with arbiter configuration

    """

    @staticmethod
    def add_capacity_to_stretch_cluster():
        """
        Perform add capacity on a stretch cluster

        """
        # get osd pods restart count before
        osd_pods_restart_count_before = get_pod_restarts_count(
            label=constants.OSD_APP_LABEL
        )

        # add capacity to the cluster
        storage_cluster.add_capacity_lso(ui_flag=False)
        logger.info("Successfully added capacity")

        # get osd pods restart count after
        osd_pods_restart_count_after = get_pod_restarts_count(
            label=constants.OSD_APP_LABEL
        )

        # assert if any osd pods restart
        assert sum(osd_pods_restart_count_before.values()) == sum(
            osd_pods_restart_count_after.values()
        ), "Some of the osd pods have restarted during the add capacity"
        logger.info("osd pod restarts counts are same before and after.")

        # assert if osd weights for both the zones are not balanced
        tools_pod = get_ceph_tools_pod()
        zone1_osd_weight = tools_pod.exec_sh_cmd_on_pod(
            command=f"ceph osd tree | grep 'zone {constants.DATA_ZONE_LABELS[0]}' | awk '{{print $2}}'",
        )
        zone2_osd_weight = tools_pod.exec_sh_cmd_on_pod(
            command=f"ceph osd tree | grep 'zone {constants.DATA_ZONE_LABELS[1]}' | awk '{{print $2}}'",
        )

        assert float(zone1_osd_weight.strip()) == float(
            zone2_osd_weight.strip()
        ), "OSD weights are not balanced"
        logger.info("OSD weights are balanced")

    @pytest.mark.last
    @pytest.mark.parametrize(
        argnames=["iterations"],
        argvalues=[
            pytest.param(
                3,
                marks=[
                    pytest.mark.polarion_id("OCS-5474"),
                    pytest.mark.bugzilla("2143858"),
                ],
            ),
        ],
    )
    def test_cluster_expansion(
        self,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        iterations,
    ):

        """
        Test cluster exapnsion and health when add capacity is performed
        continuously

        """

        sc_obj = StretchCluster()

        # setup logwriter workloads in the background
        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=0)

        sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logwriter_reader_pods(label=constants.LOGREADER_CEPHFS_LABEL)
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
        )
        logger.info("All the workloads pods are successfully up and running")

        start_time = datetime.now(timezone.utc)

        sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

        # add capacity to the cluster
        for iteration in range(iterations):
            logger.info(f"[{iteration+1}] adding capacity to the cluster now...")
            self.add_capacity_to_stretch_cluster()
            logger.info("successfully added capacity to the cluster")

        # check Io for any failures
        end_time = datetime.now(timezone.utc)
        sc_obj.post_failure_checks(start_time, end_time, wait_for_read_completion=False)
        logger.info("Successfully verified with post failure checks for the workloads")

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
        logreader_workload_factory(
            pvc=sc_obj.get_workload_pvc_obj(constants.LOGWRITER_CEPHFS_LABEL)[0],
            logreader_path=constants.LOGWRITER_CEPHFS_READER,
            duration=5,
        )
        sc_obj.get_logwriter_reader_pods(constants.LOGREADER_CEPHFS_LABEL)

        wait_for_pods_to_be_in_statuses(
            expected_statuses=constants.STATUS_COMPLETED,
            pod_names=[pod.name for pod in sc_obj.cephfs_logreader_pods],
            timeout=900,
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )
        logger.info("[CephFS] Logreader job pods have reached 'Completed' state!")

        assert sc_obj.check_for_data_corruption(
            label=constants.LOGREADER_CEPHFS_LABEL
        ), "Data is corrupted for cephFS workloads"
        logger.info("No data corruption is seen in CephFS workloads")

        assert sc_obj.check_for_data_corruption(
            label=constants.LOGWRITER_RBD_LABEL
        ), "Data is corrupted for RBD workloads"
        logger.info("No data corruption is seen in RBD workloads")
