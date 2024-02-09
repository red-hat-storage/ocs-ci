import pytest
import logging

from datetime import datetime, timezone
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    pre_upgrade,
    skipif_aws_i3,
    skipif_bm,
    skipif_external_mode,
    skipif_bmpsi,
    skipif_ibm_power,
    skipif_no_lso,
    skipif_lso,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    brown_squad,
    stretchcluster_required,
    turquoise_squad,
)
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    skipif_ocs_version,
    tier1,
    acceptance,
    cloud_platform_required,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_osd_pods,
    get_ceph_tools_pod,
    wait_for_pods_to_be_in_statuses,
    get_pod_restarts_count,
)
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.cluster import (
    check_ceph_health_after_add_capacity,
    is_flexible_scaling_enabled,
)
from ocs_ci.ocs.resources.storage_cluster import (
    get_device_class,
    osd_encryption_verification,
    verify_storage_device_class,
    verify_device_class_in_osd_tree,
)
from ocs_ci.ocs.ui.helpers_ui import ui_add_capacity_conditions, ui_add_capacity
from ocs_ci.utility.utils import is_cluster_y_version_upgraded
from ocs_ci.utility import version
from ocs_ci.ocs.resources.stretchcluster import StretchCluster

logger = logging.getLogger(__name__)


def add_capacity_test(ui_flag=False):
    """
    Add capacity on non-lso cluster

    Args:
        ui_flag(bool): add capacity via ui [true] or via cli [false]

    """
    osd_size = storage_cluster.get_osd_size()
    existing_osd_pods = get_osd_pods()
    existing_osd_pod_names = [pod.name for pod in existing_osd_pods]
    if ui_add_capacity_conditions() and ui_flag:
        result = ui_add_capacity(osd_size)
    else:
        result = storage_cluster.add_capacity(osd_size)
    osd_pods_post_expansion = get_osd_pods()
    osd_pod_names_post_expansion = [pod.name for pod in osd_pods_post_expansion]
    restarted_osds = list()
    logger.info(
        "Checking if existing OSD pods were restarted (deleted) post add capacity (bug 1931601)"
    )

    for pod in existing_osd_pod_names:
        if pod not in osd_pod_names_post_expansion:
            restarted_osds.append(pod)
    assert (
        len(restarted_osds) == 0
    ), f"The following OSD pods were restarted (deleted) post add capacity: {restarted_osds}"

    pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    if is_flexible_scaling_enabled():
        replica_count = 1
    else:
        replica_count = 3
    pod.wait_for_resource(
        timeout=300,
        condition=constants.STATUS_RUNNING,
        selector="app=rook-ceph-osd",
        resource_count=result * replica_count,
    )

    # Verify status of rook-ceph-osd-prepare pods. Verifies bug 1769061
    # pod.wait_for_resource(
    #     timeout=300,
    #     condition=constants.STATUS_COMPLETED,
    #     selector=constants.OSD_PREPARE_APP_LABEL,
    #     resource_count=result * 3
    # )
    # Commented this lines as a workaround due to bug 1842500

    # Verify OSDs are encrypted.
    if config.ENV_DATA.get("encryption_at_rest"):
        osd_encryption_verification()

    # verify device classes
    ocs_version = version.get_semantic_ocs_version_from_config()
    if ocs_version >= version.VERSION_4_14 and not is_cluster_y_version_upgraded():
        device_class = get_device_class()
        ct_pod = get_ceph_tools_pod()
        verify_storage_device_class(device_class)
        verify_device_class_in_osd_tree(ct_pod, device_class)

    check_ceph_health_after_add_capacity(ceph_rebalance_timeout=3600)


@brown_squad
@ignore_leftovers
@polarion_id("OCS-1191")
@pytest.mark.second_to_last
@skipif_managed_service
@skipif_aws_i3
@skipif_bm
@skipif_bmpsi
@skipif_lso
@skipif_external_mode
@skipif_ibm_power
@skipif_managed_service
@skipif_hci_provider_and_client
class TestAddCapacity(ManageTest):
    """
    Automates adding variable capacity to the cluster
    """

    @acceptance
    def test_add_capacity_cli(self, reduce_and_resume_cluster_load):
        """
        Add capacity on non-lso cluster via cli on Acceptance suite
        """
        add_capacity_test(ui_flag=False)

    @tier1
    def test_add_capacity_ui(self, reduce_and_resume_cluster_load):
        """
        Add capacity on non-lso cluster via UI on tier1 suite
        """
        add_capacity_test(ui_flag=True)


@brown_squad
@ignore_leftovers
@polarion_id("OCS-4647")
@pytest.mark.second_to_last
@skipif_aws_i3
@skipif_bm
@skipif_bmpsi
@skipif_external_mode
@skipif_ibm_power
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_no_lso
class TestAddCapacityLSO(ManageTest):
    """
    Add capacity on lso cluster
    """

    @acceptance
    def test_add_capacity_lso_cli(self, reduce_and_resume_cluster_load):
        """
        Add capacity on lso cluster via CLI on Acceptance suite
        """
        storage_cluster.add_capacity_lso(ui_flag=False)

    @tier1
    def test_add_capacity_lso_ui(self, reduce_and_resume_cluster_load):
        """
        Add capacity on lso cluster via UI on tier1 suite
        """
        storage_cluster.add_capacity_lso(ui_flag=True)


@brown_squad
@skipif_ocs_version("<4.4")
@pre_upgrade
@ignore_leftovers
@polarion_id("OCS-1191")
@skipif_aws_i3
@skipif_bm
@skipif_external_mode
@cloud_platform_required
@skipif_managed_service
@skipif_hci_provider_and_client
class TestAddCapacityPreUpgrade(ManageTest):
    """
    Automates adding variable capacity to the cluster pre upgrade
    """

    def test_add_capacity_pre_upgrade(self, reduce_and_resume_cluster_load):
        """
        Test to add variable capacity to the OSD cluster while IOs running
        """
        add_capacity_test()


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
