import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, tier2
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import TimeoutExpiredError, StorageNotSufficientException
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    magenta_squad,
)
from ocs_ci.ocs.cluster import (
    change_ceph_full_ratio,
    get_percent_used_capacity,
    get_osd_utilization,
    get_ceph_df_detail,
    CephCluster,
)

logger = logging.getLogger(__name__)


@tier2
@pytest.mark.parametrize(
    argnames=["interface_type"],
    argvalues=[
        pytest.param(constants.CEPHBLOCKPOOL),
        pytest.param(constants.CEPHFILESYSTEM),
    ],
)
class TestCloneDeletion(E2ETest):
    """
    Tests to verify clone deletion without error
    after cluster out of full ratio
    """

    @pytest.fixture(autouse=True)
    def setup(self, interface_type, pvc_factory, pod_factory, request):
        """
        create resources for the test

        Args:
        interface_type(str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)
        pvc_factory: A fixture to create new pvc
        pod_factory: A fixture to create new pod

        """

        def teardown():
            # change ceph full ratio to standard value
            change_ceph_full_ratio(85)

        request.addfinalizer(teardown)

        logger.info("Starting the test setup")

        self.num_of_clones = 30

        # Getting the total Storage capacity
        self.ceph_cluster = CephCluster()
        self.ceph_capacity = int(self.ceph_cluster.get_ceph_capacity())
        logger.info(f"ceph_capacity: {self.ceph_capacity}")

        # Getting the free Storage capacity
        self.ceph_free_capacity = int(self.ceph_cluster.get_ceph_free_capacity())
        logger.info(f"ceph_free_capacity: {self.ceph_free_capacity}")

        # Change ceph full ratio
        # change_ceph_full_ratio(30)

        # Available free storage capacity in the test
        self.capacity_to_use = self.ceph_free_capacity
        logger.info(f"capacity_to_use: {self.capacity_to_use}")

        self.need_capacity = int((self.num_of_clones * 1.15))

        if self.capacity_to_use < self.need_capacity:
            err_msg = (
                f"The system has only {self.ceph_capacity} GiB, "
                f"Of which {self.ceph_free_capacity} GiB is free, "
                f"we want to use  {self.capacity_to_use} GiB, "
                f"and we need {self.need_capacity} GiB to run the test"
            )
            logger.error(err_msg)
            raise StorageNotSufficientException(err_msg)

        # Calculating the PVC size in GiB
        self.pvc_size = int(self.capacity_to_use / (self.num_of_clones))
        logger.info(f"pvc size: {self.pvc_size}")

        logger.info(
            f"Total capacity size is : {self.ceph_capacity} GiB, "
            f"Free capacity size is : {self.ceph_free_capacity} GiB, "
            f"With {self.num_of_clones} clones to {self.pvc_size} GiB PVC. "
        )

        self.pvc_obj = pvc_factory(
            interface=interface_type, size=self.pvc_size, status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface_type, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )

        # Calculating the file size as 86% of the PVC size - in MB
        self.filesize = f"{int(self.pvc_size * 1024 * 0.86)}M"
        logger.info(f"filesize: {self.filesize}")

        self.pod_obj.run_io(
            size=self.filesize,
            io_direction="write",
            storage_type="fs",
        )

        self.pod_obj.get_fio_results()
        logger.info(f"IO finished on pod {self.pod_obj.name}")

    def verify_osd_used_capacity_greater_than_expected(self, expected_used_capacity):
        """
        Verify OSD percent used capacity greate than ceph_full_ratio

        Args:
            expected_used_capacity (float): expected used capacity

        Returns:
                bool: True if used_capacity greater than expected_used_capacity, False otherwise

        """
        used_capacity = get_percent_used_capacity()
        logger.info(f"Used Capacity is {used_capacity}%")
        ceph_df_detail = get_ceph_df_detail()
        logger.info(f"ceph df detail: {ceph_df_detail}")
        osds_utilization = get_osd_utilization()
        logger.info(f"osd utilization: {osds_utilization}")
        for osd_id, osd_utilization in osds_utilization.items():
            if osd_utilization > expected_used_capacity:
                logger.info(f"OSD ID:{osd_id}:{osd_utilization} greater than 85%")
                return True
        return False

    @skipif_external_mode
    @magenta_squad
    def test_clone_deletion_after_max_cluster_space_utilization(
        self, interface_type, pvc_clone_factory, threading_lock
    ):
        """
        Steps:
            1. Have a cluster with OCP + ODF internal mode
            2. Create cephfs + RBD pvcs with data
            3. Create clones of pvcs till cluster full ratio.
            4. Make the cluster out of full by increasing the full ratio.
            5. After the cluster is out of full state and IOs started , Try to delete clones.
            6. Clone deletion should be successful and should not give error messages.
        """
        # Creating the clones one by one and wait until they bound
        self.timeout = 1800
        logger.info(
            f"Start creating {self.num_of_clones} clones on {interface_type} PVC of size {self.pvc_size} GB."
        )
        clones_list = []

        for clone_num in range(self.num_of_clones + 1):
            logger.info(f"Start creation of clone number {clone_num}.")

            cloned_pvc_obj = pvc_clone_factory(
                self.pvc_obj, storageclass=self.pvc_obj.backed_sc, timeout=360
            )

            cloned_pvc_obj.reload()
            clones_list.append(cloned_pvc_obj)
            logger.info(
                f"Clone with name {cloned_pvc_obj.name} for {self.pvc_size} pvc {self.pvc_obj.name} was created."
            )

        logger.info("Verify used capacity bigger than 85%")
        sample = TimeoutSampler(
            timeout=2500,
            sleep=40,
            func=self.verify_osd_used_capacity_greater_than_expected,
            expected_used_capacity=85.0,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error("The after 1800 seconds the used capacity smaller than 85%")
            raise TimeoutExpiredError

        logger.info(
            "Verify 'CephClusterCriticallyFull' ,CephOSDNearFull Alerts are seen "
        )

        expected_alerts = ["CephOSDCriticallyFull", "CephOSDNearFull"]
        prometheus = PrometheusAPI(threading_lock=threading_lock)
        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=prometheus.verify_alerts_via_prometheus,
            expected_alerts=expected_alerts,
            threading_lock=threading_lock,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error(f"The alerts {expected_alerts} do not exist after 1200 sec")
            raise TimeoutExpiredError

        # Make the cluster out of full by increasing the full ratio.
        logger.info("Change Ceph full_ratio from from 85% to 95%")

        change_ceph_full_ratio(95)
        # After the cluster is out of full state and IOs started , Try to delete clones.
        # Delete the clones one by one and wait for deletion
        logger.info(
            f"Start deleting {self.num_of_clones} clones on {interface_type} PVC of size {self.pvc_size} Gi."
        )

        for index, clone in enumerate(clones_list):
            index += 1
            pvc_reclaim_policy = clone.reclaim_policy
            clone.delete()
            logger.info(f"Deletion of the clone name is {clone.name}.")
            clone.ocp.wait_for_delete(clone.name, self.timeout)
            if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                helpers.validate_pv_delete(clone.backed_pv)
