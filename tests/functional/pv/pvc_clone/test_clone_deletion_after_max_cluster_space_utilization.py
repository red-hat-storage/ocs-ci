import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    tier2,
    E2ETest,
)
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.cluster import change_ceph_full_ratio, CephCluster
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    magenta_squad,
)

logger = logging.getLogger(__name__)


Interfaces_info = {
    constants.CEPHBLOCKPOOL: {
        "name": "RBD",
        "sc": constants.CEPHBLOCKPOOL_SC,
        "clone_yaml": constants.CSI_RBD_PVC_CLONE_YAML,
        "accessmode": constants.ACCESS_MODE_RWO,
    },
    constants.CEPHFILESYSTEM: {
        "name": "CephFS",
        "sc": constants.CEPHFILESYSTEM_SC,
        "clone_yaml": constants.CSI_CEPHFS_PVC_CLONE_YAML,
        "accessmode": constants.ACCESS_MODE_RWX,
    },
}


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
        # Getting the total Storage capacity
        self.ceph_cluster = CephCluster()
        self.ceph_capacity = int(self.ceph_cluster.get_ceph_capacity())
        logger.info(f"ceph_capacity: {self.ceph_capacity}")

        # Getting the free Storage capacity
        self.ceph_free_capacity = int(self.ceph_cluster.get_ceph_free_capacity())
        logger.info(f"ceph_free_capacity: {self.ceph_free_capacity}")

        # Change ceph full ratio
        change_ceph_full_ratio(10)

        # Use 10% of the free storage capacity in the test
        self.capacity_to_use = int(self.ceph_free_capacity * 0.10)
        logger.info(f"capacity_to_use: {self.capacity_to_use}")

        self.num_of_clones = 10
        # Calculating the PVC size in GiB
        self.pvc_size = int(self.capacity_to_use / (self.num_of_clones + 1))
        logger.info(f"pvc size: {self.pvc_size}")

        self.pvc_obj = pvc_factory(
            interface=interface_type, size=self.pvc_size, status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface_type, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )

        # Calculating the file size as 95% of the PVC size - in MB
        self.filesize = f"{int(self.pvc_size * 1024 * 0.95)}M"

        self.pod_obj.run_io(
            size=self.filesize,
            io_direction="write",
            storage_type="fs",
        )

        self.pod_obj.get_fio_results()
        logger.info(f"IO finished on pod {self.pod_obj.name}")

    def verify_alerts_via_prometheus(self, expected_alerts, threading_lock):
        """
        Verify Alerts on prometheus

        Args:
            expected_alerts (list): list of alert names
            threading_lock (threading.Rlock): Lock object to prevent simultaneous calls to 'oc'

        Returns:
            bool: True if expected_alerts exist, False otherwise

        """
        prometheus = PrometheusAPI(threading_lock=threading_lock)
        logger.info("Logging of all prometheus alerts started")
        alerts_response = prometheus.get(
            "alerts", payload={"silenced": False, "inhibited": False}
        )
        actual_alerts = list()
        for alert in alerts_response.json().get("data").get("alerts"):
            actual_alerts.append(alert.get("labels").get("alertname"))
            print("Actual Alerts:", actual_alerts)
        for expected_alert in expected_alerts:
            if expected_alert not in actual_alerts:
                logger.error(
                    f"{expected_alert} alert does not exist in alerts list."
                    f"The actaul alerts: {actual_alerts}"
                )
                return False
        return True

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
        for i in range(self.num_of_clones):
            index = i + 1
            logger.info(f"Start creation of clone number {index}.")

            cloned_pvc_obj = pvc_clone_factory(
                self.pvc_obj,
                storageclass=self.pvc_obj.backed_sc,
            )

            helpers.wait_for_resource_state(
                cloned_pvc_obj, constants.STATUS_BOUND, timeout=1800
            )
            cloned_pvc_obj.reload()
            clones_list.append(cloned_pvc_obj)
            logger.info(
                f"Clone with name {cloned_pvc_obj.name} for {self.pvc_size} pvc {self.pvc_obj.name} was created."
            )

        logger.info("Verify Alerts are seen 'CephClusterErrorState'")

        expected_alerts = ["CephClusterErrorState"]
        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=self.verify_alerts_via_prometheus,
            expected_alerts=expected_alerts,
            threading_lock=threading_lock,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error(f"The alerts {expected_alerts} do not exist after 1200 sec")
            raise TimeoutExpiredError

        # Make the cluster out of full by increasing the full ratio.
        logger.info("Change Ceph full_ratio from from 20% to 50%")

        change_ceph_full_ratio(50)
        # After the cluster is out of full state and IOs started , Try to delete clones.
        # Delete the clones one by one and wait for deletion
        logger.info(
            f"Start deleting {self.num_of_clones} clones on {interface_type} PVC of size {self.pvc_size} Gi."
        )
        index = 0
        for clone in clones_list:
            index += 1
            pvc_reclaim_policy = clone.reclaim_policy
            clone.delete()
            logger.info(
                f"Deletion of clone number {index} , the clone name is {clone.name}."
            )
            clone.ocp.wait_for_delete(clone.name, self.timeout)
            if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                helpers.validate_pv_delete(clone.backed_pv)
