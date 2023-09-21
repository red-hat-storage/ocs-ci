import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    tier2,
)
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.cluster import change_ceph_full_ratio
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import TimeoutExpiredError

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
class TestCloneDeletion(PASTest):
    """
    Tests to verify clone deletion without error
    after cluster out of full ratio
    """

    @pytest.fixture(autouse=True)
    def setup(self, interface_type, pvc_factory, pod_factory):
        """
        create resources for the test

        Args:
        interface_type(str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)
        pvc_factory: A fixture to create new pvc
        pod_factory: A fixture to create new pod

        """

        # Getting the total Storage capacity
        self.ceph_capacity = int(self.ceph_cluster.get_ceph_capacity())
        logger.info(self.ceph_capacity)

        # Getting the free Storage capacity
        self.ceph_free_capacity = int(self.ceph_cluster.get_ceph_free_capacity())
        logger.info(self.ceph_free_capacity)

        # Change ceph full ratio
        change_ceph_full_ratio(20)

        # Use 85% of the free storage capacity in the test
        self.capacity_to_use = int(self.ceph_free_capacity * 0.20)

        self.num_of_clones = 50
        # Calculating the PVC size in GiB
        self.pvc_size = int(self.capacity_to_use / (self.num_of_clones + 2))

        self.pvc_obj = pvc_factory(
            interface=interface_type, size=self.pvc_size, status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface_type, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )
        self.pod_obj.run_io(
            storage_type="fs",
            size=self.pvc_size,
            io_direction="write",
            runtime=60,
        )
        self.pod_obj.get_fio_results()
        logger.info(f"IO finished on pod {self.pod_obj.name}")

    def test_clone_deletion_after_cluster_outof_full(
        self, interface_type, pvc_clone_factory
    ):
        """
        Create a clone from an existing pvc
        """
        # Creating the clones one by one and wait until they bound
        logger.info(
            f"Start creating {self.number_of_clones} clones on {self.interface} PVC of size {self.pvc_size} GB."
        )
        clones_list = []
        for i in range(self.number_of_clones):
            index = i + 1
            logger.info(f"Start creation of clone number {index}.")
            cloned_pvc_obj = self.pvc_obj.create_pvc_clone(
                sc_name=self.pvc_obj.backed_sc,
                parent_pvc=self.pvc_obj.name,
                pvc_name=f"clone-pas-test-{index}",
                clone_yaml=Interfaces_info[interface_type]["clone_yaml"],
                namespace=self.namespace,
                storage_size=self.pvc_size + "Gi",
            )
            helpers.wait_for_resource_state(
                cloned_pvc_obj, constants.STATUS_BOUND, self.timeout
            )

            cloned_pvc_obj.reload()
            clones_list.append(cloned_pvc_obj)
            logger.info(
                f"Clone with name {cloned_pvc_obj.name} for {self.pvc_size} pvc {self.pvc_obj.name} was created."
            )
        return clones_list

        logger.info(
            "Verify Alerts are seen 'CephClusterCriticallyFull' and 'CephOSDNearFull'"
        )
        logger.info("Verify used capacity bigger than 85%")
        expected_alerts = ["CephOSDCriticallyFull", "CephOSDNearFull"]
        sample = TimeoutSampler(
            timeout=600,
            sleep=50,
            func=self.verify_alerts_via_prometheus,
            expected_alerts=expected_alerts,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error(f"The alerts {expected_alerts} do not exist after 600 sec")
            raise TimeoutExpiredError

        # Make the cluster out of full by increasing the full ratio.
        logger.info("Change Ceph full_ratio from from 85% to 88%")

        change_ceph_full_ratio(25)
        # After the cluster is out of full state and IOs started , Try to delete clones.
        # Delete the clones one by one and wait for deletion
        logger.info(
            f"Start deleteing {self.number_of_clones} clones on {self.interface} PVC of size {self.pvc_size} GB."
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

        # change ceph full ratio to standard value
        change_ceph_full_ratio(85)
