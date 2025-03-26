import logging
import pytest


from ocs_ci.framework.pytest_customization.marks import polarion_id
from ocs_ci.framework.testlib import ManageTest, tier4b
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import change_ceph_full_ratio
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


class TestRecoverPvcExpandFailure(ManageTest):
    """
    Test cases to verify recovery from PVC expansion failure

    """

    @pytest.fixture(autouse=True)
    def setup(self, create_pvcs_and_pods):
        """
        Create PVCs and pods
        """
        self.pvc_size = 5
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=self.pvc_size,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            access_modes_cephfs=[constants.ACCESS_MODE_RWO],
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Restore ceph full ratio

        """

        def finalizer():
            change_ceph_full_ratio(85)

        request.addfinalizer(finalizer)

    @tier4b
    @polarion_id("")
    def test_recover_from_pvc_expansion_failure(
        self, benchmark_workload_storageutilization
    ):
        """
        Test case to verify recovery from PVC expansion failure

        """
        target_percentage = 85
        logger.info(
            f"Fill up the cluster to {target_percentage}% of it's storage capacity"
        )
        benchmark_workload_storageutilization(target_percentage)

        pvc_size_expanded = 20
        pvc_size_reduced = 10

        logger.info(f"Expanding PVCs to {pvc_size_expanded} GiB")
        for pvc_obj in self.pvcs:
            logger.info(
                f"Expanding size of PVC {pvc_obj.name} to {pvc_size_expanded}Gi"
            )
            assert not pvc_obj.resize_pvc(
                pvc_size_expanded, True
            ), f"Unexpected: Expansion of PVC '{pvc_obj.name}' completed"
            logger.info(pvc_obj.describe())
        logger.info(f"All PVCs failed to expanded to the size {pvc_size_expanded}Gi")

        for pvc_obj in self.pvcs:
            logger.info(
                f"Reducing the size of expansion failed PVC {pvc_obj.name} to {pvc_size_reduced}Gi"
            )
            assert pvc_obj.resize_pvc(
                pvc_size_reduced, False
            ), f"Failed to reduce the size of the PVC '{pvc_obj.name}'"

        change_ceph_full_ratio(95)
        for pvc_obj in self.pvcs:
            for pvc_data in TimeoutSampler(240, 2, pvc_obj.get):
                capacity = pvc_data.get("status").get("capacity").get("storage")
                if capacity == f"{pvc_size_reduced}Gi":
                    break
                logger.info(
                    f"Capacity of PVC {pvc_obj.name} is not {pvc_size_reduced}Gi as "
                    f"expected, but {capacity}. Retrying."
                )
            logger.info(
                f"Verified that the capacity of PVC {pvc_obj.name} is changed to "
                f"{pvc_size_reduced}Gi."
            )
