import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import polarion_id
from ocs_ci.framework.testlib import ManageTest, tier4b, green_squad, ignore_leftovers
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import change_ceph_full_ratio
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import verify_data_integrity, cal_md5sum
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
            dep_ocp = OCP(
                kind=constants.DEPLOYMENT,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            dep_ocp.exec_oc_cmd(
                "scale deployment ceph-csi-controller-manager --replicas=1"
            )

        request.addfinalizer(finalizer)

    @tier4b
    @green_squad
    @polarion_id("")
    def test_recover_from_pvc_expansion_failure(
        self, benchmark_workload_storageutilization
    ):
        """
        Test case to verify recovery from PVC expansion failure

        """
        # Create files on the pods
        for pod_obj in self.pods:
            pod_obj.run_io(
                storage_type="fs",
                size="4G",
                io_direction="write",
                runtime=60,
                fio_filename=pod_obj.name,
                end_fsync=1,
            )

        target_percentage = 85
        logger.info(
            f"Fill up the cluster to {target_percentage}% of it's storage capacity"
        )
        benchmark_workload_storageutilization(target_percentage)

        pvc_size_expanded = 20
        pvc_size_reduced = 10

        # Find initial md5sum of file from pods
        # Add some wait time for proper sync of data before getting md5sum. This is to avoid false failure
        time.sleep(25)
        for pod_obj in self.pods:
            pod_obj.orig_md5_sum = cal_md5sum(pod_obj=pod_obj, file_name=pod_obj.name)

        logger.info(f"Expanding PVCs to {pvc_size_expanded} GiB")
        for pvc_obj in self.pvcs:
            logger.info(
                f"Expanding size of PVC {pvc_obj.name} to {pvc_size_expanded}Gi"
            )
            assert not pvc_obj.resize_pvc(
                pvc_size_expanded, True, timeout=60
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
                if capacity == f"{pvc_size_expanded}Gi":
                    break
                logger.info(
                    f"Capacity of PVC {pvc_obj.name} is not {pvc_size_expanded}Gi as "
                    f"expected, but {capacity}. Retrying."
                )
            logger.info(
                f"Verified that the capacity of PVC {pvc_obj.name} is changed to "
                f"{pvc_size_expanded}Gi. The capacity has not changed to reduced size {pvc_size_reduced}."
                f"Make sure there is no data corruption by checking md5sum"
            )

        # Verify md5sum
        for pod_obj in self.pods:
            verify_data_integrity(
                pod_obj=pod_obj,
                file_name=pod_obj.name,
                original_md5sum=pod_obj.orig_md5_sum,
            )

    @tier4b
    @ignore_leftovers
    @green_squad
    @polarion_id("")
    def test_recover_from_pending_pvc_expansion(self):
        """
        Test case to verify recovery from pending PVC expansion

        """
        # Create files on the pods
        for pod_obj in self.pods:
            pod_obj.run_io(
                storage_type="fs",
                size="4G",
                io_direction="write",
                runtime=60,
                fio_filename=f"{pod_obj.name}",
                end_fsync=1,
            )

        # Scale down rbd and cephfs provisioner pod. To do this scale down operator deployments first
        logger.info(
            "Scale down operator deployments to avoid reconciling ctrlplugin deployments after scaled down"
        )
        dep_ocp = OCP(
            kind=constants.DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
        )
        deployments = [
            "ceph-csi-controller-manager",
            f"{config.ENV_DATA['cluster_namespace']}.cephfs.csi.ceph.com-ctrlplugin",
            f"{config.ENV_DATA['cluster_namespace']}.rbd.csi.ceph.com-ctrlplugin",
        ]
        for dep in deployments:
            logger.info(f"Scaling deployment {dep} to replica 0")
            dep_ocp.exec_oc_cmd(f"scale deployment {dep} --replicas=0")
            time.sleep(10)

        pvc_size_expanded = 20
        pvc_size_reduced = 10

        # Find initial md5sum of file from pods
        # Add some wait time for proper sync of data before getting md5sum. This is to avoid false failure
        time.sleep(25)
        for pod_obj in self.pods:
            pod_obj.orig_md5_sum = cal_md5sum(pod_obj=pod_obj, file_name=pod_obj.name)

        logger.info(f"Expanding PVCs to {pvc_size_expanded} GiB")
        for pvc_obj in self.pvcs:
            logger.info(
                f"Expanding size of PVC {pvc_obj.name} to {pvc_size_expanded}Gi"
            )
            try:
                assert not pvc_obj.resize_pvc(
                    pvc_size_expanded, True, timeout=60
                ), f"Unexpected: Expansion of PVC '{pvc_obj.name}' completed"
                logger.info(pvc_obj.describe())
            except TimeoutExpiredError:
                logger.info(
                    f"Expected: Expansion of PVC {pvc_obj.name} did not complete"
                )
        logger.info(f"Expected: PVCs did not expand to the size {pvc_size_expanded}Gi")

        for pvc_obj in self.pvcs:
            logger.info(
                f"Reducing the size of expansion failed PVC {pvc_obj.name} to {pvc_size_reduced}Gi"
            )
            assert pvc_obj.resize_pvc(
                pvc_size_reduced, False
            ), f"Failed to reduce the size of the PVC '{pvc_obj.name}'"

        # Scale back the ceph-csi-controller-manager. This will scale up all other deployments that was scaled down
        dep_ocp.exec_oc_cmd("scale deployment ceph-csi-controller-manager --replicas=1")

        # Now PVCs are expected to expand to the reduced size
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

        # Verify md5sum
        for pod_obj in self.pods:
            verify_data_integrity(
                pod_obj=pod_obj,
                file_name=pod_obj.name,
                original_md5sum=pod_obj.orig_md5_sum,
            )
