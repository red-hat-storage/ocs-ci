import logging
import time
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import polarion_id
from ocs_ci.framework.testlib import ManageTest, tier4c, green_squad, ignore_leftovers
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs import constants

from ocs_ci.ocs.exceptions import TimeoutExpiredError, CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import verify_data_integrity, cal_md5sum
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


class TestRecoverPvcExpandFailure(ManageTest):
    """
    Test cases to verify recovery from PVC expansion failure

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, snapshot_restore_factory, create_pvcs_and_pods):
        """
        Create PVCs and pods
        """
        self.pvc_size = 5
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=self.pvc_size,
            pods_for_rwx=1,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            access_modes_cephfs=[constants.ACCESS_MODE_RWO],
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Revert the deployment replica value

        """

        def finalizer():
            dep_ocp = OCP(
                kind=constants.DEPLOYMENT,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            dep_ocp.exec_oc_cmd(
                "scale deployment ceph-csi-controller-manager --replicas=1"
            )

        request.addfinalizer(finalizer)

    @tier4c
    @ignore_leftovers
    @green_squad
    @polarion_id("OCS-6867")
    def test_recover_from_pending_pvc_expansion(
        self, snapshot_factory, snapshot_restore_factory, pod_factory
    ):
        """
        Test case to verify recovery from pending PVC expansion. The PVC will expand to the size given after the initial
        expand request. Test this scenario on PVCs created from snapshots also.

        """
        # Create file on the pods
        for pod_obj in self.pods:
            pod_obj.run_io(
                storage_type="fs",
                size="4G",
                io_direction="write",
                runtime=60,
                fio_filename="test_pvc_expand_recover",
                end_fsync=1,
            )
        for pod_obj in self.pods:
            pod_obj.get_fio_results()

        # Create snapshots
        logger.info("Creating snapshot of all the PVCs")
        snap_objs = []
        for pvc_obj in self.pvcs:
            logger.info(f"Creating snapshot of the PVC {pvc_obj.name}")
            snap_obj = snapshot_factory(pvc_obj, wait=False)
            snap_obj.interface = pvc_obj.interface
            snap_objs.append(snap_obj)
            logger.info(f"Created snapshot of PVC {pvc_obj.name}")

        logger.info("Wait for the snapshots to be in Ready")
        for snap_obj in snap_objs:
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE,
                timeout=360,
            )
            snap_obj.reload()
        logger.info("Snapshots are in Ready state")

        logger.info("Restoring the snapshots to create new PVCs")
        restore_pvcs = []
        for snap_obj in snap_objs:
            restore_obj = snapshot_restore_factory(
                snapshot_obj=snap_obj,
                volume_mode=snap_obj.parent_volume_mode,
                access_mode=snap_obj.parent_access_mode,
                status="",
            )
            logger.info(
                f"Created PVC {restore_obj.name} from snapshot {snap_obj.name}."
            )
            restore_obj.interface = snap_obj.interface
            restore_pvcs.append(restore_obj)
        logger.info("Restored all the snapshots to create new PVCs")

        logger.info("Verifying that the restored PVCs are Bound")
        for pvc_obj in restore_pvcs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=480
            )
            pvc_obj.reload()
        logger.info("Verified that the PVCs created from the snapshots are Bound")

        # Attach the restored PVCs to pods
        logger.info("Attach the restored PVCs to pods")
        restore_pod_objs = []
        for restore_pvc_obj in restore_pvcs:
            restore_pod_obj = pod_factory(
                interface=restore_pvc_obj.interface,
                pvc=restore_pvc_obj,
                status="",
            )
            logger.info(
                f"Attached the PVC {restore_pvc_obj.name} to pod {restore_pod_obj.name}"
            )
            restore_pod_objs.append(restore_pod_obj)

        # Find initial md5sum of file from pods
        all_pods = self.pods + restore_pod_objs
        for pod_obj in all_pods:
            pod_obj.orig_md5_sum = cal_md5sum(
                pod_obj=pod_obj, file_name="test_pvc_expand_recover"
            )

        all_pvcs = self.pvcs + restore_pvcs
        pvc_size_expanded_initial = 30
        pvc_size_reduced_final = 10
        reduce_size_step = -10

        # Test recover from pending PVC expansion 3 times on each PVC
        for count in range(3):
            logger.info(
                f"Iteration {count + 1} of recovering from pending PVC expansion"
            )

            # Scale down rbd and cephfs ctrlplugin pod. To do this scale down operator deployments first
            logger.info(
                "Scale down operator deployments first to avoid reconciling ctrlplugin deployments after scaled down"
            )
            dep_ocp = OCP(
                kind=constants.DEPLOYMENT,
                namespace=config.ENV_DATA["cluster_namespace"],
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

            logger.info(
                f"Trying to expand the PVCs to {pvc_size_expanded_initial}Gi and when that is pending, reduce the "
                f"size in steps of {reduce_size_step}Gi"
            )
            # Size in each stage will be pvc_size_expanded_initial, pvc_size_expanded_initial-10 and
            # pvc_size_reduced_final
            for size in range(
                pvc_size_expanded_initial,
                pvc_size_reduced_final + reduce_size_step,
                reduce_size_step,
            ):
                for pvc_obj in all_pvcs:
                    logger.info(
                        f"Change the size of the PVC {pvc_obj.name} to {size}Gi"
                    )
                    try:
                        assert not pvc_obj.resize_pvc(
                            size, True, timeout=60
                        ), f"Unexpected: Expansion of PVC '{pvc_obj.name}' completed"
                        logger.debug(pvc_obj.describe())
                    except TimeoutExpiredError:
                        logger.info(
                            f"Expected: Expansion of PVC {pvc_obj.name} to {size}Gi did not complete"
                        )
                logger.info(
                    f"Expected: PVCs did not change capacity to the size {size}Gi"
                )
            logger.info(
                f"Expected: PVCs did not change capacity to the different size in stages. Last applied size in the "
                f"spec of all PVCs is {pvc_size_reduced_final}Gi"
            )

            # Verify that the size cannot be reduced below the current capacity
            logger.info(
                "Verify that the size of the PVCs cannot be reduced below the current capacity"
            )
            for pvc_obj in all_pvcs:
                try:
                    pvc_obj.resize_pvc(self.pvc_size - 1, False)
                except CommandFailed as err:
                    expected_error = "field can not be less than status.capacity"
                    if expected_error in str(err):
                        logger.info(
                            f"Verified: The size of the PVC {pvc_obj.name} cannot be reduced to {self.pvc_size - 1}Gi "
                            f"which is below its current capacity of {self.pvc_size}Gi"
                        )
                    else:
                        raise

            # Scale back the ceph-csi-controller-manager. This will scale up all other deployments that was scaled down
            dep_ocp.exec_oc_cmd(
                "scale deployment ceph-csi-controller-manager --replicas=1"
            )

            # Now PVCs are expected to expand to the reduced size
            for pvc_obj in all_pvcs:
                for pvc_data in TimeoutSampler(240, 2, pvc_obj.get):
                    capacity = pvc_data.get("status").get("capacity").get("storage")
                    if capacity == f"{pvc_size_reduced_final}Gi":
                        break
                    logger.info(
                        f"Capacity of PVC {pvc_obj.name} is not {pvc_size_reduced_final}Gi as "
                        f"expected, but {capacity}. Retrying."
                    )
                logger.info(
                    f"Verified that the capacity of PVC {pvc_obj.name} is changed to "
                    f"{pvc_size_reduced_final}Gi."
                )

            logger.info(
                f"Iteration {count + 1} of recovering from pending PVC expansion completed"
            )
            pvc_size_expanded_initial = (
                pvc_size_expanded_initial + pvc_size_reduced_final
            )
            pvc_size_reduced_final += abs(reduce_size_step)

        # Verify md5sum
        for pod_obj in all_pods:
            verify_data_integrity(
                pod_obj=pod_obj,
                file_name="test_pvc_expand_recover",
                original_md5sum=pod_obj.orig_md5_sum,
            )
