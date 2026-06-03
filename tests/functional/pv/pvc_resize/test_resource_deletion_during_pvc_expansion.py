import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier4c,
    ignore_leftover_label,
    skipif_upgraded_from,
    skipif_external_mode,
)
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler
from ocs_ci.helpers import disruption_helpers
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


@green_squad
@tier4c
@skipif_ocs_version("<4.5")
@skipif_upgraded_from(["4.4"])
@ignore_leftover_label(constants.drain_canary_pod_label)
@pytest.mark.parametrize(
    argnames="resource_to_delete",
    argvalues=[
        pytest.param(
            "mgr", marks=[pytest.mark.polarion_id("OCS-2224"), skipif_external_mode]
        ),
        pytest.param(
            "osd", marks=[pytest.mark.polarion_id("OCS-2225"), skipif_external_mode]
        ),
        pytest.param("rbdplugin", marks=pytest.mark.polarion_id("OCS-2226")),
        pytest.param("cephfsplugin", marks=pytest.mark.polarion_id("OCS-2227")),
        pytest.param(
            "rbdplugin_provisioner", marks=pytest.mark.polarion_id("OCS-2228")
        ),
        pytest.param(
            "cephfsplugin_provisioner", marks=pytest.mark.polarion_id("OCS-2229")
        ),
    ],
)
class TestResourceDeletionDuringPvcExpansion(ManageTest):
    """
    Tests to verify PVC expansion will be success even if rook-ceph, csi pods
    are re-spun during the expansion

    """

    provider_index = None

    @pytest.fixture(autouse=True)
    def setup(self, resource_to_delete, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        if (
            config.ENV_DATA["platform"].lower() in constants.HCI_PC_OR_MS_PLATFORM
        ) and (resource_to_delete in ["mds", "mon", "mgr", "osd"]):
            # Get the index of current cluster
            self.initial_cluster_index = config.cur_index
            # Get the index of a consumer cluster
            self.consumer_index = config.get_consumer_indexes_list()[0]
            # Get the index of provider cluster. provider_index will act as the flag to decide if switch to provider is
            # required
            self.provider_index = config.get_provider_index()
        self.pvcs, self.pods = create_pvcs_and_pods(pvc_size=10, pods_for_rwx=2)

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Switch back to initial cluster context if applicable
        Make sure ceph health is ok

        """

        def finalizer():
            # Switching to provider cluster context will be done during the test case in certain cases.
            # Switch back to consumer cluster context after the test case.
            if self.provider_index:
                config.switch_ctx(self.initial_cluster_index)
            assert ceph_health_check(), "Ceph cluster health is not OK"
            logger.info("Ceph cluster health is OK")

        request.addfinalizer(finalizer)

    def test_resource_deletion_during_pvc_expansion(self, resource_to_delete):
        """
        Verify PVC expansion will succeed when rook-ceph, csi pods are re-spun
        during expansion

        """
        pvc_size_expanded = 30
        executor = ThreadPoolExecutor(max_workers=len(self.pvcs))
        disruption_ops = disruption_helpers.Disruptions()

        logger.test_step("Run IO on all pods to fill data before PVC expansion")
        for pod_obj in self.pods:
            storage_type = "block" if pod_obj.pvc.volume_mode == "Block" else "fs"
            pod_obj.run_io(
                storage_type=storage_type,
                size="4G",
                io_direction="write",
                runtime=30,
                rate="10M",
                fio_filename=f"{pod_obj.name}_f1",
                direct=int(storage_type == "block"),
            )

        logger.info("Waiting for IO to complete on all pods")
        for pod_obj in self.pods:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert err_count == 0, (
                f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
            )
            logger.debug(f"Verified IO on pod {pod_obj.name}")
        logger.info("IO is successful on all pods before PVC expansion")

        if self.provider_index is not None:
            # Switch to provider cluster context to get ceph pods
            config.switch_to_provider()

        # Select the pod to be deleted
        disruption_ops.set_resource(resource=resource_to_delete)

        if self.provider_index is not None:
            config.switch_ctx(self.consumer_index)

        logger.test_step(
            f"Expand all PVCs to {pvc_size_expanded}G while deleting {resource_to_delete} pod"
        )
        for pvc_obj in self.pvcs:
            logger.debug(
                f"Expanding size of PVC {pvc_obj.name} to {pvc_size_expanded}G"
            )
            pvc_obj.expand_proc = executor.submit(
                pvc_obj.resize_pvc, pvc_size_expanded, True
            )

        # Delete the pod 'resource_to_delete'
        disruption_ops.delete_resource()

        logger.test_step("Verify PVC expansion completed successfully")
        for pvc_obj in self.pvcs:
            expand_result = pvc_obj.expand_proc.result()
            logger.assertion(
                f"PVC {pvc_obj.name} expansion: expected=True, actual={expand_result}"
            )
            assert expand_result, f"Expansion failed for PVC {pvc_obj.name}"
        logger.info("PVC expansion was successful on all PVCs")

        logger.test_step(
            f"Verify expanded size {pvc_size_expanded}G is reflected on pods"
        )
        for pod_obj in self.pods:
            if pod_obj.pvc.volume_mode == "Block":
                logger.debug(
                    f"Skipping size check on pod {pod_obj.name} (volume mode is Block)"
                )
                continue

            # Wait for 240 seconds to reflect the change on pod
            logger.debug(f"Checking pod {pod_obj.name} to verify expanded size")
            for df_out in TimeoutSampler(
                240, 3, pod_obj.exec_cmd_on_pod, command="df -kh"
            ):
                if not df_out:
                    continue
                df_out = df_out.split()
                new_size_mount = df_out[df_out.index(pod_obj.get_storage_path()) - 4]
                if (
                    pvc_size_expanded - 0.7
                    <= float(new_size_mount[:-1])
                    <= pvc_size_expanded
                    and new_size_mount[-1] == "G"
                ):
                    logger.info(
                        f"Verified: Expanded size of PVC {pod_obj.pvc.name} "
                        f"is reflected on pod {pod_obj.name}"
                    )
                    break
                logger.debug(
                    f"Expanded size of PVC {pod_obj.pvc.name} not yet reflected"
                    f" on pod {pod_obj.name}. Current mount size: {new_size_mount},"
                    f" expected: {pvc_size_expanded}G. Retrying."
                )
        logger.info(
            f"Verified: Modified size {pvc_size_expanded}G is reflected on all pods."
        )

        logger.test_step("Run IO on all pods after PVC expansion to verify usability")
        for pod_obj in self.pods:
            storage_type = "block" if pod_obj.pvc.volume_mode == "Block" else "fs"
            pod_obj.run_io(
                storage_type=storage_type,
                size="10G",
                io_direction="write",
                runtime=30,
                rate="10M",
                fio_filename=f"{pod_obj.name}_f2",
                end_fsync=1,
                direct=int(storage_type == "block"),
            )

        logger.info("Waiting for IO to complete on all pods")
        for pod_obj in self.pods:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert err_count == 0, (
                f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
            )
            logger.debug(f"Verified IO on pod {pod_obj.name}")
        logger.info("IO is successful on all pods after PVC expansion")
