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
    skipif_ocp_version,
)
from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity
from ocs_ci.helpers import disruption_helpers
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.framework import config

log = logging.getLogger(__name__)


@green_squad
@tier4c
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@ignore_leftover_label(
    constants.drain_canary_pod_label, constants.ROOK_CEPH_DETECT_VERSION_LABEL
)
@pytest.mark.polarion_id("OCS-2413")
class TestResourceDeletionDuringPvcClone(ManageTest):
    """
    Tests to verify PVC clone will succeeded if rook-ceph, csi pods are
    re-spun while creating the clone

    """

    provider_index = None
    consumer_index = None

    @pytest.fixture(autouse=True)
    def setup(self, request, project_factory, pvc_clone_factory, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        if config.ENV_DATA["platform"].lower() in constants.HCI_PC_OR_MS_PLATFORM:
            # Get the index of current cluster
            initial_cluster_index = config.cur_index
            # Get the index of provider cluster. provider_index will be used as a flag to decide whether switching to
            # provider cluster index is required
            self.provider_index = config.get_provider_index()
            # Get the index of a consumer cluster
            self.consumer_index = config.get_consumer_indexes_list()[0]

            def finalizer():
                # Switching to provider cluster context will be done during the test case.
                # Switch back to consumer cluster context after the test case.
                config.switch_ctx(initial_cluster_index)

            request.addfinalizer(finalizer)

        self.pvc_size = 3
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=self.pvc_size, num_of_rbd_pvc=6, num_of_cephfs_pvc=4
        )

    def test_resource_deletion_during_pvc_clone(self, pvc_clone_factory, pod_factory):
        """
        Verify PVC clone will succeeded if rook-ceph, csi pods are re-spun
        while creating the clone

        """
        pods_to_delete = [
            "rbdplugin_provisioner",
            "cephfsplugin_provisioner",
            "cephfsplugin",
            "rbdplugin",
        ]
        if not config.DEPLOYMENT["external_mode"]:
            pods_to_delete.extend(["osd", "mgr"])
        executor = ThreadPoolExecutor(max_workers=len(self.pvcs) + len(pods_to_delete))
        disruption_ops = [disruption_helpers.Disruptions() for _ in pods_to_delete]
        file_name = "file_clone"

        # Run IO
        log.info("Running fio on all pods to create a file")
        for pod_obj in self.pods:
            storage_type = (
                "block"
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK)
                else "fs"
            )
            pod_obj.run_io(
                storage_type=storage_type,
                size="1G",
                runtime=30,
                fio_filename=file_name,
                end_fsync=1,
            )

        log.info("Wait for IO to complete on pods")
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            log.info(f"Verified IO on pod {pod_obj.name}")
            # Calculate md5sum
            file_name_pod = (
                file_name
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_FILESYSTEM)
                else pod_obj.get_storage_path(storage_type="block")
            )
            pod_obj.pvc.md5sum = cal_md5sum(
                pod_obj,
                file_name_pod,
                pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK,
            )
            log.info(f"md5sum obtained from pod {pod_obj.name}")
        log.info("IO is successful on all pods")

        # Select the pods to be deleted
        for disruption, pod_type in zip(disruption_ops, pods_to_delete):
            cluster_index = None
            # 'provider_index' will not be None if the platform is Managed Services
            if self.provider_index is not None:
                if pod_type in ["osd", "mgr"]:
                    cluster_index = self.provider_index
                    config.switch_to_provider()
                else:
                    cluster_index = self.consumer_index
                    config.switch_ctx(cluster_index)

            disruption.set_resource(resource=pod_type, cluster_index=cluster_index)

        # Switch cluster context if the platform is MS. 'provider_index' will not be None if platform is MS.
        if self.provider_index is not None:
            config.switch_ctx(self.consumer_index)

        # Clone PVCs
        log.info("Start creating clone of PVCs")
        for pvc_obj in self.pvcs:
            log.info(f"Creating clone of PVC {pvc_obj.name}")
            pvc_obj.clone_proc = executor.submit(
                pvc_clone_factory,
                pvc_obj=pvc_obj,
                status="",
                access_mode=pvc_obj.get_pvc_access_mode,
                volume_mode=pvc_obj.volume_mode,
            )
        log.info("Started creating clone")

        # Delete the pods 'pods_to_delete'
        log.info(f"Deleting pods {pods_to_delete}")
        for disruption in disruption_ops:
            disruption.delete_proc = executor.submit(disruption.delete_resource)

        # Wait for delete and recovery
        [disruption.delete_proc.result() for disruption in disruption_ops]

        # Get cloned PVCs
        clone_pvc_objs = []
        for pvc_obj in self.pvcs:
            clone_obj = pvc_obj.clone_proc.result()
            clone_pvc_objs.append(clone_obj)
            log.info(f"Created clone {clone_obj.name} of PVC {pvc_obj.name}")
        log.info("Created clone of all PVCs")

        # Confirm that the cloned PVCs are Bound
        log.info("Verifying the cloned PVCs are Bound")
        for pvc_obj in clone_pvc_objs:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=300
            )
            pvc_obj.reload()
            pvc_obj.volume_mode = pvc_obj.data["spec"]["volumeMode"]
        log.info("Verified: Cloned PVCs are Bound.")

        clone_pod_objs = []

        # Attach the cloned PVCs to pods
        log.info("Attach the cloned PVCs to pods")
        for pvc_obj in clone_pvc_objs:
            if pvc_obj.volume_mode == constants.VOLUME_MODE_BLOCK:
                pod_dict_path = constants.CSI_RBD_RAW_BLOCK_POD_YAML
            else:
                pod_dict_path = ""
            restore_pod_obj = pod_factory(
                interface=pvc_obj.interface,
                pvc=pvc_obj,
                status="",
                pod_dict_path=pod_dict_path,
                raw_block_pv=pvc_obj.volume_mode == constants.VOLUME_MODE_BLOCK,
            )
            clone_pod_objs.append(restore_pod_obj)

        # Verify the new pods are running
        log.info("Verify the new pods are running")
        for pod_obj in clone_pod_objs:
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        log.info("Verified: New pods are running")

        # Verify md5sum
        log.info("Verify md5sum")
        for pod_obj in clone_pod_objs:
            file_name_pod = (
                file_name
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_FILESYSTEM)
                else pod_obj.get_storage_path(storage_type="block")
            )
            verify_data_integrity(
                pod_obj,
                file_name_pod,
                pod_obj.pvc.parent.md5sum,
                pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK,
            )
            log.info(
                f"Verified: md5sum of {file_name_pod} on pod {pod_obj.name} "
                f"matches with the original md5sum"
            )
        log.info("Data integrity check passed on all pods")

        # Run IO
        log.info("Running IO on new pods")
        for pod_obj in clone_pod_objs:
            storage_type = (
                "block"
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK)
                else "fs"
            )
            pod_obj.run_io(
                storage_type=storage_type,
                size="1G",
                runtime=20,
                fio_filename=file_name,
                end_fsync=1,
            )

        log.info("Wait for IO to complete on new pods")
        for pod_obj in clone_pod_objs:
            pod_obj.get_fio_results()
            log.info(f"Verified IO on new pod {pod_obj.name}")
        log.info("IO to completed on new pods")
