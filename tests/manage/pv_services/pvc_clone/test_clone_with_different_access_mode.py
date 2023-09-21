import logging
import pytest
from itertools import cycle

from ocs_ci.ocs import constants, node
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    polarion_id,
    skipif_ocp_version,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@green_squad
@tier1
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@polarion_id("OCS-2368")
class TestCloneWithDifferentAccessMode(ManageTest):
    """
    Tests to verify PVC clone with access mode different than parent PVC

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, pvc_clone_factory, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        self.pvcs, self.pods = create_pvcs_and_pods(pvc_size=3)

    def test_clone_with_different_access_mode(self, pvc_clone_factory, pod_factory):
        """
        Create clone of a PVC with an access mode different than parent PVC

        """
        file_name = "fio_test"
        access_modes_dict = {
            constants.CEPHBLOCKPOOL: {
                constants.VOLUME_MODE_FILESYSTEM: [constants.ACCESS_MODE_RWO],
                constants.VOLUME_MODE_BLOCK: [
                    constants.ACCESS_MODE_RWX,
                    constants.ACCESS_MODE_RWO,
                ],
            },
            constants.CEPHFILESYSTEM: {
                constants.VOLUME_MODE_FILESYSTEM: [
                    constants.ACCESS_MODE_RWX,
                    constants.ACCESS_MODE_RWO,
                ]
            },
        }

        # Run IO
        log.info("Starting IO on all pods")
        for pod_obj in self.pods:
            storage_type = (
                "block"
                if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK
                else "fs"
            )
            pod_obj.run_io(
                storage_type=storage_type,
                size="1G",
                runtime=20,
                fio_filename=file_name,
                end_fsync=1,
            )
            log.info(f"IO started on pod {pod_obj.name}")
        log.info("Started IO on all pods")

        # Wait for IO to finish
        log.info("Wait for IO to finish on pods")
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            log.info(f"IO finished on pod {pod_obj.name}")
            # Calculate md5sum
            file_name_pod = (
                file_name
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_FILESYSTEM)
                else pod_obj.get_storage_path(storage_type="block")
            )
            pod_obj.pvc.md5sum = pod.cal_md5sum(
                pod_obj,
                file_name_pod,
                pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK,
            )

        log.info("Creating clone of the PVCs with different access modes")
        cloned_pvcs = []
        for pvc_obj in self.pvcs:
            access_modes = access_modes_dict[pvc_obj.interface][pvc_obj.volume_mode]
            for access_mode in access_modes:
                clone_obj = pvc_clone_factory(
                    pvc_obj=pvc_obj, status="", access_mode=access_mode
                )
                clone_obj.interface = pvc_obj.interface
                log.info(
                    f"Clone {clone_obj.name} created. "
                    f"Parent PVC: {pvc_obj.name}. "
                    f"Parent accessMode: {pvc_obj.get_pvc_access_mode}. "
                    f"Cloned PVC accessMode: {access_mode}"
                )
                cloned_pvcs.append(clone_obj)
        log.info("Created clone of the PVCs with different access modes")

        log.info("Verifying cloned PVCs are Bound")
        for pvc_obj in cloned_pvcs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=200
            )
            pvc_obj.reload()
        log.info("Verified: Cloned PVCs are Bound")

        # Get worker node names and create an iterator
        nodes_iter = cycle(node.get_worker_nodes())

        # Attach the cloned PVCs to pods
        log.info("Attach the cloned PVCs to pods")
        clone_pod_objs = []
        for pvc_obj in cloned_pvcs:
            if pvc_obj.volume_mode == "Block":
                pod_dict_path = constants.CSI_RBD_RAW_BLOCK_POD_YAML
            else:
                pod_dict_path = ""
            # Create 2 pods if access mode is RWX, else 1
            for _ in range(
                int(pvc_obj.get_pvc_access_mode != constants.ACCESS_MODE_RWX), 2
            ):
                clone_pod_obj = pod_factory(
                    interface=pvc_obj.interface,
                    pvc=pvc_obj,
                    status="",
                    node_name=next(nodes_iter),
                    pod_dict_path=pod_dict_path,
                    raw_block_pv=pvc_obj.volume_mode == "Block",
                )
                log.info(
                    f"Attaching the PVC {pvc_obj.name} to pod " f"{clone_pod_obj.name}"
                )
                clone_pod_objs.append(clone_pod_obj)

        # Verify the new pods are running
        log.info("Verify the new pods are running")
        for pod_obj in clone_pod_objs:
            helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        log.info("Verified: New pods are running")

        # Verify md5sum
        for pod_obj in clone_pod_objs:
            file_name_pod = (
                pod_obj.get_storage_path(storage_type="block")
                if (pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK)
                else file_name
            )
            pod.verify_data_integrity(
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
