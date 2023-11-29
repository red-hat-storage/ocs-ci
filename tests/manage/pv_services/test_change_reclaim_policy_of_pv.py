import logging
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier1, skipif_managed_service
from ocs_ci.ocs.constants import RECLAIM_POLICY_DELETE, RECLAIM_POLICY_RETAIN
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    verify_volume_deleted_in_backend,
    default_ceph_block_pool,
    default_storage_class,
)

log = logging.getLogger(__name__)


@green_squad
@tier1
@pytest.mark.parametrize(
    argnames=["interface", "reclaim_policy"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, RECLAIM_POLICY_DELETE],
            marks=pytest.mark.polarion_id("OCS-939"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, RECLAIM_POLICY_RETAIN],
            marks=[pytest.mark.polarion_id("OCS-962"), skipif_managed_service],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, RECLAIM_POLICY_DELETE],
            marks=pytest.mark.polarion_id("OCS-963"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, RECLAIM_POLICY_RETAIN],
            marks=[pytest.mark.polarion_id("OCS-964"), skipif_managed_service],
        ),
    ],
)
class TestChangeReclaimPolicyOfPv(ManageTest):
    """
    This test class consists of tests to update reclaim policy of PV
    """

    pvc_objs = None
    pod_objs = None
    sc_obj = None
    num_of_pvc = 10
    executor = ThreadPoolExecutor(max_workers=num_of_pvc)

    @pytest.fixture(autouse=True)
    def setup(
        self,
        interface,
        reclaim_policy,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Create pvc and pod
        """
        # Create storage class if reclaim policy is not "Delete"
        self.sc_obj = (
            default_storage_class(interface)
            if reclaim_policy == RECLAIM_POLICY_DELETE
            else storageclass_factory(
                interface=interface, reclaim_policy=reclaim_policy
            )
        )

        # Create PVCs
        self.pvc_objs = multi_pvc_factory(
            interface=interface,
            project=None,
            storageclass=self.sc_obj,
            size=5,
            status=constants.STATUS_BOUND,
            num_of_pvc=self.num_of_pvc,
            wait_each=False,
        )

        # Create pods
        self.pod_objs = []
        for pvc_obj in self.pvc_objs:
            self.pod_objs.append(
                pod_factory(interface=interface, pvc=pvc_obj, status=None)
            )
        for pod in self.pod_objs:
            wait_for_resource_state(pod, constants.STATUS_RUNNING, 180)
            pod.reload()

    def run_and_verify_io(self, pods_list, do_setup=True):
        """
        Run IO on pods and verify IO result

        Args:
            pods_list(list): List of POD objects
            do_setup(bool): True if workload setup has to be done, else False

        """
        if do_setup:
            # Do setup on pods for running IO
            log.info("Setting up pods for running IO.")
            for pod_obj in pods_list:
                self.executor.submit(pod_obj.workload_setup, storage_type="fs")

            # Wait for setup on pods to complete
            for pod_obj in pods_list:
                for sample in TimeoutSampler(360, 2, getattr, pod_obj, "wl_setup_done"):
                    if sample:
                        log.info(
                            f"Setup for running IO is completed on pod "
                            f"{pod_obj.name}."
                        )
                        break
            log.info("Setup for running IO is completed on all pods.")

        # Run IO on pods
        for pod_obj in pods_list:
            pod_obj.run_io(
                storage_type="fs",
                size="1G",
                runtime=30,
                fio_filename=f"{pod_obj.name}_io_file1",
            )
        log.info("Ran IO on pods.")

        # Verify IO results
        for pod_obj in pods_list:
            fio_result = pod_obj.get_fio_results()
            err_num = fio_result.get("jobs")[0].get("error")
            assert (
                err_num == 0
            ), f"FIO error on pod {pod_obj.name}. FIO result: {fio_result}"
            log.info(f"IOPs after FIO on pod {pod_obj.name}:")
            log.info(f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}")
            log.info(f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}")

    def test_change_reclaim_policy_of_pv(self, interface, reclaim_policy, pod_factory):
        """
        This test case tests update of reclaim policy of PV
        """
        reclaim_policy_to = "Delete" if reclaim_policy == "Retain" else ("Retain")

        # Fetch name of PVs
        pvs = [pvc_obj.backed_pv_obj for pvc_obj in self.pvc_objs]

        # Fetch image uuid associated with PVCs
        pvc_uuid_map = {}
        for pvc_obj in self.pvc_objs:
            pvc_uuid_map[pvc_obj.name] = pvc_obj.image_uuid
        log.info("Fetched image uuid associated with each PVC")

        # Select PVs to change reclaim policy
        changed_pvs = pvs[:5]

        # Run IO on pods
        self.run_and_verify_io(self.pod_objs)
        log.info("Verified IO result on pods.")

        # Change relaimPolicy to 'reclaim_policy_to'
        for pv_obj in changed_pvs:
            pv_name = pv_obj.name
            patch_param = (
                f'{{"spec":{{"persistentVolumeReclaimPolicy":'
                f'"{reclaim_policy_to}"}}}}'
            )
            assert pv_obj.ocp.patch(
                resource_name=pv_name, params=patch_param, format_type="strategic"
            ), (
                f"Failed to change persistentVolumeReclaimPolicy of pv "
                f"{pv_name} to {reclaim_policy_to}"
            )
            log.info(
                f"Changed persistentVolumeReclaimPolicy of pv {pv_name} "
                f"to {reclaim_policy_to}"
            )

        retain_pvs = []
        delete_pvs = []

        # Verify reclaim policy of all PVs
        for pv_obj in pvs:
            policy = pv_obj.get().get("spec").get("persistentVolumeReclaimPolicy")
            retain_pvs.append(pv_obj) if policy == "Retain" else (
                delete_pvs.append(pv_obj)
            )
            if pv_obj in changed_pvs:
                assert policy == reclaim_policy_to, (
                    f"Reclaim policy of {pv_obj.name} is {policy}. "
                    f"It has not changed to {reclaim_policy_to}"
                )
            else:
                assert policy == reclaim_policy, (
                    f"Reclaim policy of {pv_obj.name} is {policy} instead "
                    f"of {reclaim_policy}."
                )
        log.info("Verified reclaim policy of all PVs")

        # Run IO on pods
        self.run_and_verify_io(self.pod_objs, do_setup=False)
        log.info("Ran and verified IO on pods after changing reclaim policy.")

        # Delete all pods
        log.info("Deleting all pods")
        for pod_obj in self.pod_objs:
            pod_obj.delete()

        # Verify pods are deleted
        for pod_obj in self.pod_objs:
            pod_obj.ocp.wait_for_delete(pod_obj.name, 300)
        log.info("Verified: Pods are deleted.")

        # Create new pods mounting one volume on each pod
        log.info("Creating new pods.")
        new_pod_objs = []
        for pvc_obj in self.pvc_objs:
            new_pod_objs.append(
                pod_factory(interface=interface, pvc=pvc_obj, status=None)
            )
        for pod in new_pod_objs:
            wait_for_resource_state(pod, constants.STATUS_RUNNING, 180)
            pod.reload()

        # Run IO on new pods
        self.run_and_verify_io(new_pod_objs)
        log.info("Ran and verified IO on new pods.")

        # Delete all pods
        log.info("Deleting all new pods.")
        for pod_obj in new_pod_objs:
            pod_obj.delete()

        # Verify pods are deleted
        for pod_obj in new_pod_objs:
            pod_obj.ocp.wait_for_delete(pod_obj.name, 300)
        log.info("Verified: All new pods are deleted.")

        # Delete PVCs
        log.info("Deleting all PVCs.")
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete()

        # Verify PVCs are deleted
        for pvc_obj in self.pvc_objs:
            pvc_obj.ocp.wait_for_delete(pvc_obj.name, 300)
        log.info("Verified: All PVCs are deleted")

        # PVs having reclaim policy 'Delete' will be deleted
        for pv_obj in delete_pvs:
            pv_obj.ocp.wait_for_delete(pv_obj.name, 300)
        log.info("Verified: All PVs having reclaim policy 'Delete' are deleted.")

        # PVs having reclaim policy 'Retain' will be in Released state
        for pv_obj in retain_pvs:
            wait_for_resource_state(resource=pv_obj, state=constants.STATUS_RELEASED)
        log.info(
            "Verified: All PVs having reclaim policy 'Retain' are "
            "in 'Released' state."
        )

        # Change relaimPolicy to Delete
        for pv_obj in retain_pvs:
            pv_name = pv_obj.name
            patch_param = '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
            assert pv_obj.ocp.patch(
                resource_name=pv_name, params=patch_param, format_type="strategic"
            ), (
                f"Failed to change persistentVolumeReclaimPolicy "
                f"for pv {pv_name} to Delete"
            )
        log.info("Changed reclaim policy of all remaining PVs to Delete")

        # Verify PVs deleted. PVs will be deleted immediately after setting
        # reclaim policy to Delete
        for pv_obj in retain_pvs:
            pv_obj.ocp.wait_for_delete(pv_obj.name, 300)
        log.info(
            "Verified: All remaining PVs are deleted after changing reclaim "
            "policy to Delete."
        )

        # Verify PV using ceph toolbox. Wait for Image/Subvolume to be deleted.
        pool_name = (
            default_ceph_block_pool() if interface == constants.CEPHBLOCKPOOL else None
        )
        for pvc_name, uuid in pvc_uuid_map.items():
            assert verify_volume_deleted_in_backend(
                interface=interface, image_uuid=uuid, pool_name=pool_name
            ), f"Volume associated with PVC {pvc_name} still exists in backend"
        log.info("Verified: Image/Subvolume removed from backend.")
