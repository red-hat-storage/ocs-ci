import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    polarion_id,
    ManageTest,
    tier1,
    acceptance,
    ignore_data_rebalance,
)
from ocs_ci.helpers import helpers
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@ignore_data_rebalance
@skipif_ocs_version("<=4.9")
class TestRbdThickProvisioning(ManageTest):
    """
    Tests to verify PVC creation and consumption using RBD thick provisioning enabled storage class

    """

    @pytest.fixture(autouse=True)
    def setup(self, storageclass_factory):
        """
        Create storage class

        """

        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            new_rbd_pool=True,
            rbd_thick_provision=True,
        )

    @acceptance
    @tier1
    @polarion_id("OCS-2500")
    def test_rbd_thick_provisioning(self, multi_pvc_factory, pod_factory):
        """
        Test to verify RBD thick provisioning enabled storage class creation, PVC creation and consumption using
        the storage class.

        """
        # Dict represents the PVC size and size of the file used to consume the volume
        pvc_and_file_sizes = {1: "900M", 5: "4G"}

        access_modes_rbd = [
            constants.ACCESS_MODE_RWO,
            f"{constants.ACCESS_MODE_RWO}-Block",
            f"{constants.ACCESS_MODE_RWX}-Block",
        ]

        pvcs = []

        # Create PVCs
        for pvc_size in pvc_and_file_sizes.keys():
            pvc_objs = multi_pvc_factory(
                interface=constants.CEPHBLOCKPOOL,
                storageclass=self.sc_obj,
                size=pvc_size,
                access_modes=access_modes_rbd,
                status=constants.STATUS_BOUND,
                num_of_pvc=3,
                timeout=300,
            )
            for pvc_obj in pvc_objs:
                pvc_obj.io_file_size = pvc_and_file_sizes[pvc_size]
                pvc_obj.storage_type = (
                    constants.WORKLOAD_STORAGE_TYPE_BLOCK
                    if pvc_obj.get()["spec"]["volumeMode"]
                    == constants.VOLUME_MODE_BLOCK
                    else constants.WORKLOAD_STORAGE_TYPE_FS
                )
            pvcs.extend(pvc_objs)

        # Create pods
        pods = helpers.create_pods(
            pvc_objs=pvcs,
            pod_factory=pod_factory,
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_RUNNING,
        )

        executor = ThreadPoolExecutor(max_workers=len(pods))

        # Do setup for running IO on pods
        log.info("Setting up pods to running IO")
        for pod_obj in pods:
            executor.submit(
                pod_obj.workload_setup, storage_type=pod_obj.pvc.storage_type
            )

        # Wait for setup on pods to complete
        for pod_obj in pods:
            log.info(f"Waiting for IO setup to complete on pod {pod_obj.name}")
            for sample in TimeoutSampler(360, 2, getattr, pod_obj, "wl_setup_done"):
                if sample:
                    log.info(
                        f"Setup for running IO is completed on pod " f"{pod_obj.name}."
                    )
                    break
        log.info("Setup for running IO is completed on pods")

        # Start IO
        for pod_obj in pods:
            log.info(f"Starting IO on pod {pod_obj.name}.")
            pod_obj.run_io(
                storage_type=pod_obj.pvc.storage_type,
                size=pod_obj.pvc.io_file_size,
                runtime=30,
            )
            log.info(f"IO started on pod {pod_obj.name}.")
        log.info("IO started on pods.")

        log.info("Verifying IO on pods.")
        for pod_obj in pods:
            pod_obj.get_fio_results()
            log.info(f"IO completed on pod {pod_obj.name}.")
        log.info("IO finished on all pods.")
