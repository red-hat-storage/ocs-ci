import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    polarion_id,
    ManageTest,
    tier1,
    acceptance,
)
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@skipif_ocs_version("<4.8")
@polarion_id("OCS-2500")
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
            interface=constants.CEPHBLOCKPOOL, new_rbd_pool=True
        )

    @acceptance
    @tier1
    @pytest.mark.polarion_id("")
    def test_rbd_thick_provisioning(self, multi_pvc_factory, pod_factory):
        """
        Tests to verify PVC creation and consumption using RBD thick provisioning enabled storage class

        """
        # Size will be added to the list once the bug https://bugzilla.redhat.com/show_bug.cgi?id=1936388 is fixed
        pvc_sizes = [1]

        access_modes_rbd = [
            constants.ACCESS_MODE_RWO,
            f"{constants.ACCESS_MODE_RWO}-Block",
            f"{constants.ACCESS_MODE_RWX}-Block",
        ]

        pvcs = []

        # Create PVCs
        for pvc_size in pvc_sizes:
            pvcs.extend(
                multi_pvc_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    storageclass=self.sc_obj,
                    size=pvc_size,
                    access_modes=access_modes_rbd,
                    status=constants.STATUS_BOUND,
                    num_of_pvc=3,
                    timeout=120,
                )
            )

        # Create pods
        pods = helpers.create_pods(
            pvc_objs=pvcs,
            pod_factory=pod_factory,
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_RUNNING,
        )

        # Start IO
        for pod_obj in pods:
            storage_type = (
                "block" if pod_obj.pvc.get()["spec"]["volumeMode"] == "Block" else "fs"
            )
            log.info(f"Starting IO on pod {pod_obj.name}.")
            pod_obj.run_io(
                storage_type=storage_type,
                size="500M",
                runtime=20,
            )
            log.info(f"IO started on pod {pod_obj.name}.")
        log.info("IO started on pods.")

        log.info("Verifying IO on pods.")
        for pod_obj in pods:
            pod_obj.get_fio_results()
            log.info(f"IO completed on pod {pod_obj.name}.")
        log.info("IO finished on all pods.")
