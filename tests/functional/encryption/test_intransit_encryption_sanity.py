import logging
import pytest
import time
from ocs_ci.ocs.resources.storage_cluster import (
    set_in_transit_encryption,
    get_in_transit_encryption_config_state,
)
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_ocs_version,
    green_squad,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_pods
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger(__name__)


@green_squad
@skipif_ocs_version("<4.18")
class TestInTransitEncryptionSanity:
    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        deletes any created pods after the test finishes.
        """

        def teardown():
            # Delete all pods created during the test
            for pod_obj in getattr(self, "all_pods", []):
                pod_obj.delete()

        # Add the teardown function to pytest's finalizer
        request.addfinalizer(teardown)

    def toggle_intransit_encryption_state(self):
        """
        Toggles the in-transit encryption state on the cluster.
        """
        current_state = get_in_transit_encryption_config_state()
        new_state = not current_state

        log.info(
            f"Toggling in-transit encryption from "
            f"{'enabled' if current_state else 'disabled'} to "
            f"{'enabled' if new_state else 'disabled'}."
        )

        result = set_in_transit_encryption(enabled=new_state)
        assert result, "Failed to toggle in-transit encryption state."
        log.info(
            f"In-transit encryption is now {'enabled' if new_state else 'disabled'}."
        )
        return result

    @tier1
    @pytest.mark.polarion_id("OCS-4861")
    def test_intransit_encryption_enable_disable_statetransition(
        self, multi_pvc_factory, pod_factory, set_encryption_at_teardown
    ):
        """
        Test to validate in-transit encryption enable-disable state transitions.

        Steps:
        1. Create a cephfs, rpd pvcs with different access mode.
        2. Change in-transit Encryption state.
        3. Create a pods and attach the PVC to it.
        4. Start IO from  All pods.
        5. During the IO running on the pod toggle intransit encryption state.
        """
        size = 5
        access_modes = {
            constants.CEPHBLOCKPOOL: [
                f"{constants.ACCESS_MODE_RWO}-Block",
                f"{constants.ACCESS_MODE_RWX}-Block",
            ],
            constants.CEPHFILESYSTEM: [
                constants.ACCESS_MODE_RWO,
                constants.ACCESS_MODE_RWX,
            ],
        }

        # Create PVCs for CephBlockPool and CephFS
        pvc_objects = {
            interface: multi_pvc_factory(
                interface=interface,
                access_modes=modes,
                size=size,
                num_of_pvc=2,
            )
            for interface, modes in access_modes.items()
        }

        for interface, pvcs in pvc_objects.items():
            assert pvcs, f"Failed to create PVCs for {interface}."

        # Toggle encryption state
        assert (
            self.toggle_intransit_encryption_state()
        ), "Failed to change in-transit encryption state."

        # Create pods for each interface
        self.all_pods = []
        for interface, pvcs in pvc_objects.items():
            pods = create_pods(
                pvc_objs=pvcs,
                pod_factory=pod_factory,
                interface=interface,
                pods_for_rwx=2,  # Create 2 pods for each RWX PVC
                status=constants.STATUS_RUNNING,
            )
            assert pods, f"Failed to create pods for {interface}."
            self.all_pods.extend(pods)

        # Perform I/O on all pods using ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    pod_obj.run_io, storage_type="fs", size="1G", runtime=60
                )
                for pod_obj in self.all_pods
            ]

            # Toggle encryption state during I/O operations
            for _ in range(2):
                log.info("Toggling encryption state during I/O.")
                assert (
                    self.toggle_intransit_encryption_state()
                ), "Failed to change in-transit encryption state."
                time.sleep(5)

            # Wait for I/O operations to complete
            for future in futures:
                future.result()
