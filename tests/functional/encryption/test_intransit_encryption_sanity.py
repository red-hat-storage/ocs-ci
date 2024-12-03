import logging
import pytest

from ocs_ci.ocs.resources.storage_cluster import (
    in_transit_encryption_verification,
    set_in_transit_encryption,
    get_in_transit_encryption_config_state,
)
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_ocs_version,
    green_squad,
    skipif_hci_provider_and_client,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_pods


log = logging.getLogger(__name__)


@green_squad
@skipif_hci_provider_and_client
class TestInTransitEncryptionSanity:
    @pytest.fixture(autouse=True)
    def set_encryption_at_teardown(self, request):
        def teardown():
            if config.ENV_DATA.get("in_transit_encryption"):
                set_in_transit_encryption()
            else:
                set_in_transit_encryption(enabled=False)

        request.addfinalizer(teardown)

    def toggle_intransit_encryption_state(self):
        """
        Toggles the in-transit encryption state on the cluster.
        """
        encryption_state = get_in_transit_encryption_config_state()
        new_state = not encryption_state

        log.info(
            f"In-transit encryption is currently {'enabled' if encryption_state else 'disabled'} on the cluster."
        )

        result = set_in_transit_encryption(enabled=new_state)

        log.info(
            f"In-transit encryption state is now {'enabled' if new_state else 'disabled'}."
        )
        return result

    @tier1
    @skipif_ocs_version("<4.18")
    @pytest.mark.polarion_id("OCS-4861")
    def test_intransit_encryption_enable_disable_statetransition(
        self, multi_pvc_factory, pod_factory
    ):
        """
        The test does the following:
        1. Enable in-transit Encryption if not Enabled.
        2. Verify in-transit Encryption is Enable on setup.
        3. Disable Encryption
        4. Verify in-transit encryption configuration is removed.
        5. Enable encryption Again and verify it.
        6. Verify in-transit encryption config is exists.

        """
        access_modes_cephfs = [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        access_modes_rbd = [
            f"{constants.ACCESS_MODE_RWO}-Block",
            f"{constants.ACCESS_MODE_RWX}-Block",
        ]
        size = 5

        rbd_pvcs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_modes=access_modes_rbd,
            size=size,
            num_of_pvc=2,
        )
        assert rbd_pvcs, "Failed to create custom_rbd_pvcs PVC"

        cephfs_pvcs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            access_modes=access_modes_cephfs,
            size=size,
            num_of_pvc=2,
        )
        assert cephfs_pvcs, "Failed to create custom_cephfs_pvcs PVC"

        log.info("Changing intransit encryption state")
        assert (
            self.toggle_intransit_encryption_state()
        ), " Failed to change intransit encryption state."

        rbd_pods = create_pods(
            pvc_objs=rbd_pvcs,
            pod_factory=pod_factory,
            interface=constants.CEPHBLOCKPOOL,
            pods_for_rwx=2,  # Create 2 pods for each RWX PVC
            status=constants.STATUS_RUNNING,
        )
        assert rbd_pods

        cephfs_pods = create_pods(
            pvc_objs=cephfs_pvcs,
            pod_factory=pod_factory,
            interface=constants.CEPHFILESYSTEM,  # Specify CephFS as the interface
            pods_for_rwx=2,  # Create 2 pods for each RWX PVC
            status=constants.STATUS_RUNNING,
        )
        assert cephfs_pods

        log.info("Changing intransit encryption state")
        assert (
            self.toggle_intransit_encryption_state()
        ), " Failed to change intransit encryption state."

        # if not get_in_transit_encryption_config_state():
        #     if config.ENV_DATA.get("in_transit_encryption"):
        #         pytest.fail("In-transit encryption is not enabled on the setup")
        #     else:
        #         set_in_transit_encryption()

        # log.info("Verifying the in-transit encryption is enable on setup.")
        # assert in_transit_encryption_verification()
        # Verify that encryption is actually disabled by checking that a ValueError is raised.

        log.info("Verifying the in-transit encryption is disabled.")
        with pytest.raises(ValueError):
            assert (
                not in_transit_encryption_verification()
            ), "In-transit Encryption was expected to be disabled, but it's enabled in the setup."

        if config.ENV_DATA.get("in_transit_encryption"):
            log.info("Re-enabling in-transit encryption.")
            set_in_transit_encryption()

            # Verify that encryption is enabled again after re-enabling it
            log.info(
                "Verifying the in-transit encryption config after enabling the cluster."
            )
            assert in_transit_encryption_verification()
