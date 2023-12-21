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

log = logging.getLogger(__name__)


@pytest.mark.skip(
    reason="Skip due to issue https://github.com/red-hat-storage/ocs-ci/issues/8759"
)
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

    @tier1
    @skipif_ocs_version("<4.13")
    @pytest.mark.polarion_id("OCS-4861")
    def test_intransit_encryption_enable_disable_statetransition(self):
        """
        The test does the following:
        1. Enable in-transit Encryption if not Enabled.
        2. Verify in-transit Encryption is Enable on setup.
        3. Disable Encryption
        4. Verify in-transit encryption configuration is removed.
        5. Enable encryption Again and verify it.
        6. Verify in-transit encryption config is exists.

        """
        if not get_in_transit_encryption_config_state():
            if config.ENV_DATA.get("in_transit_encryption"):
                pytest.fail("In-transit encryption is not enabled on the setup")
            else:
                set_in_transit_encryption()

        log.info("Verifying the in-transit encryption is enable on setup.")
        assert in_transit_encryption_verification()

        log.info("Disabling the in-transit encryption.")
        set_in_transit_encryption(enabled=False)

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
