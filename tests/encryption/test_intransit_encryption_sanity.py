import logging
import pytest

from ocs_ci.ocs.resources.storage_cluster import (
    in_transit_encryption_verification,
    set_in_transit_encryption,
)
from ocs_ci.framework.pytest_customization.marks import (
    skipif_intransit_encryption_notset,
    tier1,
    skipif_ocs_version,
)

log = logging.getLogger(__name__)


@skipif_intransit_encryption_notset
class TestInTransitEncryptionSanity:
    def teardown(self):
        set_in_transit_encryption()

    @tier1
    @skipif_ocs_version("<4.12")
    @pytest.mark.polarion_id("OCS-4861")
    def test_intransit_encryption_enable_disable_statetransition(self, request):
        """
        The test does the following:
        1. Verify in-transit Encryption is Enable on setup.
        2. Disable Encryption
        3. Verify in-transit encryption configuration is removed.
        4. Enable encryption Again and verify it.
        5. Verify in-transit encryption config is exists.

        """
        log.info("Verifying the in-transit encryption is enable on setup.")
        assert in_transit_encryption_verification()
        request.addfinalizer(self.teardown)

        log.info("Disabling the in-transit encryption.")
        set_in_transit_encryption(enabled=False)

        # Verify that encryption is actually disabled by checking that a ValueError is raised.
        log.info("Verifying the in-transit encryption is disabled.")
        with pytest.raises(ValueError):
            assert not in_transit_encryption_verification()

        log.info("Re-enabling in-transit encryption.")
        set_in_transit_encryption()

        # Verify that encryption is enabled again after re-enabling it
        log.info(
            "Verifying the in-transit encryption config after enabling the cluster."
        )
        assert in_transit_encryption_verification()
