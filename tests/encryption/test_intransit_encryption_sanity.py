import logging

from ocs_ci.ocs.resources.storage_cluster import (
    verify_in_transit_encryption_keys_exists,
    enable_in_transit_encryption,
    disable_in_transit_encryption,
)
from ocs_ci.framework.pytest_customization.marks import (
    skipif_intransit_encryption_notset,
    tier1,
    skipif_ocs_version,
)

log = logging.getLogger(__name__)


@skipif_intransit_encryption_notset
class TestInTransitEncryptionSanity:
    @tier1
    @skipif_ocs_version("<4.12")
    def test_intransit_encryption_enable_disable_statetransition(self):
        """
        1. Verify intransit Encryption is Enable on setup.
        2. Disable Intransit Encryption from setup.
        3. Verify config is removed from the cluster.
        4. re-enable intransit encryption on setup.
        5. Again verify intransit encryption config is added.
        """
        log.info("Verifying the in-transit encryption is enable on setup.")
        assert verify_in_transit_encryption_keys_exists()
        log.info("Disabling the intransit encryption.")
        disable_in_transit_encryption()
        log.info("Verifying the intransit encryption is disabled.")
        assert not verify_in_transit_encryption_keys_exists()
        log.info("Re-enabling intransit encryption.")
        enable_in_transit_encryption()
        log.info(
            "Verifying the intransit encryption config after enabling the cluster."
        )
        assert verify_in_transit_encryption_keys_exists()
