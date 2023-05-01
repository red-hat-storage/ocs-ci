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
        log.info("*******")
        set_in_transit_encryption()

    @tier1
    @skipif_ocs_version("<4.12")
    def test_intransit_encryption_enable_disable_statetransition(self):
        """
        1. Verify intransit Encryption is Enable on setup.
        2. Start IO in background
        3. Disable Encryption
        4. Run verification JOb
        5. enable encryption Again.
        """
        log.info("Verifying the in-transit encryption is enable on setup.")
        assert in_transit_encryption_verification()

        log.info("Disabling the intransit encryption.")
        set_in_transit_encryption(enabled=False)
        log.info("Verifying the intransit encryption is disabled.")
        with pytest.raises(ValueError):
            assert not in_transit_encryption_verification()

        log.info("Re-enabling intransit encryption.")
        set_in_transit_encryption()
        log.info(
            "Verifying the intransit encryption config after enabling the cluster."
        )
        assert in_transit_encryption_verification()
