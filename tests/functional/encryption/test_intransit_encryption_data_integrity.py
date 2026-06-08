import logging
import pytest
import time
from threading import Thread

from ocs_ci.ocs.resources.storage_cluster import (
    in_transit_encryption_verification,
    set_in_transit_encryption,
    get_in_transit_encryption_config_state,
)
from ocs_ci.framework.pytest_customization.marks import (
    skipif_intransit_encryption_notset,
    tier1,
    skipif_ocs_version,
    green_squad,
)
from ocs_ci.ocs.constants import STATUS_BOUND, CEPHBLOCKPOOL

from ocs_ci.framework import config

logger = logging.getLogger(__name__)


@green_squad
@skipif_intransit_encryption_notset
@green_squad
class TestDataIntegrityWithInTransitEncryption:
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
    @pytest.mark.polarion_id("OCS-4920")
    def test_data_integrity_with_intransit_encryption(self, pvc_factory, pod_factory):
        """
        Test data integrity with in-transit encryption.

        Steps:
        1. Verify in-transit encryption is configured on the cluster.
        2. Create a PVC.
        3. Attach the PVC to a pod and run IO with the `verify=True` option.
        4. Disable in-transit encryption while IO is running in the background.
        5. Sleep for 10 seconds.
        6. Enable in-transit encryption.
        7. Wait for the storage cluster to become ready.
        8. Check for IO errors and data corruption errors in the fio logs.
        """
        logger.test_step("Verify in-transit encryption is configured on the cluster")
        if not get_in_transit_encryption_config_state():
            if config.ENV_DATA.get("in_transit_encryption"):
                pytest.fail(
                    "In-transit encryption is not enabled on the setup while it was supposed to be."
                )
            else:
                set_in_transit_encryption()

        in_transit_verified = in_transit_encryption_verification()
        logger.assertion(
            f"In-transit encryption verification: expected='True', "
            f"actual='{in_transit_verified}'"
        )
        assert in_transit_verified, "In transit encryption verification failed."

        logger.test_step("Create PVC and pod, then start IO with verify=True")
        pvc_obj = pvc_factory(interface=CEPHBLOCKPOOL, status=STATUS_BOUND)
        pod_obj = pod_factory(interface=CEPHBLOCKPOOL, pvc=pvc_obj)

        kwargs = {
            "storage_type": "fs",
            "size": "1G",
            "runtime": 120,
            "verify": True,
        }

        io_thread = Thread(
            target=pod_obj.run_io,
            name="io_thread",
            kwargs=kwargs,
        )
        io_thread.start()

        # Disable in-transit encryption for 10 seconds.
        logger.test_step("Disable in-transit encryption while IO is running")
        logger.info("IO thread started. Disabling in-transit encryption for 10 seconds")
        set_in_transit_encryption(enabled=False)

        # Sleeping for 10 seconds to allow some IO workload to occur in the in-transit encryption disabled state.
        time.sleep(10)

        logger.test_step("Re-enable in-transit encryption and wait for IO completion")
        set_in_transit_encryption()

        # Wait for IO thread to finish
        logger.info(
            "In-transit encryption re-enabled. Waiting for the storage cluster to become ready"
        )
        io_thread.join()

        logger.test_step(
            "Verify no IO errors or data corruption after encryption toggle"
        )
        fio_result = pod_obj.get_fio_results()
        err_count = fio_result.get("jobs")[0].get("error")

        logger.assertion(f"FIO error count: expected='0', actual='{err_count}'")
        assert err_count == 0, "Error found in IO"
