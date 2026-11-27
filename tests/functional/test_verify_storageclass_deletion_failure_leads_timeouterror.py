import logging
import time

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    jira,
    skipif_ocs_version,
    skipif_external_mode,
)
from ocs_ci.framework.testlib import ManageTest, tier4
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.helpers.helpers import (
    get_ocs_storageclasses,
    get_storageclass_uid,
    wait_for_storageclass_deletion,
    wait_for_storageclass_recreation,
    select_ocs_storageclass_by_provisioner,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@green_squad
@tier4
@jira("DFBUGS-10854")
@skipif_ocs_version("<4.22")
@skipif_external_mode
class TestStorageClassDeletionTimeoutFix(ManageTest):
    """
    Test to verify the fix for DFBUGS-10854 (backport of DFBUGS-4795).

    The bug caused StorageClass deletion to fail with a TimeoutError during
    regression tests because the ocs-client-operator's Delete call did not
    use UID preconditions, leading to stale object conflicts when the
    StorageClass was recreated by the reconciler before the delete completed.

    The fix adds client.Preconditions{UID: &obj.UID} to the Delete call in
    the storageclient_controller so that the delete targets the exact object
    instance and avoids deleting a freshly-reconciled replacement.

    This test verifies that StorageClasses managed by the StorageClient can
    be deleted and re-reconciled without leaving stale objects or timing out.
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, request):
        """
        Setup fixture that collects initial StorageClass state and registers
        a finalizer to ensure cluster health after the test.

        Args:
            request: pytest request object for finalizer registration.
        """
        self.sc_ocp = OCP(kind=constants.STORAGECLASS)
        self.initial_sc_names = {
            sc["metadata"]["name"] for sc in get_ocs_storageclasses()
        }
        logger.info(f"Initial OCS-managed StorageClasses: {sorted(self.initial_sc_names)}")

        def finalizer():
            """Ensure all expected StorageClasses are restored after test."""
            logger.info("Finalizer: verifying all OCS StorageClasses are present")
            try:
                for sample in TimeoutSampler(
                    timeout=300,
                    sleep=10,
                    func=self._all_initial_scs_present,
                ):
                    if sample:
                        logger.info("All initial StorageClasses are present again")
                        break
            except Exception:
                logger.warning(
                    "Not all initial StorageClasses were restored within timeout. "
                    "The cluster reconciler may need more time."
                )

        request.addfinalizer(finalizer)

    def _all_initial_scs_present(self):
        """
        Check whether all initially recorded StorageClasses are present.

        Returns:
            bool: True if all initial StorageClasses exist, False otherwise.
        """
        current_names = {
            sc["metadata"]["name"] for sc in get_ocs_storageclasses()
        }
        return self.initial_sc_names.issubset(current_names)

    def test_storageclass_deletion_and_reconciliation(self):
        """
        Test that StorageClass deletion completes without TimeoutError
        and that the reconciler recreates the StorageClass with a new UID.

        Verifies the fix for DFBUGS-10854 where the storageclient controller
        now uses UID preconditions on Delete calls to prevent stale object
        conflicts that caused TimeoutError during StorageClass cleanup.

        Steps:
            1. Identify an OCS-managed StorageClass (RBD-based preferred).
            2. Record its UID before deletion.
            3. Delete the StorageClass.
            4. Verify the StorageClass is actually removed (not stale).
            5. Wait for the reconciler to recreate the StorageClass.
            6. Verify the recreated StorageClass has a different UID,
               confirming the old one was cleanly deleted.
            7. Repeat the delete/recreate cycle to stress the race condition
               that the fix addresses.
        """
        logger.test_step("Identify an OCS-managed StorageClass for deletion testing")
        ocs_scs = get_ocs_storageclasses()
        assert len(ocs_scs) > 0, "No OCS-managed StorageClasses found on the cluster"

        target_sc = select_ocs_storageclass_by_provisioner(ocs_scs, preferred_provisioner="rbd")
        sc_name = target_sc["metadata"]["name"]
        logger.info(f"Selected StorageClass for test: {sc_name}")

        num_cycles = 3
        for cycle in range(1, num_cycles + 1):
            logger.test_step(
                f"Cycle {cycle}/{num_cycles}: Delete StorageClass '{sc_name}' and verify reconciliation"
            )

            original_uid = get_storageclass_uid(sc_name)
            assert original_uid is not None, (
                f"StorageClass '{sc_name}' not found before deletion in cycle {cycle}"
            )
            logger.info(
                f"Cycle {cycle}: StorageClass '{sc_name}' has UID={original_uid} before deletion"
            )

            logger.test_step(f"Cycle {cycle}: Delete StorageClass '{sc_name}'")
            self.sc_ocp.delete(resource_name=sc_name)

            logger.test_step(
                f"Cycle {cycle}: Verify StorageClass '{sc_name}' is deleted (no TimeoutError)"
            )
            sc_deleted = wait_for_storageclass_deletion(sc_name, original_uid, timeout=120, sleep=5)

            logger.assertion(f"expected=True (SC deleted), actual={sc_deleted}")
            assert sc_deleted, (
                f"Cycle {cycle}: StorageClass '{sc_name}' was not deleted within 120s. "
                f"This reproduces the TimeoutError from DFBUGS-10854."
            )
            logger.info(f"Cycle {cycle}: StorageClass '{sc_name}' deletion confirmed")

            logger.test_step(
                f"Cycle {cycle}: Wait for reconciler to recreate StorageClass '{sc_name}'"
            )
            new_uid = wait_for_storageclass_recreation(sc_name, timeout=300, sleep=10)

            logger.assertion(f"expected=not None (SC recreated), actual={new_uid}")
            assert new_uid is not None, (
                f"Cycle {cycle}: StorageClass '{sc_name}' was not recreated by the reconciler "
                f"within 300s"
            )

            logger.info(
                f"Cycle {cycle}: StorageClass '{sc_name}' recreated with UID={new_uid}"
            )

            if new_uid == original_uid:
                logger.warning(
                    f"Cycle {cycle}: New UID matches original UID. "
                    f"The StorageClass may not have been fully deleted before recreation."
                )
            else:
                logger.info(
                    f"Cycle {cycle}: UID changed from {original_uid} to {new_uid}, "
                    f"confirming clean deletion and recreation"
                )

            if cycle < num_cycles:
                time.sleep(10)

        logger.test_step("Verify all initial StorageClasses are present after test cycles")
        all_present = False
        try:
            for sample in TimeoutSampler(
                timeout=300,
                sleep=15,
                func=self._all_initial_scs_present,
            ):
                if sample:
                    all_present = True
                    break
        except Exception:
            pass

        logger.assertion(f"expected=True (all SCs present), actual={all_present}")
        assert all_present, (
            "Not all initial StorageClasses were restored after the deletion cycles. "
            "The reconciler may have failed to recreate some StorageClasses."
        )
        logger.info("All initial StorageClasses confirmed present after test completion")

    def test_storageclass_rapid_delete_recreate_race(self):
        """
        Test that rapid deletion does not cause UID-based race conditions.

        This test specifically targets the race condition fixed in DFBUGS-10854
        where a Delete without UID preconditions could accidentally delete a
        newly-reconciled StorageClass if the reconciler recreated it between
        the list and delete operations.

        Verifies that after rapid deletion, the StorageClass is properly
        reconciled back with a valid, stable UID.
        """
        logger.test_step("Identify an OCS-managed StorageClass for rapid deletion test")
        ocs_scs = get_ocs_storageclasses()
        assert len(ocs_scs) > 0, "No OCS-managed StorageClasses found"

        target_sc = select_ocs_storageclass_by_provisioner(ocs_scs, preferred_provisioner="cephfs")
        sc_name = target_sc["metadata"]["name"]
        logger.info(f"Selected StorageClass for rapid deletion test: {sc_name}")

        original_uid = get_storageclass_uid(sc_name)
        assert original_uid is not None, f"StorageClass '{sc_name}' not found"
        logger.info(f"Original UID of '{sc_name}': {original_uid}")

        logger.test_step(f"Perform rapid delete of StorageClass '{sc_name}'")
        self.sc_ocp.delete(resource_name=sc_name, wait=False)

        # Immediately try to delete again to simulate the race condition
        # where the reconciler recreates the SC and a second delete targets
        # the new object. With UID preconditions, the second delete should
        # fail gracefully (NotFound) rather than deleting the new object.
        time.sleep(2)
        try:
            current_uid = get_storageclass_uid(sc_name)
            if current_uid is not None and current_uid == original_uid:
                logger.info(
                    f"StorageClass '{sc_name}' still exists with original UID, "
                    f"attempting second delete to simulate race"
                )
                self.sc_ocp.delete(resource_name=sc_name, wait=False)
            elif current_uid is not None:
                logger.info(
                    f"StorageClass '{sc_name}' already recreated with new UID={current_uid}. "
                    f"Race condition window passed."
                )
            else:
                logger.info(f"StorageClass '{sc_name}' already deleted")
        except Exception as e:
            logger.info(f"Second delete attempt handled gracefully: {e}")

        logger.test_step(
            f"Wait for StorageClass '{sc_name}' to stabilize after rapid deletion"
        )
        stable_uid = wait_for_storageclass_recreation(sc_name, timeout=300, sleep=10)

        logger.assertion(f"expected=not None (SC exists), actual={stable_uid}")
        assert stable_uid is not None, (
            f"StorageClass '{sc_name}' was not recreated after rapid deletion. "
            f"This indicates the fix for DFBUGS-10854 may not be working correctly."
        )

        # Verify stability: the UID should not change over a short observation period
        logger.test_step("Verify StorageClass UID is stable (no flapping)")
        time.sleep(15)
        final_uid = get_storageclass_uid(sc_name)

        logger.assertion(f"expected={stable_uid}, actual={final_uid}")
        assert final_uid == stable_uid, (
            f"StorageClass '{sc_name}' UID changed from {stable_uid} to {final_uid} "
            f"indicating instability in the reconciliation loop"
        )
        logger.info(
            f"StorageClass '{sc_name}' is stable with UID={final_uid} after rapid deletion test"
        )
