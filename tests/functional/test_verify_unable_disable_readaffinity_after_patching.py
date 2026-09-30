import json
import logging
import pytest
import time

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    jira,
    skipif_ocs_version,
)
from ocs_ci.framework.testlib import ManageTest, tier4
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.storage_cluster import (
    get_storage_cluster_name,
    get_read_affinity_enabled,
    get_read_affinity_spec,
    get_read_affinity_from_cluster_config,
    patch_read_affinity,
    wait_for_read_affinity_state,
)

logger = logging.getLogger(__name__)

TIMEOUT_CONFIGMAP_UPDATE = 300
POLL_INTERVAL = 10


@green_squad
@tier4
@jira("DFBUGS-9067")
@skipif_ocs_version("<4.22")
class TestDisableReadAffinityAfterPatching(ManageTest):
    """
    Test to verify that ReadAffinity can be disabled after patching
    the StorageCluster CR.

    Verifies fix for DFBUGS-9067: Unable to disable the ReadAffinity
    after patching StorageCluster CR.
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, request):
        """
        Setup and teardown fixture.

        Captures the original ReadAffinity state from the StorageCluster CR
        and restores it after the test completes.

        Returns:
            None
        """
        self.namespace = config.ENV_DATA["cluster_namespace"]
        self.storage_cluster_name = get_storage_cluster_name(namespace=self.namespace)

        self.storage_cluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            namespace=self.namespace,
        )

        original_exists, original_ra_spec = get_read_affinity_spec(
            self.storage_cluster_name, namespace=self.namespace
        )
        original_enabled = original_ra_spec.get("enabled", None)

        logger.info(
            f"Original ReadAffinity state: exists={original_exists}, "
            f"enabled={original_enabled}"
        )

        def finalizer():
            """Restore the original ReadAffinity state on the StorageCluster CR."""
            logger.info("Restoring original ReadAffinity state on StorageCluster CR")
            try:
                if original_exists and original_enabled is not None:
                    patch_read_affinity(
                        self.storage_cluster_name,
                        enabled=original_enabled,
                        namespace=self.namespace,
                    )
                    logger.info(f"Restored ReadAffinity enabled to {original_enabled}")
                elif not original_exists:
                    remove_patch = '[{"op": "remove", "path": "/spec/csi/readAffinity"}]'
                    try:
                        self.storage_cluster_obj.patch(
                            resource_name=self.storage_cluster_name,
                            params=remove_patch,
                            format_type="json",
                        )
                        logger.info("Removed ReadAffinity spec (restored to original absent state)")
                    except CommandFailed:
                        logger.warning("ReadAffinity spec already absent, nothing to remove")
            except Exception as e:
                logger.warning(f"Failed to restore original ReadAffinity state: {e}")

            # Wait for the configmap to settle after restoration
            time.sleep(30)

        request.addfinalizer(finalizer)

    def test_disable_read_affinity_after_patching(self):
        """
        Test that ReadAffinity can be disabled after patching StorageCluster CR.

        Verifies fix for DFBUGS-9067.

        Steps:
        1. Enable ReadAffinity on the StorageCluster CR via patch.
        2. Verify ReadAffinity is enabled in the StorageCluster spec.
        3. Wait for the enabled state to propagate to CephConnection/ConfigMap.
        4. Disable ReadAffinity on the StorageCluster CR via patch.
        5. Verify ReadAffinity is disabled in the StorageCluster spec.
        6. Wait for the disabled state to propagate to CephConnection/ConfigMap.
        7. Confirm that ReadAffinity is truly disabled and not stale.
        """
        logger.test_step("Enable ReadAffinity on the StorageCluster CR")
        patch_result = patch_read_affinity(
            self.storage_cluster_name, enabled=True, namespace=self.namespace
        )
        logger.assertion(f"expected=patch success, actual={patch_result}")
        assert patch_result, "Failed to patch StorageCluster to enable ReadAffinity"

        logger.test_step("Verify ReadAffinity is enabled in StorageCluster spec")
        ra_enabled = get_read_affinity_enabled(
            self.storage_cluster_name, namespace=self.namespace
        )
        logger.assertion(f"expected=True, actual={ra_enabled}")
        assert ra_enabled is True, (
            f"ReadAffinity should be enabled in StorageCluster spec, got {ra_enabled}"
        )

        logger.test_step("Wait for ReadAffinity enabled state to propagate to cluster config")
        propagated = wait_for_read_affinity_state(
            expected_enabled=True,
            namespace=self.namespace,
            timeout=TIMEOUT_CONFIGMAP_UPDATE,
            poll_interval=POLL_INTERVAL,
        )
        logger.assertion(f"expected=propagated True, actual={propagated}")
        assert propagated, (
            "ReadAffinity enabled=True did not propagate to CephConnection/ConfigMap "
            f"within {TIMEOUT_CONFIGMAP_UPDATE}s"
        )

        logger.test_step("Disable ReadAffinity on the StorageCluster CR")
        patch_result = patch_read_affinity(
            self.storage_cluster_name, enabled=False, namespace=self.namespace
        )
        logger.assertion(f"expected=patch success, actual={patch_result}")
        assert patch_result, "Failed to patch StorageCluster to disable ReadAffinity"

        logger.test_step("Verify ReadAffinity is disabled in StorageCluster spec")
        ra_enabled = get_read_affinity_enabled(
            self.storage_cluster_name, namespace=self.namespace
        )
        logger.assertion(f"expected=False, actual={ra_enabled}")
        assert ra_enabled is False, (
            f"ReadAffinity should be disabled in StorageCluster spec, got {ra_enabled}"
        )

        logger.test_step("Wait for ReadAffinity disabled state to propagate to cluster config")
        propagated = wait_for_read_affinity_state(
            expected_enabled=False,
            namespace=self.namespace,
            timeout=TIMEOUT_CONFIGMAP_UPDATE,
            poll_interval=POLL_INTERVAL,
        )
        logger.assertion(f"expected=propagated True, actual={propagated}")
        assert propagated, (
            "ReadAffinity enabled=False did not propagate to CephConnection/ConfigMap "
            f"within {TIMEOUT_CONFIGMAP_UPDATE}s. "
            "This indicates the bug DFBUGS-9067 is not fixed."
        )

        logger.test_step("Confirm ReadAffinity is truly disabled in final state")
        final_ra = get_read_affinity_from_cluster_config(namespace=self.namespace)
        if final_ra is not None:
            final_enabled = final_ra.get("enabled", None)
            logger.assertion(f"expected=False, actual={final_enabled}")
            assert final_enabled is False, (
                f"ReadAffinity should be disabled in final config state, "
                f"but got enabled={final_enabled}. Bug DFBUGS-9067 may not be fixed."
            )
        else:
            logger.info(
                "ReadAffinity is absent from cluster config, which is consistent with disabled state"
            )

        logger.info("ReadAffinity was successfully disabled after patching StorageCluster CR")
