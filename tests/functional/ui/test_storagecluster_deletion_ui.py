import json
import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    runs_on_provider,
    skipif_managed_service,
    skipif_ocs_version,
    tier1,
    ui,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)

# Substring from the ValidatingAdmissionPolicy denial message that must appear
# in the UI danger alert when deletion is attempted with confirm-deletion=false.
_WEBHOOK_ERROR_FRAGMENT = "StorageCluster deletion is IRREVERSIBLE"


@ui
@black_squad
@runs_on_provider
@skipif_managed_service
@skipif_ocs_version("<4.23")
class TestStorageClusterDeletionProtectionUI(ManageTest):
    """
    Verify the ODF Console deletion-protection flow for StorageCluster CR
    (RHSTOR-8282 / OCS-8173).

    The ValidatingAdmissionPolicy 'storagecluster-delete-protection' blocks
    any DELETE request unless the StorageCluster carries the annotation
    ``uninstall.ocs.openshift.io/confirm-deletion=true``.

    Test flow (non-destructive):
    1. Set confirm-deletion=false via the UI Edit Annotations dialog.
    2. Attempt deletion via Actions → Delete StorageCluster.
    3. Assert the webhook blocks the request and the error is surfaced in the UI.
    4. Close the dialog — StorageCluster is never deleted.
    """

    @tier1
    @pytest.mark.polarion_id("OCS-8294")
    def test_storagecluster_delete_annotation_protection(self, setup_ui_class, request):
        """
        Verify the ValidatingAdmissionPolicy blocks StorageCluster deletion
        when confirm-deletion=false, and that the error is surfaced clearly
        in the OCP Console UI dialog.

        The StorageCluster is never deleted — the dialog is closed after
        the error is asserted.

        Teardown (two-branch):
        - If annotation existed before test → restore original value via patch.
        - If annotation did not exist before test → strip via oc annotate -.

        Steps:
        1. Build OCP handle and record the pre-test annotation value.
        2. Register two-branch teardown finalizer.
        3. Navigate to Storage → Data Foundation → StorageCluster detail page.
        4. Open Actions → Edit Annotations:
           - If annotation absent → Add new row with confirm-deletion=false.
           - If annotation present → Edit existing row, set value to false.
        5. Open Actions → Delete StorageCluster.
        6. Click Delete in the confirmation dialog.
        7. Assert a danger alert appears containing the webhook error message.
        8. Assert the error message instructs the user to set confirm-deletion=true.
        9. Close the dialog — StorageCluster is not deleted.
        """
        ns_name = config.ENV_DATA["cluster_namespace"]
        sc_name = constants.DEFAULT_CLUSTERNAME

        # Step 1 — OCP handle + record original annotation value
        logger.info("Build OCP handle and record pre-test annotation value")
        storage_cluster = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            resource_name=sc_name,
            namespace=ns_name,
        )
        annotations = storage_cluster.get().get("metadata", {}).get("annotations", {})
        original_value = annotations.get(constants.CONFIRM_DELETION_ANNOTATION)

        # Step 2 — Two-branch teardown
        def restore_annotation():
            if original_value is not None:
                logger.info(
                    "Teardown: restoring confirm-deletion annotation to original value"
                )
                patch = json.dumps(
                    {
                        "metadata": {
                            "annotations": {
                                constants.CONFIRM_DELETION_ANNOTATION: original_value
                            }
                        }
                    }
                )
                storage_cluster.patch(
                    resource_name=sc_name,
                    params=patch,
                    format_type="merge",
                )
            else:
                logger.info(
                    "Teardown: removing confirm-deletion annotation "
                    "(annotation did not exist before test)"
                )
                storage_cluster.exec_oc_cmd(
                    f"annotate storagecluster {sc_name} -n {ns_name} "
                    f"{constants.CONFIRM_DELETION_ANNOTATION}-"
                )

        request.addfinalizer(restore_annotation)

        # Step 3 — Navigate to StorageCluster detail page
        logger.test_step(
            "Navigate to Storage → Data Foundation → StorageCluster detail page"
        )
        storage_cluster_page = PageNavigator().nav_storage_cluster_default_page()

        # Step 4 — Set confirm-deletion=false via Edit Annotations in the UI
        if original_value is None:
            logger.test_step(
                "Annotation absent: open Actions → Edit Annotations → "
                "add confirm-deletion=false"
            )
        else:
            logger.test_step(
                "Annotation present: open Actions → Edit Annotations → "
                "edit confirm-deletion value to false"
            )
        storage_cluster_page.set_storagecluster_annotation(
            key=constants.CONFIRM_DELETION_ANNOTATION,
            value="false",
        )

        # Step 5 — Open Actions → Delete StorageCluster
        logger.test_step("Open Actions menu and click Delete StorageCluster")
        confirm_dialog = storage_cluster_page.initiate_storagecluster_delete()

        # Step 6 — Click Delete in the confirmation dialog
        # (confirm-deletion=false → webhook must block the DELETE)
        logger.test_step(
            "Click Delete in confirmation dialog "
            "(webhook expected to block — confirm-deletion=false)"
        )
        logger.info("Click Delete button in the Delete StorageCluster dialog")
        confirm_dialog.dialog_confirm()

        try:
            # Step 7 — Assert danger alert with webhook error text is visible
            logger.test_step(
                "Assert danger alert is visible in dialog with webhook error message"
            )
            logger.info(
                f"Waiting for danger alert containing: '{_WEBHOOK_ERROR_FRAGMENT}'"
            )
            alert_element = confirm_dialog.wait_for_element_to_be_visible(
                confirm_dialog.attach_storage_loc["annotation_alert_description"],
                timeout=30,
            )
            assert alert_element is not None, (
                "Expected a danger alert to appear in the Delete dialog after the "
                "admission webhook blocked the DELETE request, but no alert was found."
            )
            alert_text = alert_element.text
            logger.info(f"Danger alert text: {alert_text!r}")
            assert _WEBHOOK_ERROR_FRAGMENT in alert_text, (
                f"Danger alert is present but does not contain the expected webhook "
                f"error message.\n"
                f"Expected fragment: {_WEBHOOK_ERROR_FRAGMENT!r}\n"
                f"Actual alert text: {alert_text!r}"
            )

            # Step 8 — Assert error instructs user to set confirm-deletion=true
            logger.test_step(
                "Assert error message instructs user to set confirm-deletion=true"
            )
            expected_instruction = f"{constants.CONFIRM_DELETION_ANNOTATION}=true"
            assert expected_instruction in alert_text, (
                f"Expected error message to instruct the user to set "
                f"'{expected_instruction}', but got: {alert_text!r}"
            )
        finally:
            # Step 9 — Close the dialog (StorageCluster is never deleted)
            # Always runs regardless of assertion outcome to prevent dialog leaking.
            logger.test_step("Close the dialog — StorageCluster is not deleted")
            logger.info(
                "Click Cancel button to dismiss the Delete StorageCluster dialog"
            )
            confirm_dialog.dialog_cancel()
