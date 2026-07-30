import logging

import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    brown_squad,
    skipif_ocs_version,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework import config

logger = logging.getLogger(__name__)

CONFIRM_DELETION_ANNOTATION = "uninstall.ocs.openshift.io/confirm-deletion"


@brown_squad
class TestStorageClusterDeletionGuard(ManageTest):
    """
    Verify that a ValidatingAdmissionPolicy prevents StorageCluster
    deletion unless the confirm-deletion annotation is set.
    """

    @tier1
    @skipif_ocs_version("<5.0")
    # @pytest.mark.polarion_id("OCS-XXXX") - to be added once polarion test case is created
    def test_storagecluster_delete_blocked_without_annotation(self, request):
        """
        Attempt to delete the StorageCluster without the
        confirm-deletion annotation and verify the request
        is rejected by the ValidatingAdmissionPolicy.
        """
        ns_name = config.ENV_DATA["cluster_namespace"]
        sc_name = constants.DEFAULT_CLUSTERNAME

        storage_cluster = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            resource_name=sc_name,
            namespace=ns_name,
        )

        annotations = storage_cluster.get().get("metadata", {}).get("annotations", {})
        original_value = annotations.get(CONFIRM_DELETION_ANNOTATION)

        def restore_annotation():
            if original_value is not None:
                logger.info("Restoring confirm-deletion annotation")
                patch = (
                    f'{{"metadata":{{"annotations":'
                    f'{{"{CONFIRM_DELETION_ANNOTATION}":"{original_value}"}}}}}}'
                )
                storage_cluster.patch(
                    resource_name=sc_name,
                    params=patch,
                    format_type="merge",
                )
            else:
                logger.info("Removing confirm-deletion annotation")
                storage_cluster.exec_oc_cmd(
                    f"annotate storagecluster {sc_name} -n {ns_name} "
                    f"{CONFIRM_DELETION_ANNOTATION}-"
                )

        request.addfinalizer(restore_annotation)

        if original_value == "true":
            logger.info("Removing confirm-deletion annotation before test")
            storage_cluster.exec_oc_cmd(
                f"annotate storagecluster {sc_name} -n {ns_name} "
                f"{CONFIRM_DELETION_ANNOTATION}-"
            )

        logger.info(
            "Attempting to delete StorageCluster without confirm-deletion annotation"
        )
        with pytest.raises(CommandFailed) as exc_info:
            storage_cluster.delete(resource_name=sc_name)

        error_msg = str(exc_info.value)
        assert (
            "StorageCluster deletion is IRREVERSIBLE" in error_msg
        ), f"Expected deletion warning message, got: {error_msg}"
        logger.info(
            "StorageCluster deletion correctly blocked by ValidatingAdmissionPolicy"
        )

        # Verify StorageCluster still exists
        assert storage_cluster.is_exist(
            resource_name=sc_name
        ), "StorageCluster should still exist after blocked deletion"
