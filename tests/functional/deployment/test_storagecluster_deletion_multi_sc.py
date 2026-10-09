import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    multi_storagecluster_required,
)
from ocs_ci.framework.testlib import (
    ManageTest,
    brown_squad,
    skipif_ocs_version,
    tier4,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import ceph_health_check_multi_storagecluster_external

logger = logging.getLogger(__name__)


@brown_squad
@multi_storagecluster_required
class TestStorageClusterDeletionGuardMultiSC(ManageTest):
    """
    Verify StorageCluster deletion-guard webhook behaviour on a
    multi-storagecluster deployment (internal SC in openshift-storage,
    external SC in openshift-storage-extended).

    This test is destructive — it deletes the external StorageCluster.
    It must run last in any test suite it is collected with.
    """

    @tier4
    @skipif_ocs_version("<4.23")
    @pytest.mark.polarion_id("OCS-XXXX")
    @pytest.mark.order("last")
    def test_storagecluster_deletion_guard_multi_sc(self, request):
        """
        Verify that the ValidatingAdmissionPolicy blocks StorageCluster
        deletion without the confirm-deletion annotation on both the
        internal and external StorageClusters, that annotating only the
        external SC allows it to be deleted while the internal SC remains
        unaffected, and that the external RHCS Ceph cluster is healthy
        after the external SC is removed.

        Steps:
            1. Verify neither SC has the confirm-deletion annotation.
            2. Attempt to delete the internal SC — expect webhook denial.
            3. Attempt to delete the external SC — expect webhook denial.
            4. Set confirm-deletion=true on the external SC only.
            5. Delete the external SC and confirm it is gone.
            6. Verify the internal SC is still present and in Ready phase.
            7. Attempt to delete the internal SC again — expect denial.
            8. Verify the external RHCS Ceph cluster is healthy.
        """
        internal_ns = config.ENV_DATA["cluster_namespace"]
        internal_sc_name = constants.DEFAULT_CLUSTERNAME
        external_ns = config.ENV_DATA["external_storage_cluster_namespace"]
        external_sc_name = config.ENV_DATA["external_storage_cluster_name"]

        internal_sc = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            resource_name=internal_sc_name,
            namespace=internal_ns,
        )
        external_sc = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            resource_name=external_sc_name,
            namespace=external_ns,
        )

        internal_original = (
            internal_sc.get()
            .get("metadata", {})
            .get("annotations", {})
            .get(constants.CONFIRM_DELETION_ANNOTATION)
        )
        external_original = (
            external_sc.get()
            .get("metadata", {})
            .get("annotations", {})
            .get(constants.CONFIRM_DELETION_ANNOTATION)
        )

        def restore_internal_annotation():
            if internal_original is not None:
                logger.info("Restoring confirm-deletion annotation on internal SC")
                patch = (
                    f'{{"metadata":{{"annotations":'
                    f'{{"{constants.CONFIRM_DELETION_ANNOTATION}":'
                    f'"{internal_original}"}}}}}}'
                )
                internal_sc.patch(
                    resource_name=internal_sc_name,
                    params=patch,
                    format_type="merge",
                )
            else:
                logger.info("Removing confirm-deletion annotation from internal SC")
                internal_sc.exec_oc_cmd(
                    f"annotate storagecluster {internal_sc_name}"
                    f" -n {internal_ns}"
                    f" {constants.CONFIRM_DELETION_ANNOTATION}-"
                )

        def restore_external_annotation():
            if not external_sc.is_exist(resource_name=external_sc_name):
                logger.info("External SC no longer exists; skipping annotation restore")
                return
            if external_original is not None:
                logger.info("Restoring confirm-deletion annotation on external SC")
                patch = (
                    f'{{"metadata":{{"annotations":'
                    f'{{"{constants.CONFIRM_DELETION_ANNOTATION}":'
                    f'"{external_original}"}}}}}}'
                )
                external_sc.patch(
                    resource_name=external_sc_name,
                    params=patch,
                    format_type="merge",
                )
            else:
                logger.info("Removing confirm-deletion annotation from external SC")
                external_sc.exec_oc_cmd(
                    f"annotate storagecluster {external_sc_name}"
                    f" -n {external_ns}"
                    f" {constants.CONFIRM_DELETION_ANNOTATION}-"
                )

        request.addfinalizer(restore_internal_annotation)
        request.addfinalizer(restore_external_annotation)

        logger.test_step("Verify confirm-deletion annotation is absent on both SCs")
        for sc_obj, sc_name, label in (
            (internal_sc, internal_sc_name, "internal"),
            (external_sc, external_sc_name, "external"),
        ):
            annotations = sc_obj.get().get("metadata", {}).get("annotations", {})
            logger.assertion(f"No confirm-deletion annotation on {label} SC")
            assert constants.CONFIRM_DELETION_ANNOTATION not in annotations, (
                f"Expected no confirm-deletion annotation on {label} SC, "
                f"found: {annotations.get(constants.CONFIRM_DELETION_ANNOTATION)}"
            )

        logger.test_step(
            "Attempt to delete internal SC without confirm-deletion annotation"
        )
        with pytest.raises(CommandFailed) as exc_info:
            internal_sc.delete(resource_name=internal_sc_name)
        logger.assertion("Webhook blocked internal SC deletion")
        assert "StorageCluster deletion is IRREVERSIBLE" in str(
            exc_info.value
        ), f"Unexpected error message: {exc_info.value}"
        logger.assertion("Internal SC still exists after blocked deletion")
        assert internal_sc.is_exist(resource_name=internal_sc_name)

        logger.test_step(
            "Attempt to delete external SC without confirm-deletion annotation"
        )
        with pytest.raises(CommandFailed) as exc_info:
            external_sc.delete(resource_name=external_sc_name)
        logger.assertion("Webhook blocked external SC deletion")
        assert "StorageCluster deletion is IRREVERSIBLE" in str(
            exc_info.value
        ), f"Unexpected error message: {exc_info.value}"
        logger.assertion("External SC still exists after blocked deletion")
        assert external_sc.is_exist(resource_name=external_sc_name)

        logger.test_step("Annotating external SC with confirm-deletion=true")
        confirm_annotation = (
            f'{{"metadata":{{"annotations":'
            f'{{"{constants.CONFIRM_DELETION_ANNOTATION}":"true"}}}}}}'
        )
        result = external_sc.patch(
            resource_name=external_sc_name,
            params=confirm_annotation,
            format_type="merge",
        )
        if not result:
            raise CommandFailed(
                f"Setting confirm-deletion annotation on "
                f"'{external_sc_name}' failed"
            )

        logger.test_step("Deleting external SC")
        external_sc.delete(resource_name=external_sc_name, wait=True, timeout=300)
        logger.assertion("External SC no longer exists")
        assert not external_sc.is_exist(
            resource_name=external_sc_name
        ), "External SC should not exist after deletion"

        logger.test_step("Verify internal SC is unaffected after external SC deletion")
        logger.assertion("Internal SC still exists")
        assert internal_sc.is_exist(
            resource_name=internal_sc_name
        ), "Internal SC should still exist after external SC was deleted"
        sc_data = internal_sc.get()
        logger.assertion("Internal SC phase is Ready")
        assert sc_data.get("status", {}).get("phase") == "Ready", (
            f"Internal SC phase is not Ready: "
            f"{sc_data.get('status', {}).get('phase')}"
        )

        logger.test_step("Attempt to delete internal SC (annotation still absent)")
        with pytest.raises(CommandFailed) as exc_info:
            internal_sc.delete(resource_name=internal_sc_name)
        logger.assertion("Webhook still blocks internal SC deletion")
        assert "StorageCluster deletion is IRREVERSIBLE" in str(
            exc_info.value
        ), f"Unexpected error message: {exc_info.value}"

        logger.test_step(
            "Verify external RHCS Ceph cluster is healthy after " "external SC deletion"
        )
        ceph_health_check_multi_storagecluster_external()
