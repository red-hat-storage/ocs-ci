import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    jira,
    skipif_ocs_version,
    provider_client_ms_platform_required,
)
from ocs_ci.framework.testlib import ManageTest, tier4
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import (
    get_storage_consumer_name,
    get_storage_consumer_quota,
    set_storage_consumer_quota,
    get_storage_client_cluster_resource_quotas,
    verify_clusterrole_has_verb_for_resource,
    check_operator_logs_for_error,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@green_squad
@tier4
@jira("DFBUGS-9023")
@skipif_ocs_version("<4.22")
@provider_client_ms_platform_required
class TestClientClusterQuotaMovedToUnlimited(ManageTest):
    """
    Test to verify DFBUGS-9023: When client cluster quota is moved to unlimited
    from custom, the clusterresourcequota deletion succeeds.

    The bug was caused by the ocs-client-operator-controller-manager service account
    missing the 'delete' verb for clusterresourcequotas in its ClusterRole RBAC.
    When a storage quota was removed (set to unlimited), the operator could not
    delete the corresponding ClusterResourceQuota object.

    The fix adds the 'delete' verb to the ClusterRole for clusterresourcequotas.
    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """
        Setup fixture to initialize OCP objects and store state for cleanup.

        Returns:
            None
        """
        self.client_index = None
        self.provider_index = None
        self.original_quota = None
        self.storage_consumer_name = None

        client_indices = config.get_consumer_indexes_list()
        if not client_indices:
            pytest.skip("No client clusters found - provider/client setup required")

        self.client_index = client_indices[0]
        self.provider_index = config.get_provider_index()

        def finalizer():
            """
            Cleanup: Restore original quota settings if they were modified.
            """
            if self.original_quota and self.storage_consumer_name:
                try:
                    logger.info(
                        f"Restoring original quota for storage consumer "
                        f"{self.storage_consumer_name}"
                    )
                    set_storage_consumer_quota(
                        self.provider_index,
                        self.storage_consumer_name,
                        self.original_quota,
                    )
                    logger.info(
                        f"Restored quota to {self.original_quota} for "
                        f"{self.storage_consumer_name}"
                    )
                except Exception as e:
                    logger.warning(f"Failed to restore quota during cleanup: {e}")

        request.addfinalizer(finalizer)

    def test_client_quota_moved_to_unlimited_deletes_crq(self):
        """
        Test that when client cluster quota is moved from custom to unlimited,
        the ClusterResourceQuota is successfully deleted.

        Verifies fix for DFBUGS-9023: The ocs-client-operator ClusterRole now
        includes the 'delete' verb for clusterresourcequotas, allowing the
        operator to remove the ClusterResourceQuota when quota is set to unlimited.

        Steps:
        1. Verify the ocs-client-operator ClusterRole has 'delete' verb for
           clusterresourcequotas (RBAC fix verification)
        2. Get the StorageConsumer on the provider and record current quota
        3. Set a custom storage quota on the provider for the client
        4. Verify ClusterResourceQuota is created on the client cluster
        5. Set the quota to unlimited (0) on the provider
        6. Verify ClusterResourceQuota is deleted on the client cluster
        7. Verify no delete permission errors in operator logs
        """
        logger.test_step(
            "Verify ocs-client-operator ClusterRole has 'delete' verb for "
            "clusterresourcequotas"
        )
        has_delete = verify_clusterrole_has_verb_for_resource(
            cluster_index=self.client_index,
            service_account_name=constants.OCS_CLIENT_OPERATOR_SA,
            api_group=constants.CLUSTER_RESOURCE_QUOTA_API_GROUP,
            resource=constants.CLUSTER_RESOURCE_QUOTA_RESOURCE,
            verb="delete",
        )
        logger.assertion(
            f"expected='delete' verb present in ClusterRole, actual={has_delete}"
        )
        assert has_delete, (
            "The ocs-client-operator ClusterRole is missing the 'delete' verb for "
            "clusterresourcequotas. The fix for DFBUGS-9023 has not been applied."
        )

        logger.test_step(
            "Get StorageConsumer on provider and record current quota"
        )
        self.storage_consumer_name = get_storage_consumer_name(self.provider_index)
        self.original_quota = get_storage_consumer_quota(
            self.provider_index, self.storage_consumer_name
        )
        logger.info(
            f"StorageConsumer: {self.storage_consumer_name}, "
            f"original quota: {self.original_quota}"
        )

        logger.test_step("Set a custom storage quota (50 GiB) on the provider")
        custom_quota_gib = 50
        set_storage_consumer_quota(
            self.provider_index, self.storage_consumer_name, custom_quota_gib
        )

        logger.test_step(
            "Wait for ClusterResourceQuota to be created on the client cluster"
        )
        crq_found = False
        for sample in TimeoutSampler(
            timeout=300,
            sleep=15,
            func=get_storage_client_cluster_resource_quotas,
            cluster_index=self.client_index,
        ):
            if sample:
                crq_found = True
                crq_names = [crq["metadata"]["name"] for crq in sample]
                logger.info(
                    f"ClusterResourceQuota(s) found on client: {crq_names}"
                )
                break

        logger.assertion(
            f"expected=ClusterResourceQuota created on client, actual found={crq_found}"
        )
        assert crq_found, (
            "ClusterResourceQuota was not created on the client cluster after "
            f"setting quota to {custom_quota_gib} GiB"
        )

        logger.test_step(
            "Set the quota to unlimited (0) on the provider to trigger CRQ deletion"
        )
        set_storage_consumer_quota(self.provider_index, self.storage_consumer_name, 0)

        logger.test_step(
            "Verify ClusterResourceQuota is deleted on the client cluster"
        )
        crq_deleted = False
        for sample in TimeoutSampler(
            timeout=300,
            sleep=15,
            func=get_storage_client_cluster_resource_quotas,
            cluster_index=self.client_index,
        ):
            if not sample:
                crq_deleted = True
                logger.info(
                    "ClusterResourceQuota successfully deleted on client cluster"
                )
                break

        logger.assertion(
            f"expected=ClusterResourceQuota deleted (empty list), actual deleted={crq_deleted}"
        )
        assert crq_deleted, (
            "ClusterResourceQuota was NOT deleted on the client cluster after "
            "setting quota to unlimited. This indicates DFBUGS-9023 is not fixed."
        )

        logger.test_step(
            "Verify no delete permission errors in ocs-client-operator logs"
        )
        has_delete_error = check_operator_logs_for_error(
            cluster_index=self.client_index,
            operator_name=constants.OCS_CLIENT_OPERATOR_SA,
            error_string='cannot delete resource "clusterresourcequotas"',
        )
        logger.assertion(
            f"expected=no delete permission errors in logs, "
            f"actual error_found={has_delete_error}"
        )
        assert not has_delete_error, (
            "Found 'cannot delete resource clusterresourcequotas' error in "
            "ocs-client-operator logs. The RBAC fix for DFBUGS-9023 may not "
            "be fully applied."
        )

        logger.info(
            "DFBUGS-9023 verification complete: ClusterResourceQuota was "
            "successfully deleted when quota was moved to unlimited"
        )

    def test_rbac_delete_verb_for_clusterresourcequotas(self):
        """
        Verify that the ocs-client-operator ClusterRole includes the 'delete'
        verb for clusterresourcequotas resource.

        This is a direct verification of the code fix for DFBUGS-9023 which
        added the 'delete' verb to the RBAC configuration.
        """
        logger.test_step(
            "Check ocs-client-operator ClusterRole for 'delete' verb on "
            "clusterresourcequotas"
        )
        has_delete = verify_clusterrole_has_verb_for_resource(
            cluster_index=self.client_index,
            service_account_name=constants.OCS_CLIENT_OPERATOR_SA,
            api_group=constants.CLUSTER_RESOURCE_QUOTA_API_GROUP,
            resource=constants.CLUSTER_RESOURCE_QUOTA_RESOURCE,
            verb="delete",
        )
        logger.assertion(
            f"expected='delete' verb in ClusterRole rules for "
            f"clusterresourcequotas, actual present={has_delete}"
        )
        assert has_delete, (
            "The ocs-client-operator ClusterRole does not include 'delete' verb "
            "for clusterresourcequotas in API group quota.openshift.io. "
            "This is the root cause of DFBUGS-9023."
        )
        logger.info(
            "Confirmed: 'delete' verb is present in ClusterRole for "
            "clusterresourcequotas - DFBUGS-9023 fix verified at RBAC level"
        )
