import logging

from ocs_ci.framework.pytest_customization.marks import (
    skipif_ibm_cloud_managed,
    skipif_managed_service,
    black_squad,
    polarion_id,
    tier3,
    bugzilla,
    skipif_ocs_version,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.base_ui import PageNavigator

logger = logging.getLogger(__name__)


@tier3
@black_squad
@skipif_ibm_cloud_managed
@skipif_managed_service
@skipif_ocs_version("<4.13")
class TestErrorMessageImprovements(ManageTest):
    @bugzilla("2193109")
    @polarion_id("OCS-4865")
    def test_backing_store_creation_rules(self, setup_ui_class):
        """
        Test to verify error rules for the name when creating a new backing store
            No more than 43 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            A unique name for the BackingStore within the project
        """
        backing_store_tab = (
            PageNavigator().nav_odf_default_page().nav_backing_store_tab()
        )
        backing_store_tab.proceed_resource_creation()
        backing_store_tab.check_error_messages()

    @bugzilla("2193109")
    @polarion_id("OCS-4867")
    def test_obc_creation_rules(self, setup_ui_class):
        """
        Test to verify error rules for the name when creating a new object bucket claim
            No more than 253 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            Cannot be used before
        """
        object_bucket_claim_create_tab = (
            PageNavigator().nav_odf_default_page().navigate_object_bucket_claims_page()
        )
        object_bucket_claim_create_tab.proceed_resource_creation()
        object_bucket_claim_create_tab.check_error_messages()

    @bugzilla("2193109")
    @polarion_id("OCS-4869")
    def test_bucket_class_creation_rules(self, setup_ui_class):
        """
        Test to verify error rules for the name when creating a new bucket class
            3-63 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            Avoid using the form of an IP address
            Cannot be used before
        """
        bucket_class_create_tab = (
            PageNavigator().nav_odf_default_page().nav_bucket_class_tab()
        )
        bucket_class_create_tab.proceed_resource_creation()
        bucket_class_create_tab.check_error_messages()

    @bugzilla("2193109")
    @polarion_id("OCS-4871")
    def test_namespace_store_creation_rules(
        self, cld_mgr, namespace_store_factory, setup_ui_class
    ):
        """
        Test to verify error rules for the name when creating a new namespace store
            No more than 43 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            A unique name for the NamespaceStore within the project

        * check_error_messages function requires 1 existing namespacestore as pre-condition for checking rule
        'A unique name for the NamespaceStore within the project'
        """
        existing_namespace_store_names = OCP().exec_oc_cmd(
            "get namespacestore --all-namespaces -o custom-columns=':metadata.name'"
        )
        if not existing_namespace_store_names:
            logger.info("Create namespace resource")
            nss_tup = ("oc", {"aws": [(1, "us-east-2")]})
            namespace_store_factory(*nss_tup)

        namespace_store_tab = (
            PageNavigator().nav_odf_default_page().nav_namespace_store_tab()
        )
        namespace_store_tab.proceed_resource_creation()
        namespace_store_tab.check_error_messages()

    @bugzilla("2193109")
    @polarion_id("OCS-4873")
    def test_blocking_pool_creation_rules(self, setup_ui_class):
        """
        Test to verify error rules for the name when creating a new blocking pool
            No more than 253 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            Cannot be used before
        """
        blocking_pool_tab = (
            PageNavigator()
            .nav_odf_default_page()
            .nav_storage_systems_tab()
            .nav_storagecluster_storagesystem_details()
            .nav_ceph_blockpool()
        )
        blocking_pool_tab.proceed_resource_creation()
        blocking_pool_tab.check_error_messages()

    @bugzilla("2193109")
    @polarion_id("OCS-4875")
    def test_storage_class_creation_rules(self, setup_ui_class):
        """
        Test to verify error rules for the name when creating a new storage class
            No more than 253 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            Cannot be used before
        """
        storage_systems_tab = (
            PageNavigator().nav_odf_default_page().nav_storage_systems_tab()
        )
        storage_systems_tab.proceed_resource_creation()
        storage_systems_tab.fill_backing_storage_form(
            "Use an existing StorageClass", "Next"
        )
        storage_systems_tab.check_error_messages()
