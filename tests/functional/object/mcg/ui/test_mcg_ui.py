import logging
import time

from ocs_ci.framework.logger_helper import log_step
from ocs_ci.framework.pytest_customization.marks import (
    on_prem_platform_required,
    black_squad,
    runs_on_provider,
    mcg,
    skipif_ibm_cloud_managed,
    provider_mode,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_unique_resource_name

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_disconnected_cluster,
    tier1,
    tier2,
    skipif_ui_not_support,
    ui,
    post_upgrade,
)
from ocs_ci.ocs.exceptions import IncorrectUiOptionRequested
from ocs_ci.ocs.ocp import OCP, get_all_resource_names_of_a_kind
from ocs_ci.ocs.ui.mcg_ui import BucketClassUI
from ocs_ci.ocs.ui.page_objects.object_bucket_claims_tab import (
    ObjectBucketClaimsTab,
)
from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.scale_noobaa_lib import fetch_noobaa_storage_class_name
from ocs_ci.ocs.bucket_utils import wait_for_bucket_count_stability


logger = logging.getLogger(__name__)


@mcg
@black_squad
@runs_on_provider
class TestStoreUserInterface(object):
    """
    Test the MCG store UI

    """

    def teardown(self):
        for store_kind in ["namespacestore", "backingstore"]:
            test_stores = [
                store_name
                for store_name in get_all_resource_names_of_a_kind(store_kind)
                if f"{store_kind}-ui" in store_name
            ]
            for store_name in test_stores:
                OCP(
                    kind=store_kind, namespace=config.ENV_DATA["cluster_namespace"]
                ).delete(resource_name=store_name)

    @ui
    @tier1
    @runs_on_provider
    @skipif_disconnected_cluster
    @skipif_ibm_cloud_managed
    @pytest.mark.parametrize(
        argnames=["kind", "provider", "region"],
        argvalues=[
            pytest.param(
                *["BackingStore", "aws", "us-east-2"],
                marks=pytest.mark.polarion_id("OCS-2549"),
            ),
            pytest.param(
                *["NamespaceStore", "aws", "us-east-2"],
                marks=pytest.mark.polarion_id("OCS-2547"),
            ),
        ],
    )
    def test_store_creation_and_deletion(
        self,
        setup_ui_class_factory,
        cld_mgr,
        cloud_uls_factory,
        kind,
        provider,
        region,
    ):
        """
        Test creation and deletion of MCG stores via the UI

        Steps:
        1. Navigate to Data Foundation / Object Storage / (Backing Store | Namespace Store)
        2. Create store with given parameters
        3. Verify via UI that status of the store is ready
        4. Delete resource via UI
        5. Verify store has been deleted via 'oc' cmd

        """
        setup_ui_class_factory()

        log_step(
            "Navigate to Data Foundation / Object Storage / (Backing Store | Namespace Store)"
        )
        object_storage = PageNavigator().nav_object_storage_page()

        if kind == "BackingStore":
            store_tab = object_storage.nav_backing_store_tab()
        elif kind == "NamespaceStore":
            store_tab = object_storage.nav_namespace_store_tab()
        else:
            raise IncorrectUiOptionRequested(f"Unknown store kind {kind}")

        log_step("Create store with given parameters")
        uls_name = list(cloud_uls_factory({provider: [(1, region)]})[provider])[0]
        store_name = create_unique_resource_name(
            resource_description="ui", resource_type=kind.lower()
        )

        resource_page, store_ready = store_tab.create_store_verify_state(
            kind=kind,
            store_name=store_name,
            provider=provider,
            region=region,
            secret=cld_mgr.aws_client.secret.name,
            uls_name=uls_name,
        )
        assert (
            store_ready
        ), f"Created kind='{kind}' name='{store_name}' was not ready in time"

        log_step("Delete resource via UI")
        store_tab = resource_page.nav_resource_list_via_breadcrumbs()
        store_tab.delete_resource(delete_via="three_dots", resource=store_name)

        log_step("Verify store has been deleted via 'oc' cmd")
        test_store = OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=kind,
            resource_name=store_name,
        )
        assert test_store.check_resource_existence(
            should_exist=False
        ), f"resource kind='{kind}' name='{store_name}' preserved on cluster after deletion"


@mcg
@black_squad
@ui
@runs_on_provider
@skipif_ui_not_support("bucketclass")
@tier1
@skipif_ocs_version("!=4.8")
@skipif_disconnected_cluster
class TestBucketclassUserInterface(object):
    """
    Test the bucketclass UI

    """

    def teardown(self):
        bc_lst = get_all_resource_names_of_a_kind("bucketclass")
        test_bucketclasses = [
            bc_name for bc_name in bc_lst if "bucketclass-ui" in bc_name
        ]
        for bc_name in test_bucketclasses:
            OCP(
                kind="bucketclass", namespace=config.ENV_DATA["cluster_namespace"]
            ).delete(resource_name=bc_name)

    @pytest.mark.parametrize(
        argnames=["policy", "bs_amount"],
        argvalues=[
            pytest.param(
                *["spread", 2],
                marks=pytest.mark.polarion_id("OCS-2548"),
            ),
            pytest.param(
                *["mirror", 2],
                marks=pytest.mark.polarion_id("OCS-2543"),
            ),
        ],
    )
    def test_standard_bc_creation_and_deletion(
        self,
        setup_ui_class,
        backingstore_factory,
        policy,
        bs_amount,
    ):
        """
        Test creation and deletion of a BS via the UI

        """
        test_stores = backingstore_factory("oc", {"aws": [(bs_amount, "us-east-2")]})

        bc_name = create_unique_resource_name(
            resource_description="ui", resource_type="bucketclass"
        )

        bc_ui_obj = BucketClassUI()
        bc_ui_obj.create_standard_bucketclass_ui(
            bc_name, policy, [bs.name for bs in test_stores]
        )

        # TODO: replace with ResourcePage().verify_current_page_resource_status(...)
        assert bc_ui_obj.verify_current_page_resource_status(
            constants.STATUS_READY
        ), "Created bucketclass was not ready in time"

        logger.info(f"Delete {bc_name}")
        bc_ui_obj.delete_bucketclass_ui(bc_name)

        test_bc = OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind="bucketclass",
            resource_name=bc_name,
        )

        assert test_bc.check_resource_existence(should_exist=False)

    @pytest.mark.parametrize(
        argnames=["policy", "amount"],
        argvalues=[
            pytest.param(
                *["single", 1],
                marks=pytest.mark.polarion_id("OCS-2544"),
            ),
            pytest.param(
                *["multi", 2],
                marks=pytest.mark.polarion_id("OCS-2545"),
            ),
            pytest.param(
                *["cache", 1],
                marks=pytest.mark.polarion_id("OCS-2546"),
            ),
        ],
    )
    def test_namespace_bc_creation_and_deletion(
        self,
        setup_ui_class,
        backingstore_factory,
        namespace_store_factory,
        policy,
        amount,
    ):
        """
        Test creation and deletion of a bucketclass via the UI

        """
        nss_names = [
            nss.name
            for nss in namespace_store_factory("oc", {"aws": [(amount, "us-east-2")]})
        ]

        bs_names = []
        if policy == "cache":
            bs_names = [
                bs.name
                for bs in backingstore_factory("oc", {"aws": [(amount, "us-east-2")]})
            ]

        bc_name = create_unique_resource_name(
            resource_description="ui", resource_type="bucketclass"
        )

        bc_ui_obj = BucketClassUI()
        bc_ui_obj.create_namespace_bucketclass_ui(bc_name, policy, nss_names, bs_names)

        # TODO: replace with ResourcePage().verify_current_page_resource_status(...)
        assert bc_ui_obj.verify_current_page_resource_status(
            constants.STATUS_READY
        ), "Created bucketclass was not ready in time"

        logger.info(f"Delete {bc_name}")
        bc_ui_obj.delete_bucketclass_ui(bc_name)

        test_bc = OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind="bucketclass",
            resource_name=bc_name,
        )

        assert test_bc.check_resource_existence(should_exist=False)


def generate_test_params():
    """
    Generate test parameters for the test_obc_creation_and_deletion - helper function to reuse fixture in parametrize
    """

    noobaa_sc = fetch_noobaa_storage_class_name().decode("utf-8")
    return [
        pytest.param(
            *[
                noobaa_sc,
                "noobaa-default-bucket-class",
                "three_dots",
                True,
            ],
            marks=[pytest.mark.polarion_id("OCS-4698"), mcg],
        ),
        pytest.param(
            *[
                noobaa_sc,
                "noobaa-default-bucket-class",
                "Actions",
                True,
            ],
            marks=[pytest.mark.polarion_id("OCS-2542"), mcg],
        ),
        pytest.param(
            *[
                "ocs-storagecluster-ceph-rgw",
                None,
                "three_dots",
                True,
            ],
            marks=[pytest.mark.polarion_id("OCS-4845"), on_prem_platform_required],
        ),
    ]


@skipif_disconnected_cluster
@black_squad
@runs_on_provider
@skipif_ui_not_support("obc")
class TestObcUserInterface(object):
    """
    Test the object bucket claim UI

    """

    def teardown(self):
        obc_lst = get_all_resource_names_of_a_kind("obc")
        test_obcs = [
            obc_name
            for obc_name in obc_lst
            if ("obc-testing" or "test-bucket-") in obc_name
        ]
        for obc_name in test_obcs:
            OCP(kind="obc", namespace=config.ENV_DATA["cluster_namespace"]).delete(
                resource_name=obc_name
            )

    @pytest.mark.parametrize(
        argnames=["storageclass", "bucketclass", "delete_via", "verify_ob_removal"],
        argvalues=generate_test_params(),
    )
    @provider_mode
    @ui
    @tier1
    @runs_on_provider
    def test_obc_creation_and_deletion(
        self,
        setup_ui_class_factory,
        storageclass,
        bucketclass,
        delete_via,
        verify_ob_removal,
    ):
        """
        Test creation and deletion of an OBC via the UI

        The test covers BZ #2097772 Introduce tooltips for contextual information
        The test covers BZ #2175685 RGW OBC creation via the UI is blocked by "Address form errors to proceed"
        """
        setup_ui_class_factory()

        obc_name = create_unique_resource_name(
            resource_description="ui", resource_type="obc"
        )

        obc_ui_obj = ObjectBucketClaimsTab()

        if (
            config.DEPLOYMENT["external_mode"]
            and storageclass == "ocs-storagecluster-ceph-rgw"
        ):
            storageclass = "ocs-external-storagecluster-ceph-rgw"
        obc_page = obc_ui_obj.create_obc_ui(obc_name, storageclass, bucketclass)

        assert obc_page.verify_current_page_resource_status(
            constants.STATUS_BOUND
        ), "Created OBC was not ready in time"

        test_obc = OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind="obc",
            resource_name=obc_name,
        )

        test_obc_obj = test_obc.get()

        obc_storageclass = test_obc_obj.get("spec").get("storageClassName")
        assert (
            obc_storageclass == storageclass
        ), f"StorageClass mismatch. Expected: {storageclass}, found: {obc_storageclass}"

        # no Bucket Classes available for ocs-storagecluster-ceph-rgw Storage Class
        if bucketclass:
            obc_bucketclass = (
                test_obc_obj.get("spec").get("additionalConfig").get("bucketclass")
            )
            assert (
                obc_bucketclass == bucketclass
            ), f"BucketClass mismatch. Expected: {bucketclass}, found: {obc_bucketclass}"

        # covers BZ 2097772
        if verify_ob_removal:
            BucketsTab().delete_bucket_ui(
                delete_via="three_dots", expect_fail=True, resource_name=obc_name
            )

        logger.info(f"Delete {obc_name}")
        obc_ui_obj.delete_obc_ui(obc_name, delete_via)

        assert test_obc.check_resource_existence(should_exist=False)


@ui
@black_squad
class TestBucketCreate:
    @tier1
    @post_upgrade
    @pytest.mark.polarion_id("OCS-6334")
    def test_bucket_create(self, setup_ui_class_factory):
        """
        Test bucket creation functionality in UI.

        Creates both OBC and S3 buckets, then creates a folder in one of them.
        Verifies basic bucket and folder creation workflows through the UI.

        """
        setup_ui_class_factory()
        bucket_ui = BucketsTab()
        bucket_ui.nav_object_storage_page()
        assert bucket_ui.create_bucket_ui("obc"), "Failed to create OBC bucket"
        time.sleep(15)
        bucket_ui.nav_object_storage_page()
        assert bucket_ui.create_bucket_ui("s3"), "Failed to create S3 bucket"
        time.sleep(15)
        bucket_ui.nav_object_storage_page()
        assert (
            bucket_ui.create_folder_in_bucket()
        ), "Failed to create and upload folder in bucket"

    @post_upgrade
    @tier2
    @pytest.mark.polarion_id("OCS-6397")
    def test_empty_bucket_delete(self, setup_ui_class_factory):
        """
        Test bucket deletion functionality in UI.

        Steps:
        1. Navigate to the Object Storage Buckets page
        2. Create a new bucket with a simple name for deletion testing
        3. Delete the bucket using the three_dots menu option
        4. Verify the bucket was deleted successfully

        """
        setup_ui_class_factory()
        bucket_ui = BucketsTab()

        logger.info("Creating a new bucket with a simple name for deletion testing")
        bucket_ui.nav_object_storage_page()

        bucket_name = "s3"
        bucket_ui.create_bucket_ui(bucket_name)
        time.sleep(10)

        bucket_ui.nav_object_storage_page()
        bucket_ui.nav_buckets_page()

        buckets = bucket_ui.get_buckets_list()
        logger.info(f"Found {len(buckets)} buckets")

        bucket_to_delete = None
        for bucket in buckets:
            if bucket.startswith("test-bucket-s3-"):
                bucket_to_delete = bucket
                break

        assert bucket_to_delete is not None, "Could not find the test bucket to delete"
        logger.info(f"Selected bucket for deletion: {bucket_to_delete}")

        bucket_ui.delete_bucket_ui(
            delete_via="three_dots", expect_fail=False, resource_name=bucket_to_delete
        )

        bucket_ui.nav_object_storage_page()
        bucket_ui.nav_buckets_page()

        updated_buckets = bucket_ui.get_buckets_list()
        assert (
            bucket_to_delete not in updated_buckets
        ), f"Bucket {bucket_to_delete} was not deleted successfully"
        logger.info(f"Successfully deleted bucket: {bucket_to_delete}")

    @pytest.mark.polarion_id("OCS-6398")
    @tier2
    def test_bucket_list_comparison(self, setup_ui_class_factory, mcg_obj):
        """
        Test that the bucket list from UI matches the bucket list from CLI.

        Args:
            setup_ui_class_factory (any): UI setup fixture.
            mcg_obj (any): MCG CLI client fixture assumed to have a method list_buckets() returning list.

        Returns:
            None

        Raises:
            AssertionError: If UI bucket count doesn't match CLI bucket count.
        """
        setup_ui_class_factory()
        bucket_ui = BucketsTab()
        bucket_ui.nav_object_storage_page()

        # Create test buckets via UI to ensure comparison is meaningful
        bucket_ui.create_bucket_ui("s3")
        bucket_ui.nav_object_storage_page()
        bucket_ui.create_bucket_ui("obc")

        bucket_ui.nav_buckets_page()

        # Get CLI bucket list for comparison
        cli_buckets = list(mcg_obj.cli_list_all_buckets())
        logger.info(f"CLI bucket count: {len(cli_buckets)}")

        all_ui_buckets = []

        first_page_buckets = bucket_ui.get_buckets_list()
        all_ui_buckets.extend(first_page_buckets)
        logger.info(f"First page bucket count: {len(first_page_buckets)}")

        if bucket_ui.has_pagination_controls() and len(first_page_buckets) == 100:
            logger.info("Pagination detected, collecting buckets from additional pages")

            while bucket_ui.navigate_to_next_page():
                next_page_buckets = bucket_ui.get_buckets_list()
                logger.info(f"Additional page bucket count: {len(next_page_buckets)}")
                all_ui_buckets.extend(next_page_buckets)

                if len(all_ui_buckets) >= len(cli_buckets):
                    break

        logger.info(f"Total UI bucket count across all pages: {len(all_ui_buckets)}")
        logger.info(f"CLI bucket count: {len(cli_buckets)}")

        ui_bucket_set = set(all_ui_buckets)
        cli_bucket_set = set(cli_buckets)

        missing_in_ui = cli_bucket_set - ui_bucket_set
        if missing_in_ui:
            logger.warning(f"Buckets in CLI but missing in UI: {missing_in_ui}")

        missing_in_cli = ui_bucket_set - cli_bucket_set
        if missing_in_cli:
            logger.warning(f"Buckets in UI but missing in CLI: {missing_in_cli}")

        assert (
            len(all_ui_buckets) >= 2
        ), "Expected at least 2 buckets (OBC and S3) but found less"

        assert len(all_ui_buckets) == len(
            cli_buckets
        ), f"UI bucket count ({len(all_ui_buckets)}) does not match CLI bucket count ({len(cli_buckets)})"

    @ui
    @tier2
    @black_squad
    @pytest.mark.polarion_id("OCS-6399")
    def test_bucket_pagination(self, setup_ui_class_factory, mcg_obj):
        """
        Test bucket pagination functionality in UI.

        Steps:
        1. Navigate to the Object Storage Buckets page
        2. Check initial bucket count via CLI and wait for stability
        3. Create additional buckets if needed to exceed 100 total buckets
        4. Verify pagination controls appear with >100 buckets
        5. Collect buckets listed on first page (should be 100)
        6. Navigate to next page using pagination controls
        7. Collect buckets on second page and verify they differ from first page
        8. Navigate back to previous page using pagination controls
        9. Verify the returned page matches the original first page
        """
        setup_ui_class_factory()
        bucket_ui = BucketsTab()
        bucket_ui.nav_object_storage_page()
        bucket_ui.nav_buckets_page()

        # Get initial bucket count and wait for stability
        initial_count, _ = wait_for_bucket_count_stability(mcg_obj)
        logger.info(f"Initial stable bucket count: {initial_count}")

        # Determine if we need to create more buckets
        buckets_needed = max(0, 101 - initial_count)
        if buckets_needed > 0:
            logger.info(
                f"Creating {buckets_needed} additional buckets to test pagination"
            )

            s3_buckets_to_create = buckets_needed // 2
            obc_buckets_to_create = buckets_needed - s3_buckets_to_create

            bucket_ui.create_multiple_buckets_ui(
                s3_buckets=s3_buckets_to_create, obc_buckets=obc_buckets_to_create
            )

            bucket_ui.nav_object_storage_page()
            bucket_ui.nav_buckets_page()

            # Wait for bucket count to stabilize after creation
            final_count, reached_expected = wait_for_bucket_count_stability(
                mcg_obj, expected_count=101
            )
            logger.info(f"Final bucket count after creation: {final_count}")

            assert reached_expected, (
                f"Failed to create enough buckets for pagination. "
                f"Current count: {final_count}, Expected: 101"
            )

        assert (
            bucket_ui.has_pagination_controls()
        ), "Pagination controls not found despite having >100 buckets"
        logger.info("Pagination controls are present")

        first_page_buckets = bucket_ui.get_buckets_list()
        assert (
            len(first_page_buckets) == 100
        ), f"Expected 100 buckets on first page but found {len(first_page_buckets)}"
        logger.info(f"First page has {len(first_page_buckets)} buckets")

        assert bucket_ui.navigate_to_next_page(), "Failed to navigate to next page"
        logger.info("Successfully navigated to the second page")

        second_page_buckets = bucket_ui.get_buckets_list()
        logger.info(f"Second page has {len(second_page_buckets)} buckets")

        assert (
            len(second_page_buckets) > 0
        ), "Second page should have at least one bucket"

        first_page_bucket_ids = set(first_page_buckets)
        second_page_bucket_ids = set(second_page_buckets)
        assert not first_page_bucket_ids.intersection(
            second_page_bucket_ids
        ), "Found duplicate buckets between pages"

        assert (
            bucket_ui.navigate_to_previous_page()
        ), "Failed to navigate back to previous page"
        logger.info("Successfully navigated back to the first page")

        returned_first_page_buckets = bucket_ui.get_buckets_list()
        returned_page_bucket_ids = set(returned_first_page_buckets)
        assert (
            returned_page_bucket_ids == first_page_bucket_ids
        ), "Returned to first page but buckets don't match"

        logger.info("Pagination test completed successfully")
