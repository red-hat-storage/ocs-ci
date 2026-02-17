import logging
import time

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    rgw,
    runs_on_provider,
)
from ocs_ci.framework.testlib import (
    tier1,
    tier2,
    ui,
    post_upgrade,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_rgw_pods
from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab


logger = logging.getLogger(__name__)


@ui
@rgw
@black_squad
@runs_on_provider
class TestRGWBucketUI:
    """
    Test RGW bucket operations via the Object Browser UI.
    Requires ODF 4.19+ with RGW provider support.
    """

    @tier1
    @post_upgrade
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_navigate_to_rgw_object_browser(self, setup_ui_class_factory):
        """
        Verify navigation to the RGW object browser and bucket list loading.

        Steps:
        1. Navigate to Object Storage page
        2. Verify RGW provider card is available and enabled
        3. Select RGW provider
        4. Verify bucket list loads

        """
        if not get_rgw_pods():
            pytest.skip("No RGW pods running on this cluster")
        setup_ui_class_factory()
        bucket_ui = BucketsTab()
        obj_storage = bucket_ui.nav_object_storage_page()

        if not obj_storage.is_rgw_provider_available():
            pytest.skip("RGW provider card is not available or is disabled")

        obj_storage.select_storage_provider(constants.S3_PROVIDER_RGW_INTERNAL)
        bucket_ui.page_has_loaded(retries=15)

        buckets = bucket_ui.get_buckets_list()
        logger.info(f"RGW bucket list loaded with {len(buckets)} buckets")

    @tier1
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_create_rgw_bucket(self, setup_ui_class_factory):
        """
        Create an S3 bucket via the RGW provider.

        Steps:
        1. Navigate to Object Storage page, select RGW provider
        2. Click 'Create bucket', select S3 tile
        3. Fill bucket name, submit
        4. Verify bucket appears in the RGW bucket list

        """
        if not get_rgw_pods():
            pytest.skip("No RGW pods running on this cluster")
        setup_ui_class_factory()
        bucket_ui = BucketsTab()
        bucket_ui.navigate_buckets_page(provider=constants.S3_PROVIDER_RGW_INTERNAL)

        _, bucket_name = bucket_ui.create_bucket_ui_with_details(method="s3")
        logger.info(f"Created RGW bucket: {bucket_name}")
        time.sleep(10)

        bucket_ui.navigate_buckets_page(provider=constants.S3_PROVIDER_RGW_INTERNAL)
        buckets = bucket_ui.get_buckets_list()
        assert (
            bucket_name in buckets
        ), f"Bucket '{bucket_name}' not found in RGW bucket list: {buckets}"

    @tier2
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_list_rgw_buckets(self, setup_ui_class_factory):
        """
        Verify RGW buckets are displayed correctly.

        Steps:
        1. Navigate to RGW bucket list
        2. Create a test bucket if none exist
        3. Verify bucket list is non-empty
        4. Verify bucket names are displayed

        """
        if not get_rgw_pods():
            pytest.skip("No RGW pods running on this cluster")
        setup_ui_class_factory()
        bucket_ui = BucketsTab()
        bucket_ui.navigate_buckets_page(provider=constants.S3_PROVIDER_RGW_INTERNAL)

        buckets = bucket_ui.get_buckets_list()
        if not buckets:
            _, bucket_name = bucket_ui.create_bucket_ui_with_details(method="s3")
            logger.info(f"Created test bucket: {bucket_name}")
            time.sleep(10)
            bucket_ui.navigate_buckets_page(provider=constants.S3_PROVIDER_RGW_INTERNAL)
            buckets = bucket_ui.get_buckets_list()

        assert len(buckets) > 0, "Expected at least 1 RGW bucket"
        logger.info(f"Found {len(buckets)} RGW buckets: {buckets}")

    @tier2
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_delete_rgw_bucket(self, setup_ui_class_factory):
        """
        Delete an RGW bucket via the UI.

        Steps:
        1. Navigate to RGW bucket list, create a test bucket
        2. Delete the bucket using three_dots menu
        3. Verify bucket no longer appears in the list

        """
        if not get_rgw_pods():
            pytest.skip("No RGW pods running on this cluster")
        setup_ui_class_factory()
        bucket_ui = BucketsTab()
        bucket_ui.navigate_buckets_page(provider=constants.S3_PROVIDER_RGW_INTERNAL)

        _, bucket_name = bucket_ui.create_bucket_ui_with_details(method="s3")
        logger.info(f"Created bucket for deletion test: {bucket_name}")
        time.sleep(10)

        bucket_ui.delete_bucket_ui(
            delete_via="three_dots",
            expect_fail=False,
            resource_name=bucket_name,
            provider=constants.S3_PROVIDER_RGW_INTERNAL,
        )

        bucket_ui.navigate_buckets_page(provider=constants.S3_PROVIDER_RGW_INTERNAL)
        updated_buckets = bucket_ui.get_buckets_list()
        assert (
            bucket_name not in updated_buckets
        ), f"Bucket '{bucket_name}' still present after deletion"
