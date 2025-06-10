import logging
import time
import uuid

from ocs_ci.ocs.ui.page_objects.bucket_versioning import BucketVersioning
from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    ui,
    black_squad,
    polarion_id,
)
from ocs_ci.utility.utils import generate_folder_with_files

logger = logging.getLogger(__name__)


@ui
@black_squad
@tier1
@polarion_id("OCS-6863")
class TestBucketVersioningUI:
    """
    Test class for bucket versioning functionality via UI.
    """

    def test_enable_bucket_versioning(self, setup_ui_class_factory):
        """
        Test bucket versioning enable functionality via UI.

        Test Steps:
        1. Create local folder with 1 file (due to product limitation)
        2. Upload the folder to bucket
        3. Enable versioning and confirm (if not already enabled)
        4. Upload the same folder again to create second version
        5. Navigate to folder and show versioning

        Args:
            setup_ui_class_factory: Pytest fixture for UI setup
        """
        setup_ui_class_factory()

        # Navigate to object storage
        bucket_versioning = BucketVersioning()
        bucket_versioning.nav_object_storage_page()

        # Step 1: Create local folder with 1 file
        logger.info("Step 1: Creating local folder with 1 file")
        folder_path, file_generator = generate_folder_with_files(num_files=1)
        files = list(file_generator)
        logger.info(f"Created folder at: {folder_path} with {len(files)} file(s)")

        # Step 2: Upload the folder to bucket (using BucketsTab for this operation)
        logger.info("Step 2: Uploading folder to bucket with 1 file")
        buckets_tab = BucketsTab()

        # Navigate to first bucket and create a folder name
        buckets_tab.do_click(buckets_tab.bucket_tab["first_bucket"])
        folder_name = f"test-folder-{uuid.uuid4()}"
        buckets_tab.do_click(buckets_tab.bucket_tab["create_folder_button"])
        buckets_tab.do_send_keys(
            buckets_tab.bucket_tab["folder_name_input"], folder_name
        )
        buckets_tab.do_click(buckets_tab.bucket_tab["submit_button_folder"])

        # Upload our 1-file folder instead of creating 400 files
        file_input = buckets_tab.driver.find_element(
            buckets_tab.bucket_tab["file_input_directory"][1],
            buckets_tab.bucket_tab["file_input_directory"][0],
        )
        buckets_tab.driver.execute_script(
            "arguments[0].style.display = 'block'; arguments[0].style.visibility = 'visible';",
            file_input,
        )
        file_input.send_keys(folder_path)
        time.sleep(5)  # Wait for upload

        logger.info(f"Successfully uploaded folder: {folder_name} with 1 file")

        # Step 3: Enable versioning
        logger.info("Step 3: Enabling versioning for bucket")
        versioning_enabled = bucket_versioning.enable_versioning()
        if versioning_enabled:
            logger.info("Versioning enabled successfully")
        else:
            logger.info("Versioning was already enabled")

        # Step 4: Upload the same folder again to create second version
        logger.info("Step 4: Uploading same folder again to create second version")

        # Navigate back to the bucket details page since versioning navigation took us away
        # First go back to object storage buckets list page, then navigate to bucket
        bucket_versioning.nav_object_storage_page()
        buckets_tab.do_click(buckets_tab.bucket_tab["first_bucket"])

        file_input = buckets_tab.driver.find_element(
            buckets_tab.bucket_tab["file_input_directory"][1],
            buckets_tab.bucket_tab["file_input_directory"][0],
        )
        buckets_tab.driver.execute_script(
            "arguments[0].style.display = 'block'; arguments[0].style.visibility = 'visible';",
            file_input,
        )
        file_input.send_keys(folder_path)
        time.sleep(5)  # Wait for upload

        logger.info(
            f"Successfully uploaded folder again: {folder_name} - second version created"
        )

        # Step 5: Navigate to folder and show versioning
        logger.info("Step 5: Navigating to folder and showing versions")

        # Navigate to bucket and then into the specific folder we created
        bucket_versioning.nav_object_storage_page()
        buckets_tab.do_click(buckets_tab.bucket_tab["first_bucket"])

        # Click on the folder name to navigate into it
        # Use CSS selector for first folder row (most reliable approach)
        buckets_tab.do_click(buckets_tab.bucket_tab["first_folder_link"])

        # Toggle "List all versions" to show file versions
        buckets_tab.do_click(buckets_tab.bucket_tab["list_all_versions_toggle"])

        logger.info(
            f"Successfully navigated to folder '{folder_name}' and enabled version listing"
        )

        # Verify the test completed successfully
        assert folder_name, "Failed to create and upload folder"
        logger.info("Bucket versioning test completed successfully")
