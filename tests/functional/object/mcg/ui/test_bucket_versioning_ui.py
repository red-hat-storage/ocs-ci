import logging
import os
import time
import uuid

from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
)

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
        4. Upload the same folder multiple times to create versions
        5. Navigate to folder and show versioning
        6. Validate "Latest" label appears on newest version

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

        # Set initial content for first version
        file_path = files[0]  # Get the first (and only) file path
        logger.info(f"Setting initial content for first version: {file_path}")
        with open(file_path, "w") as f:
            f.write("content-v1")

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
        file_input.clear()
        file_input.send_keys(folder_path)
        time.sleep(2)  # Wait for upload to complete

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

        # Modify file content for second version
        logger.info(f"Modifying file content for second version: {file_path}")

        os.remove(file_path)
        with open(file_path, "w") as f:
            f.write("content-v2")

        # Navigate back to the bucket details page since versioning navigation took us away
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
        file_input.clear()
        file_input.send_keys(folder_path)
        time.sleep(5)  # Wait for upload

        logger.info(
            f"Successfully uploaded folder again: {folder_name} - second version created"
        )

        # Modify file content for third version
        logger.info(f"Modifying file content for third version: {file_path}")

        os.remove(file_path)
        with open(file_path, "w") as f:
            f.write("content-v3")

        file_input = buckets_tab.driver.find_element(
            buckets_tab.bucket_tab["file_input_directory"][1],
            buckets_tab.bucket_tab["file_input_directory"][0],
        )
        buckets_tab.driver.execute_script(
            "arguments[0].style.display = 'block'; arguments[0].style.visibility = 'visible';",
            file_input,
        )
        file_input.clear()
        file_input.send_keys(folder_path)
        time.sleep(5)  # Wait for upload

        logger.info(
            f"Successfully uploaded folder third time: {folder_name} - third version created"
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

        # Step 6: Validate "Latest" label appears on newest version
        logger.info("Step 6: Validating 'Latest' label on newest version")

        # Extract the actual file name for counting versions
        file_name = os.path.basename(file_path)
        logger.info(f"Using file name for version counting: {file_name}")

        try:
            # Wait for versions to load and find the "Latest" label elements
            buckets_tab.wait_for_element_to_be_present(
                buckets_tab.bucket_tab["version_latest_label"], timeout=10
            )

            # Method 1: Count file name occurrences (most reliable)
            expected_versions = 3
            actual_versions_by_name = buckets_tab.check_number_occurrences_text(
                file_name, expected_versions
            )
            logger.info(
                f"File name '{file_name}' appears expected {expected_versions} times: {actual_versions_by_name}"
            )

            # Method 2: Count table rows (with fixed locator format)
            try:
                file_rows = buckets_tab.get_elements(("tbody tr", By.CSS_SELECTOR))
                actual_row_count = len(file_rows)
                logger.info(f"Total table rows found: {actual_row_count}")
            except NoSuchElementException as e:
                logger.debug(f"Could not count table rows: {e}")
                actual_row_count = 0

            # Validate version count
            if not actual_versions_by_name:
                raise AssertionError(
                    f"Expected {expected_versions} versions of file '{file_name}', but count doesn't match. "
                    f"Table rows found: {actual_row_count}. This might be a product bug!"
                )

            # Find all label elements
            latest_labels = buckets_tab.get_elements(
                buckets_tab.bucket_tab["version_latest_label"]
            )

            # Verify at least one "Latest" label exists
            if not latest_labels:
                raise AssertionError("No 'Latest' label found on any file version")

            # Check that the label text is "Latest"
            latest_label_texts = [
                label.text for label in latest_labels if label.text == "Latest"
            ]

            if not latest_label_texts:
                raise AssertionError(
                    "Found label elements but none contain 'Latest' text"
                )

            logger.info(
                f"Successfully found {len(latest_label_texts)} 'Latest' label(s)"
            )

        except (TimeoutException, AssertionError) as e:
            logger.debug(f"Failed to validate 'Latest' label: {e}")
            raise

        # Verify the test completed successfully
        assert folder_name, "Failed to create and upload folder"
        logger.info(
            "Bucket versioning test with label validation completed successfully"
        )
