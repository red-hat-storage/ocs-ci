import logging
import os
import time
import uuid

from selenium.common.exceptions import TimeoutException

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

    # Test configuration constants
    EXPECTED_VERSIONS = 3
    UPLOAD_WAIT_TIME = 5
    NAVIGATION_WAIT_TIME = 2
    VERSIONS_LOAD_WAIT_TIME = 3
    INITIAL_UPLOAD_WAIT_TIME = 2
    VERSION_DETECTION_TIMEOUT = 10

    def _upload_folder_to_bucket(self, buckets_tab: BucketsTab, folder_path: str, wait_time: int = None) -> None:
        """
        Upload a folder to the bucket.

        Args:
            buckets_tab: BucketsTab instance for UI operations
            folder_path: Path to the folder to upload
            wait_time: Time to wait after upload (default: 2 seconds)
        """
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
        time.sleep(wait_time or self.INITIAL_UPLOAD_WAIT_TIME)

    def _modify_file_content(self, file_path: str, content: str) -> None:
        """
        Modify file content by removing and recreating with new content.

        Args:
            file_path: Path to the file to modify
            content: New content to write to the file
        """
        os.remove(file_path)
        with open(file_path, "w") as f:
            f.write(content)

    def _navigate_to_bucket(self, bucket_versioning: BucketVersioning, buckets_tab: BucketsTab) -> None:
        """
        Navigate to object storage and select the first bucket.

        Args:
            bucket_versioning: BucketVersioning instance for navigation
            buckets_tab: BucketsTab instance for UI operations
        """
        bucket_versioning.nav_object_storage_page()
        buckets_tab.do_click(buckets_tab.bucket_tab["first_bucket"])

    def _create_file_version(
        self,
        file_path: str,
        version_content: str,
        bucket_versioning: BucketVersioning,
        buckets_tab: BucketsTab,
        folder_path: str,
        folder_name: str,
        version_number: int,
    ) -> None:
        """
        Create a new version of a file by modifying content and uploading.

        Args:
            file_path: Path to the file to modify
            version_content: Content to write to the file
            bucket_versioning: BucketVersioning instance for navigation
            buckets_tab: BucketsTab instance for UI operations
            folder_path: Path to the folder containing the file
            folder_name: Name of the folder for logging
            version_number: Version number for logging
        """
        logger.info(f"Modifying file content for version {version_number}: {file_path}")
        self._modify_file_content(file_path, version_content)

        if version_number > 1:  # Only navigate back for versions after the first
            self._navigate_to_bucket(bucket_versioning, buckets_tab)

        self._upload_folder_to_bucket(buckets_tab, folder_path, wait_time=self.UPLOAD_WAIT_TIME)
        logger.info(f"Successfully uploaded folder: {folder_name} - version {version_number} created")

    def _get_actual_filename(self, buckets_tab: BucketsTab, original_filename: str) -> str:
        """
        Get the actual filename displayed in UI, handling cases where files are renamed during upload.

        Args:
            buckets_tab: BucketsTab instance for UI operations
            original_filename: Original filename before upload

        Returns:
            str: Actual filename displayed in the UI
        """
        txt_elements = buckets_tab.get_elements(buckets_tab.bucket_tab["txt_files"])

        if not txt_elements or not txt_elements[0].text:
            return original_filename

        actual_file_name = txt_elements[0].text.strip()
        if "\n" in actual_file_name:  # Handle "filename\nLatest" case
            actual_file_name = actual_file_name.split("\n")[0].strip()

        logger.info(f"Actual file name in UI: '{actual_file_name}' (original was '{original_filename}')")
        return actual_file_name

    def _setup_test_file(self) -> tuple[str, str]:
        """
        Create a local folder with a single file for testing.

        Returns:
            tuple[str, str]: (file_path, folder_path) - paths to the created file and its parent folder
        """
        logger.info("Creating local folder with 1 file")
        folder_path, file_generator = generate_folder_with_files(num_files=1)
        files = list(file_generator)
        file_path = files[0]  # Get the first (and only) file path

        # Set initial content for first version
        logger.info(f"Setting initial content for first version: {file_path}")
        with open(file_path, "w") as f:
            f.write("content-v1")

        logger.info(f"Created folder at: {folder_path} with {len(files)} file(s)")
        return file_path, folder_path

    def _navigate_to_folder_and_enable_versions(
        self,
        bucket_versioning: BucketVersioning,
        buckets_tab: BucketsTab,
        folder_name: str,
    ) -> None:
        """
        Navigate to the test folder and enable version listing.

        Args:
            bucket_versioning: BucketVersioning instance for navigation
            buckets_tab: BucketsTab instance for UI operations
            folder_name: Name of the folder to navigate to
        """
        logger.info("Navigating to folder and showing versions")

        # Navigate to bucket and then into the specific folder we created
        self._navigate_to_bucket(bucket_versioning, buckets_tab)

        # Click on the folder name to navigate into it
        logger.info(f"Clicking on folder link to navigate into folder: {folder_name}")
        buckets_tab.do_click(buckets_tab.bucket_tab["first_folder_link"])

        # Wait for navigation
        time.sleep(self.NAVIGATION_WAIT_TIME)

        # Toggle "List all versions" to show file versions
        logger.info("Clicking 'List all versions' toggle")
        buckets_tab.do_click(buckets_tab.bucket_tab["list_all_versions_toggle"])

        # Wait for versions to load after toggle
        time.sleep(self.VERSIONS_LOAD_WAIT_TIME)

        logger.info(f"Successfully navigated to folder '{folder_name}' and enabled version listing")

    def _create_test_folder(self, buckets_tab: BucketsTab, folder_path: str) -> str:
        """
        Create a test folder in the bucket and upload the initial file.

        Args:
            buckets_tab: BucketsTab instance for UI operations
            folder_path: Path to the local folder to upload

        Returns:
            str: Name of the created folder
        """
        logger.info("Uploading folder to bucket with 1 file")

        # Navigate to first bucket and create a folder name
        buckets_tab.do_click(buckets_tab.bucket_tab["first_bucket"])
        folder_name = f"test-folder-{uuid.uuid4()}"
        buckets_tab.do_click(buckets_tab.bucket_tab["create_folder_button"])
        buckets_tab.do_send_keys(buckets_tab.bucket_tab["folder_name_input"], folder_name)
        buckets_tab.do_click(buckets_tab.bucket_tab["submit_button_folder"])

        # Upload our 1-file folder
        self._upload_folder_to_bucket(buckets_tab, folder_path)

        logger.info(f"Successfully uploaded folder: {folder_name} with 1 file")
        return folder_name

    def _get_version_id_for_latest(self, buckets_tab: BucketsTab) -> str:
        """
        Get the version ID of the object marked with "Latest" tag.

        Args:
            buckets_tab: BucketsTab instance for UI operations

        Returns:
            str: Version ID of the latest object

        Raises:
            TimeoutException: If elements don't load within timeout
        """
        # Wait for the version ID element to be present
        buckets_tab.wait_for_element_to_be_present(
            buckets_tab.bucket_tab["version_id_for_latest"],
            timeout=self.VERSION_DETECTION_TIMEOUT,
        )

        # Get the version ID text from the cell
        version_id_elements = buckets_tab.get_elements(buckets_tab.bucket_tab["version_id_for_latest"])
        if not version_id_elements:
            raise TimeoutException("No version ID element found for latest object")
        version_id = version_id_elements[0].text.strip()
        logger.info(f"Version ID for latest object: {version_id}")

        return version_id

    def _delete_latest_version(self, buckets_tab: BucketsTab) -> None:
        """
        Delete the version marked with "Latest" tag.

        Args:
            buckets_tab: BucketsTab instance for UI operations

        Raises:
            TimeoutException: If elements don't load within timeout
        """
        logger.info("Clicking on 3 dots menu for latest version")
        buckets_tab.do_click(buckets_tab.bucket_tab["version_actions_menu_for_latest"])

        # Wait for dropdown to appear
        time.sleep(3)

        logger.info("Clicking 'Delete this version' option")
        buckets_tab.do_click(buckets_tab.bucket_tab["delete_this_version_option"])

        # Wait for delete confirmation modal
        buckets_tab.wait_for_element_to_be_present(buckets_tab.bucket_tab["delete_version_modal_title"], timeout=10)

        logger.info("Typing 'delete' in confirmation input")
        input_element = buckets_tab.get_elements(buckets_tab.bucket_tab["delete_version_input"])
        input_element[0].clear()
        input_element[0].send_keys("delete")

        # Wait for the button to become enabled
        time.sleep(2)

        logger.info("Clicking Delete object button to confirm")
        buckets_tab.do_click(buckets_tab.bucket_tab["delete_version_confirm_button"])

        # Wait for the deletion to complete
        time.sleep(3)
        logger.info("Version deletion completed")

    def _validate_file_versions(self, buckets_tab: BucketsTab, file_name: str) -> None:
        """
        Validate that the expected number of file versions exist and "Latest" labels are present.

        Args:
            buckets_tab: BucketsTab instance for UI operations
            file_name: Original filename to validate versions for

        Raises:
            AssertionError: If version count or Latest labels don't match expectations
            TimeoutException: If elements don't load within timeout
        """
        # Wait for versions to load and find the "Latest" label elements
        buckets_tab.wait_for_element_to_be_present(
            buckets_tab.bucket_tab["version_latest_label"],
            timeout=self.VERSION_DETECTION_TIMEOUT,
        )

        # Count file name occurrences (most reliable method)
        expected_versions = self.EXPECTED_VERSIONS

        # Get the actual filename displayed in UI (handles file renaming during upload)
        file_name_to_search = self._get_actual_filename(buckets_tab, file_name)

        file_name_locator = (
            buckets_tab.bucket_tab["file_name_text"][0].format(file_name_to_search),
            buckets_tab.bucket_tab["file_name_text"][1],
        )
        element_list = buckets_tab.get_elements(file_name_locator)
        actual_versions_by_name = len(element_list) == expected_versions
        logger.info(
            f"File name '{file_name_to_search}' appears expected {expected_versions} times: "
            f"{actual_versions_by_name} (found {len(element_list)} occurrences)"
        )

        # Validate version count
        if not actual_versions_by_name:
            raise AssertionError(
                f"Version count validation failed: Expected {expected_versions} versions of file '{file_name}', "
                f"but found {len(element_list)} occurrences"
            )

        # Find all label elements
        latest_labels = buckets_tab.get_elements(buckets_tab.bucket_tab["version_latest_label"])

        # Verify at least one "Latest" label exists
        if not latest_labels:
            raise AssertionError("Latest label validation failed: No 'Latest' label found on any file version")

        # Check that the label text is "Latest"
        latest_label_texts = [label.text for label in latest_labels if label.text == "Latest"]

        if not latest_label_texts:
            raise AssertionError("Latest label validation failed: Found label elements but none contain 'Latest' text")

        logger.info(f"Successfully found {len(latest_label_texts)} 'Latest' label(s)")

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
        file_path, folder_path = self._setup_test_file()

        # Step 2: Upload the folder to bucket (using BucketsTab for this operation)
        logger.info("Step 2: Uploading folder to bucket with 1 file")
        buckets_tab = BucketsTab()
        folder_name = self._create_test_folder(buckets_tab, folder_path)

        # Step 3: Enable versioning
        logger.info("Step 3: Enabling versioning for bucket")
        versioning_enabled = bucket_versioning.enable_versioning()
        if versioning_enabled:
            logger.info("Versioning enabled successfully")
        else:
            logger.info("Versioning was already enabled")

        # Step 4: Upload the same folder multiple times to create versions
        logger.info("Step 4: Creating multiple file versions")

        # Create second version
        self._create_file_version(
            file_path,
            "content-v2",
            bucket_versioning,
            buckets_tab,
            folder_path,
            folder_name,
            2,
        )

        # Create third version
        self._create_file_version(
            file_path,
            "content-v3",
            bucket_versioning,
            buckets_tab,
            folder_path,
            folder_name,
            3,
        )

        # Step 5: Navigate to folder and show versioning
        logger.info("Step 5: Navigating to folder and showing versions")
        self._navigate_to_folder_and_enable_versions(bucket_versioning, buckets_tab, folder_name)

        # Step 6: Validate "Latest" label appears on newest version
        logger.info("Step 6: Validating 'Latest' label on newest version")

        # Extract the actual file name for counting versions
        file_name = os.path.basename(file_path)
        logger.info(f"Using file name for version counting: {file_name}")

        try:
            self._validate_file_versions(buckets_tab, file_name)
        except (TimeoutException, AssertionError) as e:
            logger.debug(f"Failed to validate 'Latest' label: {e}")
            raise

        # Verify the test completed successfully
        assert folder_name, "Test setup validation failed: Failed to create and upload folder"
        logger.info("Bucket versioning test with label validation completed successfully")

    def test_bucket_versioning_with_modification(self, setup_ui_class_factory):
        """
        Test bucket versioning with object modification and version management.

        Test Steps:
        1. Upload an object to bucket
        2. Enable versioning for bucket (if not already enabled)
        3. Modify the object locally
        4. Re-upload the modified object
        5. Toggle "List all versions" to show versions
        6. Verify version ID mapping with "Latest" tag
        7. Delete latest object and verify new one becomes latest

        Args:
            setup_ui_class_factory: Pytest fixture for UI setup
        """
        setup_ui_class_factory()

        # Navigate to object storage
        bucket_versioning = BucketVersioning()
        bucket_versioning.nav_object_storage_page()

        # Step 1: Create and upload initial object
        logger.info("Step 1: Creating and uploading initial object")
        file_path, folder_path = self._setup_test_file()

        buckets_tab = BucketsTab()
        folder_name = self._create_test_folder(buckets_tab, folder_path)

        # Step 2: Enable versioning
        logger.info("Step 2: Enabling versioning for bucket")
        versioning_enabled = bucket_versioning.enable_versioning()
        if versioning_enabled:
            logger.info("Versioning enabled successfully")
        else:
            logger.info("Versioning was already enabled")

        # Step 3: Modify the object locally
        logger.info("Step 3: Modifying object locally")
        self._modify_file_content(file_path, "modified-content-v2")

        # Step 4: Re-upload the modified object
        logger.info("Step 4: Re-uploading modified object")
        self._navigate_to_bucket(bucket_versioning, buckets_tab)
        self._upload_folder_to_bucket(buckets_tab, folder_path, wait_time=self.UPLOAD_WAIT_TIME)

        # Create one more version for testing
        logger.info("Creating third version of the object")
        self._create_file_version(
            file_path,
            "modified-content-v3",
            bucket_versioning,
            buckets_tab,
            folder_path,
            folder_name,
            3,
        )

        # Step 5: Navigate to folder and toggle "List all versions"
        logger.info("Step 5: Navigating to folder and showing versions")
        self._navigate_to_folder_and_enable_versions(bucket_versioning, buckets_tab, folder_name)

        # Step 6: Verify version ID mapping with "Latest" tag
        logger.info("Step 6: Verifying version ID for latest object")

        # First validate versions exist
        file_name = os.path.basename(file_path)
        self._validate_file_versions(buckets_tab, file_name)

        # Get version ID for the latest object
        latest_version_id = self._get_version_id_for_latest(buckets_tab)
        assert latest_version_id, "Failed to get version ID for latest object"
        logger.info(f"Successfully retrieved version ID for latest object: {latest_version_id}")

        # Step 7: Delete latest object and verify new one becomes latest
        logger.info("Step 7: Deleting latest version and verifying new latest")

        # Store the current latest version ID before deletion
        original_latest_version_id = latest_version_id
        logger.info(f"Original latest version ID: {original_latest_version_id}")

        # Delete the latest version
        self._delete_latest_version(buckets_tab)

        # Wait for UI to refresh after deletion
        time.sleep(2)

        # Get the new latest version ID
        new_latest_version_id = self._get_version_id_for_latest(buckets_tab)
        logger.info(f"New latest version ID after deletion: {new_latest_version_id}")

        # Validate that a different version is now marked as latest
        assert new_latest_version_id != original_latest_version_id, (
            f"Latest version ID should have changed after deletion. "
            f"Original: {original_latest_version_id}, New: {new_latest_version_id}"
        )

        # Validate that we still have the correct number of versions (one less than before)
        expected_versions_after_deletion = self.EXPECTED_VERSIONS - 1

        # Update the expected versions temporarily for validation
        original_expected = self.EXPECTED_VERSIONS
        self.EXPECTED_VERSIONS = expected_versions_after_deletion

        try:
            self._validate_file_versions(buckets_tab, file_name)
            logger.info(f"Successfully validated {expected_versions_after_deletion} versions remain after deletion")
        finally:
            # Restore original expected versions
            self.EXPECTED_VERSIONS = original_expected
