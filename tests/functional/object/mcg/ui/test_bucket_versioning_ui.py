import logging
import os
import time

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
class TestBucketVersioningUI:
    """
    Test class for bucket versioning functionality via UI.
    """

    EXPECTED_VERSIONS = 3
    UPLOAD_WAIT_TIME = 5
    NAVIGATION_WAIT_TIME = 2
    VERSIONS_LOAD_WAIT_TIME = 3
    INITIAL_UPLOAD_WAIT_TIME = 2
    VERSION_DETECTION_TIMEOUT = 30

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

    def _create_file_version(
        self,
        file_path: str,
        version_content: str,
        buckets_tab: BucketsTab,
        folder_path: str,
        folder_name: str,
        version_number: int,
        bucket_name: str,
    ) -> None:
        """
        Create a new version of a file by modifying content and uploading.

        Args:
            file_path: Path to the file to modify
            version_content: Content to write to the file
            buckets_tab: BucketsTab instance for UI operations
            folder_path: Path to the folder containing the file
            folder_name: Name of the folder for logging
            version_number: Version number for logging
            bucket_name: Name of the bucket to work with
        """
        logger.info(f"Modifying file content for version {version_number}: {file_path}")
        self._modify_file_content(file_path, version_content)

        if version_number > 1:
            buckets_tab.navigate_to_bucket(bucket_name)

        buckets_tab.upload_folder_to_bucket(
            folder_path, wait_time=self.UPLOAD_WAIT_TIME
        )
        logger.info(
            f"Successfully uploaded folder: {folder_name} - version {version_number} created"
        )

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

        logger.info(f"Setting initial content for first version: {file_path}")
        with open(file_path, "w") as f:
            f.write("content-v1")

        logger.info(f"Created folder at: {folder_path} with {len(files)} file(s)")
        return file_path, folder_path

    def _create_bucket_and_upload_folder(self, folder_path: str) -> tuple[str, str]:
        """
        Create a new bucket and upload the initial file folder.

        Args:
            folder_path: Path to the local folder to upload

        Returns:
            tuple[str, str]: (folder_name, bucket_name)
        """
        logger.info("Creating new bucket and uploading folder with 1 file")
        bucket_ui = BucketsTab()
        _, bucket_name = bucket_ui.create_bucket_ui("s3", return_name=True)
        logger.info(f"Created new bucket: {bucket_name}")
        logger.info(f"Current test will work with bucket: {bucket_name}")
        buckets_tab = bucket_ui

        # Upload folder directly to bucket root (no nested test-folder creation)
        buckets_tab.upload_folder_to_bucket(folder_path)

        # Extract the actual folder name from the uploaded content
        folder_name = os.path.basename(folder_path)
        logger.info(
            f"Successfully uploaded folder: {folder_name} to bucket: {bucket_name}"
        )
        return folder_name, bucket_name

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
        buckets_tab.wait_for_element_to_be_present(
            buckets_tab.bucket_tab["version_id_for_latest"],
            timeout=self.VERSION_DETECTION_TIMEOUT,
        )

        version_id_elements = buckets_tab.get_elements(
            buckets_tab.bucket_tab["version_id_for_latest"]
        )
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

        time.sleep(3)

        logger.info("Clicking 'Delete this version' option")
        buckets_tab.do_click(buckets_tab.bucket_tab["delete_this_version_option"])

        buckets_tab.wait_for_element_to_be_present(
            buckets_tab.bucket_tab["delete_version_modal_title"], timeout=10
        )

        logger.info("Typing 'delete' in confirmation input")
        input_element = buckets_tab.get_elements(
            buckets_tab.bucket_tab["delete_version_input"]
        )
        input_element[0].clear()
        input_element[0].send_keys("delete")

        time.sleep(2)

        logger.info("Clicking Delete object button to confirm")
        buckets_tab.do_click(buckets_tab.bucket_tab["delete_version_confirm_button"])

        time.sleep(3)
        logger.info("Version deletion completed")

    def _validate_file_versions(self, buckets_tab: BucketsTab) -> None:
        """
        Validate that the expected number of file versions exist and "Latest" labels are present.

        Args:
            buckets_tab: BucketsTab instance for UI operations

        Raises:
            AssertionError: If version count or Latest labels don't match expectations
            TimeoutException: If elements don't load within timeout
        """
        buckets_tab.wait_for_element_to_be_present(
            buckets_tab.bucket_tab["version_latest_label"],
            timeout=self.VERSION_DETECTION_TIMEOUT,
        )

        expected_versions = self.EXPECTED_VERSIONS

        logger.info("Waiting for version checkboxes to be present")
        try:
            buckets_tab.wait_for_element_to_be_present(
                buckets_tab.bucket_tab["version_row_checkboxes"],
                timeout=10,
            )
        except TimeoutException:
            logger.warning("Timeout waiting for version checkboxes, continuing anyway")

        max_retries = 6
        retry_interval = 2
        actual_version_count = 0

        for attempt in range(max_retries):
            version_checkboxes = buckets_tab.get_elements(
                buckets_tab.bucket_tab["version_row_checkboxes"]
            )
            actual_version_count = len(version_checkboxes)

            logger.info(
                f"Version count by checkboxes\n"
                f"attempt {attempt + 1}/{max_retries}\n"
                f"Expected {expected_versions}\n"
                f"Found {actual_version_count}"
            )

            if actual_version_count == expected_versions:
                logger.info(f"Successfully found all {expected_versions} versions")
                break

            if attempt < max_retries - 1:
                logger.info(
                    f"Version count mismatch, waiting {retry_interval} seconds before retry..."
                )
                time.sleep(retry_interval)
                try:
                    buckets_tab.wait_for_element_to_be_present(
                        buckets_tab.bucket_tab["version_row_checkboxes"],
                        timeout=5,
                    )
                except TimeoutException:
                    logger.warning(
                        "Timeout waiting for version checkboxes during retry"
                    )
        else:
            raise AssertionError(
                f"Version count validation failed after {max_retries} attempts: Expected {expected_versions} versions, "
                f"but found {actual_version_count} checkboxes"
            )

        latest_labels = buckets_tab.get_elements(
            buckets_tab.bucket_tab["version_latest_label"]
        )

        latest_label_texts_num = len(
            [label.text for label in latest_labels if label.text == "Latest"]
        )

        if latest_label_texts_num != 1:
            raise AssertionError(
                f"Latest label validation failed: Expected a single version with "
                f"the 'Latest' label but got {latest_label_texts_num}"
            )

        logger.info(
            f"Successfully found {expected_versions} versions, one with the expected 'Latest' label"
        )

    @polarion_id("OCS-6884")
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

        bucket_versioning = BucketVersioning()
        bucket_versioning.nav_object_storage_page()

        # Step 1: Create local folder with 1 file
        logger.info("Step 1: Creating local folder with 1 file")
        file_path, folder_path = self._setup_test_file()

        # Step 2: Create new bucket and upload folder
        logger.info("Step 2: Creating new bucket and uploading folder with 1 file")
        folder_name, current_bucket_name = self._create_bucket_and_upload_folder(
            folder_path
        )
        logger.info(
            f"Test test_enable_bucket_versioning working with bucket: {current_bucket_name}"
        )

        # Step 3: Enable versioning
        logger.info("Step 3: Enabling versioning for bucket")
        versioning_enabled = bucket_versioning.enable_versioning(
            bucket_name=current_bucket_name
        )
        if versioning_enabled:
            logger.info("Versioning enabled successfully")
        else:
            logger.info("Versioning was already enabled")

        # Step 4: Upload the same folder multiple times to create versions
        logger.info("Step 4: Creating multiple file versions")

        buckets_tab = BucketsTab()

        self._create_file_version(
            file_path,
            "content-v2",
            buckets_tab,
            folder_path,
            folder_name,
            2,
            current_bucket_name,
        )

        self._create_file_version(
            file_path,
            "content-v3",
            buckets_tab,
            folder_path,
            folder_name,
            3,
            current_bucket_name,
        )

        # Step 5: Navigate to folder and show versioning
        logger.info("Step 5: Navigating to folder and showing versions")
        buckets_tab.navigate_to_folder_and_enable_versions(
            folder_name,
            current_bucket_name,
            self.NAVIGATION_WAIT_TIME,
            self.VERSIONS_LOAD_WAIT_TIME,
        )

        # Step 6: Validate "Latest" label appears on newest version
        logger.info("Step 6: Validating 'Latest' label on newest version")

        try:
            self._validate_file_versions(buckets_tab)
        except (TimeoutException, AssertionError) as e:
            logger.debug(f"Failed to validate 'Latest' label: {e}")
            raise

        assert (
            folder_name
        ), "Test setup validation failed: Failed to create and upload folder"
        logger.info(
            "Bucket versioning test with label validation completed successfully"
        )

    @polarion_id("OCS-6886")
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

        folder_name, current_bucket_name = self._create_bucket_and_upload_folder(
            folder_path
        )
        logger.info(
            f"Test test_bucket_versioning_with_modification working with bucket: {current_bucket_name}"
        )

        # Step 2: Enable versioning
        logger.info("Step 2: Enabling versioning for bucket")
        versioning_enabled = bucket_versioning.enable_versioning(
            bucket_name=current_bucket_name
        )
        if versioning_enabled:
            logger.info("Versioning enabled successfully")
        else:
            logger.info("Versioning was already enabled")

        # Step 3: Modify the object locally
        logger.info("Step 3: Modifying object locally")
        self._modify_file_content(file_path, "modified-content-v2")

        # Step 4: Re-upload the modified object
        logger.info("Step 4: Re-uploading modified object")
        buckets_tab = BucketsTab()
        buckets_tab.navigate_to_bucket(current_bucket_name)
        buckets_tab.upload_folder_to_bucket(
            folder_path, wait_time=self.UPLOAD_WAIT_TIME
        )

        logger.info("Creating third version of the object")
        self._create_file_version(
            file_path,
            "modified-content-v3",
            buckets_tab,
            folder_path,
            folder_name,
            3,
            current_bucket_name,
        )

        # Step 5: Navigate to folder and toggle "List all versions"
        logger.info("Step 5: Navigating to folder and showing versions")
        buckets_tab.navigate_to_folder_and_enable_versions(
            folder_name,
            current_bucket_name,
            self.NAVIGATION_WAIT_TIME,
            self.VERSIONS_LOAD_WAIT_TIME,
        )

        # Wait for UI to refresh after enabling version listing
        logger.info(
            "Waiting for UI to load all versions after enabling version listing..."
        )
        time.sleep(self.VERSIONS_LOAD_WAIT_TIME)

        # Step 6: Verify version ID mapping with "Latest" tag
        logger.info("Step 6: Verifying version ID for latest object")

        self._validate_file_versions(buckets_tab)

        latest_version_id = self._get_version_id_for_latest(buckets_tab)
        assert latest_version_id, "Failed to get version ID for latest object"
        logger.info(
            f"Successfully retrieved version ID for latest object: {latest_version_id}"
        )

        # Step 7: Delete latest object and verify new one becomes latest
        logger.info("Step 7: Deleting latest version and verifying new latest")

        original_latest_version_id = latest_version_id
        logger.info(f"Original latest version ID: {original_latest_version_id}")

        self._delete_latest_version(buckets_tab)

        time.sleep(4)

        new_latest_version_id = self._get_version_id_for_latest(buckets_tab)
        logger.info(f"New latest version ID after deletion: {new_latest_version_id}")

        assert new_latest_version_id != original_latest_version_id, (
            f"Latest version ID should have changed after deletion. "
            f"Original: {original_latest_version_id}, New: {new_latest_version_id}"
        )

        expected_versions_after_deletion = self.EXPECTED_VERSIONS - 1

        original_expected = self.EXPECTED_VERSIONS
        self.EXPECTED_VERSIONS = expected_versions_after_deletion

        try:
            self._validate_file_versions(buckets_tab)
            logger.info(
                f"Successfully validated {expected_versions_after_deletion} versions remain after deletion"
            )
        finally:
            self.EXPECTED_VERSIONS = original_expected
