import logging

from ocs_ci.ocs.ui.base_ui import BaseUI, SeleniumDriver
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


class PreMigrationFusionAccessUI(PageNavigator, BaseUI):
    """
    UI page-object for pre-migration Fusion Access for SAN operations.

    Handles the storage cluster creation flow that must be completed before
    any migration to Fusion Access for SAN:

    1. Navigate to **Storage → Fusion Access for SAN**.
    2. Click **"Create storage cluster"** on the empty-state landing page.
    3. Select all available worker nodes (per-row checkboxes).
    4. Verify all nodes are checked.
    5. Click the final **"Create storage cluster"** submit button.
    """

    def __init__(self):
        super().__init__()

    def nav_fusion_access_for_san_page(self):
        """
        Navigate to Storage → Fusion Access for SAN.

        Expands the Storage section in the left sidebar and clicks the
        "Fusion Access for SAN" navigation link, landing on either the
        empty-state "No storage cluster" page or the existing cluster view.

        Returns:
            PreMigrationFusionAccessUI: self, to allow method chaining.
        """
        logger.info("Navigate to Storage > Fusion Access for SAN")
        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        self.do_click(
            locator=self.fusion_access_storage_cluster["fusion_access_for_san_nav"],
            timeout=60,
            enable_screenshot=True,
        )
        self.page_has_loaded(retries=15)
        logger.info("Successfully navigated to Fusion Access for SAN page")
        return self

    def create_fusion_access_storage_cluster(self):
        """
        Create a Fusion Access for SAN storage cluster via the OpenShift Console UI.

        Steps automated:

        1. Navigate to **Storage → Fusion Access for SAN**.
        2. Click **"Create storage cluster"** on the empty-state landing page.
        3. Select all worker nodes via individual per-row checkboxes (JS click
           to bypass the PF ``<label>`` overlay).
        4. Verify all node checkboxes are checked using
           ``wait_for_element_attribute`` (``TimeoutSampler``-backed).
        5. Click the final **"Create storage cluster"** submit button.

        Raises:
            TimeoutExpiredError: If any expected UI element is not found within
                its configured timeout.
            AssertionError: If not all node checkboxes are selected after the
                JS clicks and DOM-settle wait.
        """
        logger.info("Step 1: Navigate to Storage > Fusion Access for SAN landing page")
        self.nav_fusion_access_for_san_page()
        self.take_screenshot("fusion_access_landing_page")

        logger.info("Step 2: Click 'Create storage cluster' button on the landing page")
        self.do_click(
            locator=self.fusion_access_storage_cluster["create_storage_cluster_btn"],
            timeout=60,
            enable_screenshot=True,
        )
        self.page_has_loaded(retries=10)
        self.take_screenshot("create_storage_cluster_wizard")

        logger.info("Step 3: Select all nodes — clicking each unchecked row checkbox")
        locator = self.fusion_access_storage_cluster["node_row_checkboxes"]
        logger.info(
            "Waiting for worker node checkboxes to appear on the wizard page..."
        )
        self.wait_for_element_to_be_present(locator=locator, timeout=120)
        driver = SeleniumDriver()
        for attempt in range(1, 4):
            node_checkboxes = self.get_elements(locator)
            assert node_checkboxes, (
                "No node checkboxes found on the 'Create storage cluster' page. "
                "At least one worker node must be present."
            )
            unchecked = [cb for cb in node_checkboxes if not cb.is_selected()]
            if not unchecked:
                break
            logger.info(
                f"Attempt {attempt}: clicking {len(unchecked)} unchecked "
                f"checkbox(es) out of {len(node_checkboxes)}"
            )
            for checkbox in unchecked:
                driver.execute_script("arguments[0].click();", checkbox)
            self.wait_for_element_attribute(
                locator=locator,
                attribute="checked",
                attribute_value="true",
                timeout=15,
                sleep=1,
            )
        self.take_screenshot("all_nodes_selected")

        logger.info("Step 4: Verify all node checkboxes are selected")
        node_checkboxes = self.get_elements(locator)
        checked = [cb for cb in node_checkboxes if cb.is_selected()]
        assert len(checked) == len(node_checkboxes), (
            f"Only {len(checked)} of {len(node_checkboxes)} node checkboxes are selected "
            "after attempting to select all nodes."
        )
        logger.info(f"Verified: all {len(checked)} node(s) selected")

        summary_elements = self.get_elements(
            self.fusion_access_storage_cluster["nodes_selected_summary"]
        )
        if summary_elements:
            logger.info(f"Node selection summary: '{summary_elements[0].text}'")

        logger.info("Waiting for non-zero disk count in node selection summary...")
        self.wait_for_element_to_be_present(
            locator=self.fusion_access_storage_cluster["nodes_selected_with_disks"],
            timeout=60,
        )
        summary_elements = self.get_elements(
            self.fusion_access_storage_cluster["nodes_selected_summary"]
        )
        if summary_elements:
            logger.info(f"Final node selection summary: '{summary_elements[0].text}'")

        logger.info("Step 5: Click the 'Create storage cluster' submit button")
        self.do_click(
            locator=self.fusion_access_storage_cluster[
                "submit_create_storage_cluster_btn"
            ],
            timeout=60,
            enable_screenshot=True,
        )
        self.take_screenshot("storage_cluster_creation_submitted")
        logger.info("Fusion Access storage cluster creation submitted successfully")
