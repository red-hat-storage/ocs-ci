import logging

from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.ui.base_ui import BaseUI, wait_for_element_to_be_clickable
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.views import FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS

logger = logging.getLogger(__name__)


class PreMigrationFusionAccessUI(PageNavigator, BaseUI):
    """
    UI page-object for pre-migration Fusion Access for SAN operations.

    Handles the full storage cluster creation and file system claim flow that
    must be completed before any migration to Fusion Access for SAN:

    1. Navigate to **Storage → Fusion Access for SAN**.
    2. Click **"Create storage cluster"** on the empty-state landing page.
    3. Select all available worker nodes (per-row checkboxes).
    4. Verify all nodes are checked.
    5. Click the final **"Create storage cluster"** submit button.
    6. Wait for the cluster to become ready, then click **"Create file system claim"**.
    7. Enter a unique file system claim name.
    8. Select the first available LUN and submit the form.
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
            locator=FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS["fusion_access_for_san_nav"],
            timeout=60,
            enable_screenshot=True,
        )
        self.page_has_loaded(retries=15)
        logger.info("Successfully navigated to Fusion Access for SAN page")
        return self

    def create_fusion_access_storage_cluster(self):
        """
        Create a Fusion Access for SAN storage cluster and file system claim
        via the OpenShift Console UI.

        Steps automated:

        1. Navigate to **Storage → Fusion Access for SAN**.
        2. Click **"Create storage cluster"** on the empty-state landing page.
        3. Select all worker nodes via individual per-row checkboxes (JS click
           to bypass the PF v6 ``<label>`` overlay).
        4. Verify all node checkboxes are checked using
           ``wait_for_element_attribute`` (``TimeoutSampler``-backed).
        5. Click the final **"Create storage cluster"** submit button.
        6. Wait up to 10 minutes for **"Create file system claim"** to become
           enabled, then click it.
        7. Enter a unique file system claim name in the form.
        8. Select the first available LUN and click **"Create file system claim"**.

        Raises:
            TimeoutExpiredError: If any expected UI element is not found within
                its configured timeout.
            AssertionError: If not all node checkboxes are selected after the
                JS clicks and DOM-settle wait.
        """
        # Step 1 — Navigate to the Fusion Access for SAN landing page
        logger.info("Step 1: Navigate to Storage > Fusion Access for SAN landing page")
        self.nav_fusion_access_for_san_page()
        self.take_screenshot("fusion_access_landing_page")

        # Step 2 — Click the primary "Create storage cluster" button (empty-state)
        logger.info("Step 2: Click 'Create storage cluster' button on the landing page")
        self.do_click(
            locator=FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS[
                "create_storage_cluster_btn"
            ],
            timeout=60,
            enable_screenshot=True,
        )
        self.page_has_loaded(retries=10)
        self.take_screenshot("create_storage_cluster_wizard")

        # Step 3 — Click every individual per-row checkbox that is not yet checked.
        # The "Create storage cluster" page has no "Select all" header checkbox;
        # each row has its own <input id="node-<uuid>" class="pf-v6-c-check__input">.
        # PF v6 checkboxes are visually overlaid by a <label>, so a plain click on
        # the <input> is intercepted.  We use JS .click() to bypass that overlay.
        logger.info("Step 3: Select all nodes — clicking each unchecked row checkbox")
        node_checkboxes = self.get_elements(
            FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS["node_row_checkboxes"]
        )
        assert node_checkboxes, (
            "No node checkboxes found on the 'Create storage cluster' page. "
            "At least one worker node must be present."
        )
        logger.info(f"Found {len(node_checkboxes)} node row(s) in the table")
        for checkbox in node_checkboxes:
            if not checkbox.is_selected():
                self.driver.execute_script("arguments[0].click();", checkbox)
        self.take_screenshot("all_nodes_selected")

        # Step 4 — Wait until every row checkbox reports checked=true, then verify.
        # Uses wait_for_element_attribute (TimeoutSampler-backed) per framework
        # convention instead of a bare sleep — fails fast if DOM never settles.
        logger.info("Step 4: Waiting for all node checkboxes to reach checked state")
        locator = FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS["node_row_checkboxes"]
        self.wait_for_element_attribute(
            locator=locator,
            attribute="checked",
            attribute_value="true",
            timeout=30,
            sleep=1,
        )
        node_checkboxes = self.get_elements(locator)
        checked = [cb for cb in node_checkboxes if cb.is_selected()]
        assert len(checked) == len(node_checkboxes), (
            f"Only {len(checked)} of {len(node_checkboxes)} node checkboxes are selected "
            "after attempting to select all nodes."
        )
        logger.info(f"Verified: all {len(checked)} node(s) selected")

        # Log the summary text when visible ("3 nodes were selected, sharing 9 disks…")
        summary_elements = self.get_elements(
            FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS["nodes_selected_summary"]
        )
        if summary_elements:
            logger.info(f"Node selection summary: '{summary_elements[0].text}'")

        # Step 5 — Click the final "Create storage cluster" submit button
        logger.info("Step 5: Click the 'Create storage cluster' submit button")
        self.do_click(
            locator=FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS[
                "submit_create_storage_cluster_btn"
            ],
            timeout=60,
            enable_screenshot=True,
        )
        self.take_screenshot("storage_cluster_creation_submitted")
        logger.info("Fusion Access storage cluster creation submitted successfully")

        # Step 6 — Wait for storage cluster to become ready, then click
        # "Create file system claim".  The button is present in the DOM but
        # disabled while the cluster is initialising; element_to_be_clickable
        # waits until it is both visible and enabled (up to 10 minutes).
        logger.info(
            "Step 6: Waiting for 'Create file system claim' button to become enabled"
        )
        wait_for_element_to_be_clickable(
            locator=FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS[
                "create_file_system_claim_btn"
            ],
            timeout=600,
        )
        logger.info("Step 6: Clicking 'Create file system claim' button")
        self.do_click(
            locator=FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS[
                "create_file_system_claim_btn"
            ],
            timeout=30,
            enable_screenshot=True,
        )
        self.take_screenshot("create_file_system_claim_page")
        logger.info("'Create file system claim' page opened successfully")

        # Step 7 — Enter a unique file system claim name.
        # The input already has a placeholder ("file-system-1"); clear it first
        # so we can supply a deterministic, unique name for traceability.
        fs_claim_name = create_unique_resource_name("fs-claim", "fusion")
        logger.info(f"Step 7: Entering file system claim name: '{fs_claim_name}'")
        self.do_clear(
            locator=FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS[
                "file_system_claim_name_input"
            ],
            timeout=30,
        )
        self.do_send_keys(
            locator=FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS[
                "file_system_claim_name_input"
            ],
            text=fs_claim_name,
        )
        self.take_screenshot("file_system_claim_name_entered")

        # Step 8 — Select the first LUN from the "Select LUNs" table and submit.
        # The checkbox is hidden behind a <label>; clicking the label is the
        # correct interaction (matches the XPath the UI inspector reports).
        logger.info("Step 8: Selecting the first LUN from the 'Select LUNs' table")
        self.do_click(
            locator=FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS["first_lun_row_label"],
            timeout=30,
            enable_screenshot=True,
        )
        self.take_screenshot("first_lun_selected")

        logger.info("Step 8: Clicking 'Create file system claim' submit button")
        self.do_click(
            locator=FUSION_ACCESS_STORAGE_CLUSTER_LOCATORS[
                "submit_file_system_claim_btn"
            ],
            timeout=30,
            enable_screenshot=True,
        )
        self.take_screenshot("file_system_claim_submitted")
        logger.info(
            f"File system claim '{fs_claim_name}' creation submitted successfully"
        )
