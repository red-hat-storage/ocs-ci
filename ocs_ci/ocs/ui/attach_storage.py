import logging
from time import sleep

from ocs_ci.ocs.ui.base_ui import BaseUI

logger = logging.getLogger(__name__)


class AttachStorage(BaseUI):
    """
    Class to handle the 'Attach Storage' action in Storage Cluster details page.

    """

    def __init__(self):
        super().__init__()

        self.storage_cluster_title_text = "Storage cluster"
        # Locators for the 'Attach Storage' form fields and buttons
        self.storage_cluster_actions = self.attach_storage_loc[
            "storage_cluster_actions"
        ]
        self.attach_storage_button = self.attach_storage_loc["attach_storage_button"]
        self.device_class_input = self.attach_storage_loc["device_class_input"]
        self.type_block_btn = self.attach_storage_loc["type_block_btn"]
        self.type_fs_btn = self.attach_storage_loc["type_fs_btn"]
        self.pool_name_input = self.attach_storage_loc["pool_name_input"]
        self.select_replication_dropdown = self.attach_storage_loc[
            "select_replication_dropdown"
        ]
        self.replication_2 = self.attach_storage_loc["replication_2"]
        self.replication_3 = self.attach_storage_loc["replication_3"]
        self.new_sc_name_input = self.attach_storage_loc["new_sc_name_input"]
        self.confirm_action_btn = self.attach_storage_loc["confirm_action_btn"]
        self.storage_cluster_title = self.validation_loc["storage_cluster_title"]

    def send_form_with_default_values(self):
        """
        Send the form for attaching storage with the default values. This method assumes we are
        already on the 'Attach Storage' form page.

        The default values are:
        - Device class: The default value shown in the form (retrieved from the input field)
        - Volume type: Block
        - Pool name: Same as the device class value (With the added prefix
        'ocs-storagecluster-cephblockpool' as defined in the 'Attach Storage' form)
        - Data protection policy: 3-way Replication
        - New StorageClass name: <device_class_value>-sc

        """
        logger.info(
            "Filling the form for attaching storage with default values and submitting it."
        )
        # Get the default value of Device class
        self.wait_for_element_to_be_visible(self.device_class_input)
        device_class_val = self.get_element_attribute(self.device_class_input, "value")
        # Select the Volume type 'Block'
        self.do_click(self.type_block_btn)
        # Fill the Pool name field
        self.do_send_keys(self.pool_name_input, device_class_val)
        # Select the Data protection policy '3-way Replication'
        self.do_click(self.select_replication_dropdown)
        self.wait_for_element_to_be_visible(self.replication_3)
        self.do_click(self.replication_3)
        # Fill the New StorageClass name field
        self.do_send_keys(self.new_sc_name_input, f"{device_class_val}-sc")
        wait_time = 10
        logger.info(
            f"Waiting for {wait_time} seconds before confirming the action to simulate user "
            f"review time before submitting the form."
        )
        sleep(wait_time)
        # Confirm the action
        self.do_click(self.confirm_action_btn)
        logger.info(
            "Form submitted for attaching storage with default values. "
            "Waiting for the action to complete and return to Storage Cluster details page."
        )
        self.wait_until_expected_text_is_found(
            locator=self.storage_cluster_title,
            expected_text=self.storage_cluster_title_text,
            timeout=90,
        )
