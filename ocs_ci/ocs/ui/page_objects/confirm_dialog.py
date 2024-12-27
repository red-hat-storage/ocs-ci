from ocs_ci.ocs.ui.base_ui import BaseUI


class ConfirmDialog(BaseUI):
    """
    Page object for Confirm Dialog
    """

    def dialog_confirm_delete(self, resource_name):
        """
        Action to confirm delete resource
        """
        # placeholder for the confirm dialog remains even when text input contains any text
        # remove the text if exists, for more complex scenarios
        self.dialog_type_resource_name(resource_name)
        self.dialog_confirm()

    def dialog_confirm(self):
        """
        Clicks on Delete button
        """
        self.do_click(self.generic_locators["confirm_delete_resource"])

    def dialog_cancel(self):
        """
        Clicks on Cancel button
        """
        self.do_click(self.generic_locators["cancel_delete_resource"])

    def dialog_type_resource_name(self, resource_name):
        """
        Type the resource name in the dialog
        """
        from ocs_ci.ocs.ui.helpers_ui import format_locator

        self.clear_input_gradually(
            format_locator(self.generic_locators["confirm_dilog_input"], resource_name)
        )
        self.do_send_keys(
            format_locator(self.generic_locators["confirm_dilog_input"], resource_name),
            resource_name,
        )

    def dialog_close(self):
        """
        Close the dialog
        """
        self.do_click(self.generic_locators["close_dialog"])
