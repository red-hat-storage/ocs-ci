import logging

from ocs_ci.ocs.ui.base_ui import BaseUI

logger = logging.getLogger(__name__)


class AttachStorage(BaseUI):
    """
    Class to handle the 'Attach Storage' action in Storage Cluster details page.

    """

    def __init__(self):
        super().__init__()

    def send_form_with_default_values(self):
        """
        Send the form for attaching storage with the default values. This method assumes we are
        already on the 'Attach Storage' form page.

        """
        logger.info("Sending form for attaching storage with default values")
        # Need to add the relevant code here to interact with the form fields and submit the form.
