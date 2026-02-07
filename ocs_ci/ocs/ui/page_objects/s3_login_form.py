import logging

from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.ocs.ui.helpers_ui import format_locator

logger = logging.getLogger(__name__)


class S3LoginForm(BaseUI):
    """
    Page object for S3 login form on Object Storage buckets page.

    Handles authentication via Secret namespace + Secret name selection.
    This form appears on /odf/object-storage/buckets when not authenticated.
    """

    def _select_from_dropdown(self, dropdown_locator: str, item_name: str) -> None:
        """
        Select item from dropdown with search.

        Args:
            dropdown_locator (str): Key for the dropdown button locator in bucket_tab.
            item_name (str): The item name to search and select.

        """
        self.do_click(self.bucket_tab[dropdown_locator])
        self.do_send_keys(self.bucket_tab["s3_login_dropdown_search"], item_name)
        self.do_click(
            format_locator(self.bucket_tab["s3_login_dropdown_item"], item_name)
        )

    def select_project(self, namespace: str) -> None:
        """
        Select the secret namespace from dropdown.

        Args:
            namespace (str): The namespace containing the secret (e.g., "openshift-storage").

        """
        logger.info(f"Selecting project namespace: {namespace}")
        self._select_from_dropdown("s3_login_project_dropdown", namespace)

    def select_secret(self, secret_name: str) -> None:
        """
        Select the secret from dropdown.

        Args:
            secret_name (str): The secret name (e.g., "noobaa-account-dev-xyz").

        """
        logger.info(f"Selecting secret: {secret_name}")
        self._select_from_dropdown("s3_login_secret_dropdown", secret_name)

    def click_sign_in(self) -> None:
        """Click the Sign in button."""
        logger.info("Clicking Sign in button")
        self.do_click(self.bucket_tab["s3_login_sign_in_button"])
        self.page_has_loaded()

    def sign_in_with_secret(self, namespace: str, secret_name: str) -> None:
        """
        Complete S3 sign-in flow: select project, select secret, click sign in.

        Args:
            namespace (str): The namespace containing the secret.
            secret_name (str): The secret name with S3 credentials.

        """
        self.select_project(namespace)
        self.select_secret(secret_name)
        self.click_sign_in()
        self.wait_for_login_success()

    def is_signed_in(self) -> bool:
        """
        Check if currently signed in.

        Returns:
            bool: True if "Signed in with credentials" label is visible.

        """
        return bool(self.get_elements(self.bucket_tab["s3_login_success_label"]))

    def wait_for_login_success(self, timeout: int = 30) -> None:
        """
        Wait for login to complete successfully.

        Args:
            timeout (int): Maximum wait time in seconds.

        Raises:
            TimeoutException: If login does not complete within timeout.

        """
        logger.info("Waiting for S3 login success indicator")
        self.wait_for_element_to_be_visible(
            self.bucket_tab["s3_login_success_label"], timeout=timeout
        )
        logger.info("Successfully signed in with S3 credentials")
