"""
Test dev user Object Browser UI operations.

Tests the S3 login form and console logout functionality for non-admin users.
"""

import logging
import time

import pytest

from ocs_ci.framework import config as ocsci_config
from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    mcg,
    polarion_id,
    tier1,
    ui,
)
from ocs_ci.ocs.ui.base_ui import close_browser, login_ui, logout_ui
from ocs_ci.ocs.ui.page_objects.buckets_tab import BucketsTab
from ocs_ci.ocs.ui.page_objects.s3_login_form import S3LoginForm
from ocs_ci.utility.users import (
    bind_user_to_noobaa_ui_role,
    create_noobaa_ui_clusterrole,
    delete_user_noobaa_ui_binding,
)

logger = logging.getLogger(__name__)


@tier1
@ui
@black_squad
@mcg
class TestDevUserObjectBrowserUI:
    """
    Test class for dev user Object Browser UI operations.

    Tests S3 login form authentication and console logout functionality.

    """

    @pytest.fixture()
    def dev_user_setup(self, request, user_factory, mcg_account_factory):
        """
        Setup a non-admin dev user with NooBaa account.

        Creates:
        1. htpasswd user via user_factory
        2. noobaa-odf-ui ClusterRole
        3. ClusterRoleBinding for the user
        4. NooBaa account (creates secret noobaa-account-{name})

        Returns:
            dict: Contains username, password, secret_namespace, secret_name

        """
        username, password = user_factory()
        logger.info(f"Created htpasswd user: {username}")

        time.sleep(30)

        create_noobaa_ui_clusterrole()
        bind_user_to_noobaa_ui_role(username)
        logger.info(f"Bound user {username} to noobaa-odf-ui ClusterRole")

        account_name = f"dev-{username.lower()}"
        mcg_account_factory(name=account_name)
        secret_name = f"noobaa-account-{account_name}"
        logger.info(f"Created NooBaa account with secret: {secret_name}")

        def finalizer():
            logger.info(f"Cleaning up user binding for {username}")
            delete_user_noobaa_ui_binding(username)

        request.addfinalizer(finalizer)

        return {
            "username": username,
            "password": password,
            "secret_namespace": ocsci_config.ENV_DATA["cluster_namespace"],
            "secret_name": secret_name,
        }

    @polarion_id("OCS-XXXX")
    def test_dev_user_login_logout_admin_login(self, request, dev_user_setup):
        """
        Test dev user login, console logout, and admin re-login flow.

        Steps:
        1. Login as non-admin user to OpenShift console
        2. Navigate to Object Storage page
        3. Sign in with S3 secret credentials
        4. Verify S3 sign-in success
        5. Logout from OpenShift console
        6. Login as admin (kubeadmin)
        7. Navigate to Object Storage and verify admin context

        """
        user = dev_user_setup

        def close_browser_finalizer():
            logger.info("Closing browser")
            close_browser()

        request.addfinalizer(close_browser_finalizer)

        logger.info(f"Step 1: Login as non-admin user: {user['username']}")
        login_ui(username=user["username"], password=user["password"])

        logger.info("Step 2: Navigate to Object Storage page")
        bucket_ui = BucketsTab()
        bucket_ui.nav_object_storage_page()
        time.sleep(3)

        logger.info("Step 3: Sign in with S3 secret credentials")
        s3_login = S3LoginForm()
        s3_login.sign_in_with_secret(
            namespace=user["secret_namespace"],
            secret_name=user["secret_name"],
        )

        logger.info("Step 4: Verify S3 sign-in success")
        assert s3_login.is_signed_in(), "S3 login failed - success label not visible"
        logger.info("S3 sign-in successful")

        logger.info("Step 5: Logout from OpenShift console")
        logout_ui()
        time.sleep(2)

        logger.info("Step 6: Login as admin (kubeadmin)")
        login_ui()

        logger.info("Step 7: Navigate to Object Storage and verify admin context")
        bucket_ui_admin = BucketsTab()
        bucket_ui_admin.nav_object_storage_page()
        time.sleep(2)

        s3_login_check = S3LoginForm()
        s3_login_form_visible = bool(
            s3_login_check.get_elements(
                s3_login_check.bucket_tab["s3_login_project_dropdown"]
            )
        )
        assert (
            not s3_login_form_visible
        ), "S3 login form should not be visible for admin user"
        logger.info("Admin context verified - S3 login form not shown for admin")
