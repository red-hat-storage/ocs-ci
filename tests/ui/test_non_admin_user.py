import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_ibm_cloud_managed,
    black_squad,
)
from ocs_ci.ocs.exceptions import UnexpectedODFAccessException
from ocs_ci.ocs.ui.page_objects.object_bucket_claims_tab import ObjectBucketClaimsTab

from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.framework.testlib import (
    ManageTest,
    ui,
    bugzilla,
    polarion_id,
    tier2,
    tier1,
    E2ETest,
)
from ocs_ci.utility.utils import ceph_health_check


logger = logging.getLogger(__name__)


@black_squad
class TestOBCUi(ManageTest):
    """
    Validate User able to see the OBC resource from the Console

    Test Process:
    1.Created a user
    2.Create project
    3.Added admin role to this user of the project.
    4.Validated the access of OBC from Console.

    """

    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        def finalizer():
            logger.info("Perform Ceph health checks ")
            ceph_health_check()

        request.addfinalizer(finalizer)

    @ui
    @black_squad
    @tier2
    @skipif_ibm_cloud_managed
    @bugzilla("2031705")
    @polarion_id("OCS-4620")
    @pytest.mark.parametrize("admin", [True, False])
    def test_project_admin_obcs_access(self, user_factory, login_factory, admin):
        """
        Test if user with admin access to the project can view the list of OBCs

        """
        user = user_factory()
        login_factory(user[0], user[1])
        obc_ui_obj = ObjectBucketClaimsTab()
        assert admin == obc_ui_obj.check_obc_option(
            user[0], admin=admin
        ), f"User {user[0]} is project admin: {admin} but permissions to OBCs page didn't match."


@black_squad
class TestUnprivilegedUserODFAccess(E2ETest):
    """
    Test if unprivileged user can see ODF dashboard
    """

    @ui
    @tier1
    @skipif_ibm_cloud_managed
    @bugzilla("2103975")
    @polarion_id("OCS-4667")
    def test_unprivileged_user_odf_access(self, user_factory, login_factory):
        # create a user without any role
        user = user_factory()
        logger.info(f"user created: {user[0]} password: {user[1]}")

        # login with the user created
        login_factory(user[0], user[1])
        validation_ui_obj = ValidationUI()
        try:
            validation_ui_obj.validate_unprivileged_access()
        except UnexpectedODFAccessException:
            assert False, "Unexpected, unprivileged users can access ODF dashboard"
