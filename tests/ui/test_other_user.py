import logging
import pytest


from ocs_ci.ocs.ui.mcg_ui import ObcUi
from ocs_ci.ocs import ocp
from ocs_ci.framework.testlib import ManageTest, ui, bugzilla, polarion_id, tier2
from time import sleep
from ocs_ci.utility.utils import ceph_health_check


logger = logging.getLogger(__name__)


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
    @tier2
    @bugzilla("2031705")
    @polarion_id("OCS-4620")
    def test_create_storageclass_rbd(self, user_factory, login_factory):
        """Create user"""
        user = user_factory()
        sleep(30)

        """ Create RoleBinding """
        ocp_obj = ocp.OCP()
        ocp_obj.exec_oc_cmd(
            f"-n openshift-storage create rolebinding {user[0]} --role=mcg-operator.v4.11.0-noobaa-odf-ui-548459769c "
            f"--user={user[0]} "
        )

        """Login using created user"""
        obc_ui_obj = ObcUi(login_factory(user[0], user[1]))
        obc_ui_obj.check_obc_option()
