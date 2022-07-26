import logging
import os
import pytest

from ocs_ci.ocs.ui.mcg_ui import ObcUI
from ocs_ci.ocs.ui.views import locators
from ocs_ci.framework.testlib import ManageTest, ui
from time import sleep
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


class ObcUi(ObcUI):
    def __init__(self, driver):
        super().__init__(driver)

    def check_obc_option(self, text="Object Bucket Claim"):

        "check OBC is visible to user after giving admin access"
        self.sc_loc = locators[self.ocp_version]["obc"]
        self.do_click(self.sc_loc["Developer_dropdown"])
        self.do_click(self.sc_loc["select_administrator"])

        self.choose_expanded_mode(mode=True, locator=self.page_nav["Storage"])
        assert self.check_element_text(text), f"User not able to see OBC option"

        self.do_click(self.sc_loc["create_project"])
        # self.do_click(self.sc_loc["create_obc"])


class TestOBCUi(ManageTest):
    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        def finalizer():
            logger.info("Perform Ceph health checks after 'add capacity'")
            ceph_health_check()

        request.addfinalizer(finalizer)

    @ui
    def test_create_storageclass_rbd(self, user_factory, login_factory):

        """Create user"""
        user = user_factory()
        kubeconfig = os.getenv("KUBECONFIG")
        kube_data = ""
        with open(kubeconfig, "r") as kube_file:
            kube_data = kube_file.readlines()
        sleep(30)
        with open(kubeconfig, "w+") as kube_file:
            kube_file.writelines(kube_data)
            print(kube_file.readlines())

        "Login using created user"
        obc_ui_obj = ObcUi(login_factory(user[0], user[1]))
        obc_ui_obj.check_obc_option()
