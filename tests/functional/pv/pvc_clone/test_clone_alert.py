import time
import pytest
import logging
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier2,
    # polarion_id,
    skipif_ocp_version,
)
from ocs_ci.ocs.resources import pvc
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocs_version("<4.20")
@skipif_ocp_version("<4.20")
class TestAlertWhenTooManyClonesCreated(ManageTest):
    """
    Tests for alerts when too many clones are created
    """

    @pytest.fixture(autouse=True)
    def setup(self, pvc_factory):
        """
        Create a PVC and 199 clones

        Args:
            pvc_factory: A fixture to create new pvc

        """
        log.info("Starting the test setup")
        self.pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=1,
            status=constants.STATUS_BOUND,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        log.info(f"PVC {self.pvc_obj.name} created")

        # create 199 clones
        sc_name = self.pvc_obj.backed_sc
        parent_pvc = self.pvc_obj.name
        clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        namespace = self.pvc_obj.namespace
        self.cloned_obj_list = []
        for i in range(199):
            cloned_obj = pvc.create_pvc_clone(
                sc_name, parent_pvc, clone_yaml, namespace
            )
            log.info(f"Created {i + 1} clones")
            self.cloned_obj_list.append(cloned_obj)

    def teardown(self):
        """
        Delete all clones created during setup
        """
        log.info("Deleting clones")
        for clone_obj in self.cloned_obj_list:
            clone_obj.delete()

    def test_no_alert_under_limit(self, setup_ui_class):
        """
        Test that there is no alert in the UI when limit of 200 clones is not reached
        """
        wait_time = 60
        log.info(f"Waiting for {wait_time} seconds for alert to appear")
        time.sleep(wait_time)
        alert_ui_obj = PageNavigator()
        alert_ui_obj.navigate_alerting_page()
        alert_ui_obj.take_screenshot()
        assert not alert_ui_obj.check_element_text(
            element="a", expected_text="HighRBDCloneSnapshotCount"
        ), "Clones alert present on Alerting page. Expected to be absent"

    def test_alert_200_clones(self, setup_ui_class):
        """
        Test that there is an alert in the UI when limit of 200 clones is reached"""
        new_clone = pvc.create_pvc_clone(
            self.pvc_obj.backed_sc,
            self.pvc_obj.name,
            constants.CSI_RBD_PVC_CLONE_YAML,
            self.pvc_obj.namespace,
        )
        self.cloned_obj_list.append(new_clone)
        wait_time = 300
        log.info(f"Waiting for {wait_time} seconds for alert to appear")
        time.sleep(wait_time)
        alert_ui_obj = PageNavigator()
        alert_ui_obj.navigate_alerting_page()
        alert_ui_obj.take_screenshot()
        assert alert_ui_obj.check_element_text(
            element="a", expected_text="HighRBDCloneSnapshotCount"
        ), "Clones alert not found on Alerting page. Expected to be present"

    def test_alert_clones_and_snapshot(self, setup_ui_class, teardown_factory):
        """
        Test that there is an alert in the UI when limit of 199 clones + 1 snapshot is reached
        Also test that alert disappears after clone deletion
        """
        snap_yaml = constants.CSI_RBD_SNAPSHOT_YAML
        snap_name = helpers.create_unique_resource_name("test", "snapshot")
        snap_obj = pvc.create_pvc_snapshot(
            self.pvc_obj.name,
            snap_yaml,
            snap_name,
            self.pvc_obj.namespace,
            helpers.default_volumesnapshotclass(constants.CEPHBLOCKPOOL).name,
        )
        snap_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )
        teardown_factory(snap_obj)
        wait_time = 300
        log.info(f"Waiting for {wait_time} seconds for alert to appear")
        time.sleep(wait_time)
        alert_ui_obj = PageNavigator()
        alert_ui_obj.navigate_alerting_page()
        alert_ui_obj.take_screenshot()
        assert alert_ui_obj.check_element_text(
            element="a", expected_text="HighRBDCloneSnapshotCount"
        ), "Clones alert not found on Alerting page. Expected to be present"
        snap_obj.delete()
        log.info(f"Waiting for {wait_time} seconds for alert to disappear")
        time.sleep(wait_time)
        alert_ui_obj.navigate_alerting_page()
        alert_ui_obj.take_screenshot()
        assert not alert_ui_obj.check_element_text(
            element="a", expected_text="HighRBDCloneSnapshotCount"
        ), "Clones alert present on Alerting page. Expected to be absent"
