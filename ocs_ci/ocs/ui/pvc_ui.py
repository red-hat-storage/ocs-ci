import logging
import time

from semantic_version import Version
from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators, pvc_4_8a, pvc_4_8b
from ocs_ci.utility.utils import get_ocp_version, get_running_ocp_version

logger = logging.getLogger(__name__)


class PvcUI(PageNavigator):
    """
    User Interface Selenium

    """

    def __init__(self, driver):
        super().__init__(driver)
        ocp_version = get_ocp_version()
        self.pvc_loc = locators[ocp_version]["pvc"]

    def create_pvc_ui(self, sc_type, pvc_name, access_mode, pvc_size, vol_mode):
        """
        Create PVC via UI.

        sc_type (str): storage class type
        pvc_name (str): the name of pvc
        access_mode (str): access mode
        pvc_size (str): the size of pvc (GB)

        """
        self.navigate_persistentvolumeclaims_page()

        logger.info("Select openshift-storage project")
        self.do_click(self.pvc_loc["pvc_project_selector"])
        self.do_click(self.pvc_loc["select_openshift-storage_project"])

        logger.info("Click on 'Create Persistent Volume Claim'")
        self.do_click(self.pvc_loc["pvc_create_button"])

        logger.info("Select Storage Class")
        self.do_click(self.pvc_loc["pvc_storage_class_selector"])
        self.do_click(self.pvc_loc[sc_type])

        logger.info("Select PVC name")
        self.do_send_keys(self.pvc_loc["pvc_name"], pvc_name)

        logger.info("Select Access Mode")
        self.do_click(self.pvc_loc[access_mode])

        logger.info("Select PVC size")
        self.do_send_keys(self.pvc_loc["pvc_size"], text=pvc_size)

        if Version.coerce(get_running_ocp_version()) >= Version.coerce("4.8"):
            logger.info(f"Test running on OCP version" + ":" + str({get_running_ocp_version()}))
            logger.info(f"Selecting Volume Mode of type {vol_mode}")
            self.do_click(self.pvc_loc[vol_mode])

        logger.info("Create PVC")
        self.do_click(self.pvc_loc["pvc_create"])

        time.sleep(2)

    def delete_pvc_ui(self, pvc_name):
        """
        Delete pvc via UI

        pvc_name (str): Name of the pvc

        """
        self.navigate_persistentvolumeclaims_page()

        logger.info("Select openshift-storage project")
        self.do_click(self.pvc_loc["pvc_project_selector"])
        self.do_click(self.pvc_loc["select_openshift-storage_project"])

        self.do_send_keys(self.pvc_loc["search_pvc"], text=pvc_name)

        logger.info(f"Go to PVC {pvc_name} Page")
        self.do_click(pvc_4_8b[pvc_name])

        logger.info("Click on Actions")
        self.do_click(self.pvc_loc["pvc_actions"])

        logger.info("Click on 'Delete PVC'")
        self.do_click(self.pvc_loc["pvc_delete"])

        logger.info("Confirm PVC Deletion")
        self.do_click(self.pvc_loc["confirm_pvc_deletion"])

        time.sleep(2)


