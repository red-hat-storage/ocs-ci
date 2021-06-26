import logging
import time

from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version, get_running_ocp_version
from ocs_ci.helpers import helpers

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
        vol_mode (str): volume mode type

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

        if (
            sc_type
            in ("ocs-storagecluster-ceph-rbd-thick", "ocs-storagecluster-ceph-rbd")
            and access_mode == "ReadWriteOnce"
        ):
            logger.info(f"Test running on OCP version: {get_running_ocp_version()}")

            logger.info(f"Selecting Volume Mode of type {vol_mode}")
            self.do_click(self.pvc_loc[vol_mode])

        logger.info("Create PVC")
        self.do_click(self.pvc_loc["pvc_create"])

        time.sleep(2)

    def verify_pvc_ui(self, pvc_size, access_mode, vol_mode, sc_type):
        """
        Verifying PVC details via UI

        pvc_size (str): the size of pvc (GB)
        access_mode (str): access mode
        vol_mode (str): volume mode type

        """
        pvc_size_new = f"{pvc_size} GiB"
        self.check_element_text(expected_text=pvc_size_new)
        logger.info(f"Verifying pvc size : {pvc_size_new}")

        pvc_access_mode_new = f"{access_mode}"
        self.check_element_text(expected_text=pvc_access_mode_new)
        logger.info(f"Verifying access mode : {pvc_access_mode_new}")

        if (
            sc_type
            in (
                "ocs-storagecluster-ceph-rbd-thick",
                "ocs-storagecluster-ceph-rbd",
            )
            and (access_mode == "ReadWriteOnce")
        ):
            pvc_vol_mode_new = f"{vol_mode}"
            self.check_element_text(expected_text=pvc_vol_mode_new)
            logger.info(f"Verifying volume mode : {pvc_vol_mode_new}")

    def pvc_resize_ui(self, pvc_name, pvc_size, new_size, sc_type):
        """
        Resizing pvc via UI


        pvc_name (str): the name of pvc
        pvc_size (str): the size of pvc (GB)
        new_size (int): the new size of pvc (GB)
        sc_type (str): storage class type

        """
        self.navigate_persistentvolumeclaims_page()

        logger.info("Select openshift-storage project")
        self.do_click(self.pvc_loc["pvc_project_selector"])
        self.do_click(self.pvc_loc["select_openshift-storage_project"])

        self.do_send_keys(self.pvc_loc["search_pvc"], text=pvc_name)

        logger.info(f"Go to PVC {pvc_name} Page")
        self.do_click(self.pvc_loc[pvc_name])

        if sc_type in "ocs-storagecluster-ceph-rbd":
            helpers.create_pod()

        logger.info("Checking status of Pvc")
        self.wait_for_element(self.pvc_loc["pvc-status"], text_="Bound")

        logger.info("Click on Actions")
        self.do_click(self.pvc_loc["pvc_actions"])

        logger.info("Click on Expand PVC")
        self.do_click(self.pvc_loc["expand_pvc"])

        logger.info("Clearing the size of existing pvc")
        self.do_clear(self.pvc_loc["resize-value"])

        if new_size > int(pvc_size):
            logger.info("Enter the new pvc size")
            self.do_send_keys(self.pvc_loc["resize-value"], text=new_size)
        else:
            logger.info(
                f"New pvc size can not be less than existing pvc size: {new_size}"
            )
            raise Exception

        logger.info("Click on Expand Button")
        self.do_click(self.pvc_loc["expand-btn"])

        time.sleep(3)

    def verify_pvc_resize_ui(self, pvc_name, new_size):
        """
        Verifying PVC resize via UI

        pvc_name (str): the name of pvc
        new_size (int): the new size of pvc (GB)

        """

        self.navigate_persistentvolumeclaims_page()

        logger.info("Select openshift-storage project")
        self.do_click(self.pvc_loc["pvc_project_selector"])
        self.do_click(self.pvc_loc["select_openshift-storage_project"])

        self.do_send_keys(self.pvc_loc["search_pvc"], text=pvc_name)

        logger.info(f"Go to PVC {pvc_name} Page")
        self.do_click(self.pvc_loc[pvc_name])

        resized_pvc = f"{new_size} GiB"

        self.check_element_text(expected_text=resized_pvc)

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
        self.do_click(self.pvc_loc[pvc_name])

        logger.info("Click on Actions")
        self.do_click(self.pvc_loc["pvc_actions"])

        logger.info("Click on 'Delete PVC'")
        self.do_click(self.pvc_loc["pvc_delete"])

        logger.info("Confirm PVC Deletion")
        self.do_click(self.pvc_loc["confirm_pvc_deletion"])

        time.sleep(2)
