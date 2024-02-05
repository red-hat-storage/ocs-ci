import logging
import time

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.views import generic_locators
from ocs_ci.utility.utils import get_running_ocp_version
from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.helpers_ui import get_element_type
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


class PvcUI(PageNavigator):
    """
    User Interface Selenium
    """

    def __init__(self):
        super().__init__()
        self.driver.implicitly_wait(5)

    def create_pvc_ui(
        self, project_name, sc_name, pvc_name, access_mode, pvc_size, vol_mode
    ):
        """
        Create PVC via UI.
        Args:
            project_name (str): name of test project
            sc_name (str): storage class name
            pvc_name (str): the name of pvc
            access_mode (str): access mode
            pvc_size (str): the size of pvc (GB)
            vol_mode (str): volume mode type
        """
        self.navigate_persistentvolumeclaims_page()

        logger.info(f"Search test project {project_name}")
        self.do_click(self.pvc_loc["pvc_project_selector"])
        self.do_send_keys(self.pvc_loc["search-project"], text=project_name)

        self.wait_for_namespace_selection(project_name=project_name)

        logger.info("Click on 'Create Persistent Volume Claim'")
        self.do_click(self.pvc_loc["pvc_create_button"])

        logger.info("Click on Create PVC from dropdown options 'With Form'")
        self.do_click(self.pvc_loc["create_pvc_dropdown_item"])

        logger.info("Click on Storage Class selection")
        self.do_click(self.pvc_loc["pvc_storage_class_selector"])

        logger.info("Select the Storage Class type")
        self.do_click(format_locator(self.pvc_loc["storage_class_name"], sc_name))

        logger.info("Enter PVC name")
        self.do_send_keys(self.pvc_loc["pvc_name"], pvc_name)

        logger.info("Select Access Mode")
        self.do_click(self.pvc_loc[access_mode])

        logger.info("Select PVC size")
        self.do_send_keys(self.pvc_loc["pvc_size"], text=pvc_size)

        ocs_version = version.get_semantic_ocs_version_from_config()
        if (
            not self.ocp_version_full == version.VERSION_4_6
            and not ocs_version == version.VERSION_4_6
        ):
            if (
                sc_name != constants.DEFAULT_STORAGECLASS_CEPHFS
                and access_mode == "ReadWriteOnce"
            ):
                if (
                    sc_name != constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
                    and access_mode == "ReadWriteOnce"
                ):
                    logger.info(
                        f"Test running on OCP version: {get_running_ocp_version()}"
                    )

                    logger.info(f"Selecting Volume Mode of type {vol_mode}")
                    self.do_click(self.pvc_loc[vol_mode])

        logger.info("Create PVC")
        self.do_click(self.pvc_loc["pvc_create"])

    def verify_pvc_ui(
        self, pvc_size, access_mode, vol_mode, sc_name, pvc_name, project_name
    ):
        """
        Verifying PVC details via UI

        Args:
            pvc_size (str): the size of pvc (GB)
            access_mode (str): access mode
            vol_mode (str): volume mode type
            sc_name (str): storage class name
            pvc_name (str): the name of pvc
            project_name (str): name of test project
        """
        self.navigate_persistentvolumeclaims_page()

        logger.info(f"Search and Select test project {project_name}")
        self.do_click(self.pvc_loc["pvc_project_selector"])
        self.do_send_keys(self.pvc_loc["search-project"], text=project_name)

        self.wait_for_namespace_selection(project_name=project_name)

        logger.info(f"Search for {pvc_name} inside test project {project_name}")
        self.do_send_keys(self.pvc_loc["search_pvc"], text=pvc_name)

        time.sleep(2)
        # Using sleep to avoid StaleElementReferenceException. Use of explict wait or refreshing the page didn't help.
        if pvc_name == f"{pvc_name}-clone":
            time.sleep(2)
        logger.info(f"Click on PVC {pvc_name} and go to PVC {pvc_name} Page")
        self.do_click(get_element_type(pvc_name), enable_screenshot=True)

        logger.info("Checking status of Pvc")
        assert self.wait_until_expected_text_is_found(
            locator=self.pvc_loc["pvc-status"], expected_text="Bound"
        ), "PVC did not reach Bound state"

        pvc_size_new = f"{pvc_size} GiB"
        self.check_element_text(expected_text=pvc_size_new)
        logger.info(f"Verifying pvc size : {pvc_size_new}")

        pvc_access_mode_new = f"{access_mode}"
        self.check_element_text(expected_text=pvc_access_mode_new)
        logger.info(f"Verifying access mode : {pvc_access_mode_new}")

        if (
            sc_name != constants.DEFAULT_STORAGECLASS_CEPHFS
            and access_mode == "ReadWriteOnce"
        ):
            if (
                sc_name != constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
                and access_mode == "ReadWriteOnce"
            ):
                pvc_new_vol_mode = f"{vol_mode}"
                self.check_element_text(expected_text=pvc_new_vol_mode)
                logger.info(f"Verifying volume mode : {pvc_new_vol_mode}")

    def pvc_resize_ui(self, project_name, pvc_name, new_size):
        """
        Resizing pvc via UI
        Args:
            project_name (str): name of test project
            pvc_name (str): the name of pvc
            new_size (int): the new size of pvc (GB)
        """
        self.navigate_persistentvolumeclaims_page()

        logger.info(f"Search and Select test project {project_name}")
        self.do_click(self.pvc_loc["pvc_project_selector"])
        self.do_send_keys(self.pvc_loc["search-project"], text=project_name)

        self.wait_for_namespace_selection(project_name=project_name)

        logger.info(f"Search for {pvc_name} inside test project {project_name}")
        self.do_send_keys(self.pvc_loc["search_pvc"], text=pvc_name)

        logger.info(f"Go to PVC {pvc_name} Page")
        self.do_click(get_element_type(pvc_name))

        logger.info("Click on Actions")
        self.do_click(self.pvc_loc["pvc_actions"])

        logger.info("Click on Expand PVC from dropdown options")
        self.do_click(self.pvc_loc["expand_pvc"])

        logger.info("Clear the size of existing pvc")
        self.do_clear(self.pvc_loc["resize-value"])

        logger.info("Enter the size of new pvc")
        self.do_send_keys(self.pvc_loc["resize-value"], text=new_size)

        logger.info("Click on Expand Button")
        self.do_click(self.pvc_loc["expand-btn"], enable_screenshot=True)

    def verify_pvc_resize_ui(self, project_name, pvc_name, expected_capacity):
        """
        Verifying PVC resize via UI
        Args:
            project_name (str): name of test project
            pvc_name (str): the name of pvc
            expected_capacity (str): the new size of pvc (GiB)
        """
        self.navigate_persistentvolumeclaims_page()

        logger.info(f"Search and Select test project {project_name}")
        self.do_click(self.pvc_loc["pvc_project_selector"])
        self.do_send_keys(self.pvc_loc["search-project"], text=project_name)

        self.wait_for_namespace_selection(project_name=project_name)

        logger.info(f"Search for {pvc_name} inside test project {project_name}")
        self.do_send_keys(self.pvc_loc["search_pvc"], text=pvc_name)

        logger.info(f"Go to PVC {pvc_name} Page")
        self.do_click(get_element_type(pvc_name), enable_screenshot=True)

        is_expected_capacity = self.wait_until_expected_text_is_found(
            format_locator(self.pvc_loc["expected-capacity"], expected_capacity),
            expected_text=expected_capacity,
            timeout=300,
        )

        is_capacity = self.wait_until_expected_text_is_found(
            format_locator(self.pvc_loc["new-capacity"], expected_capacity),
            expected_text=expected_capacity,
            timeout=300,
        )

        if not is_expected_capacity:
            logger.error("Expected capacity text is not found")

        if not is_capacity:
            logger.error("Capacity text is not found")

        if is_expected_capacity and is_capacity:
            return True
        else:
            return False

    def delete_pvc_ui(self, pvc_name, project_name):
        """
        Delete pvc via UI
        Args:
            pvc_name (str): Name of the pvc
            project_name (str): name of test project
        """
        self.navigate_persistentvolumeclaims_page()

        logger.info(f"Select test project {project_name}")
        self.do_click(self.pvc_loc["pvc_project_selector"])
        self.do_send_keys(self.pvc_loc["search-project"], text=project_name)

        self.wait_for_namespace_selection(project_name=project_name)

        logger.info(f"Search for {pvc_name} inside test project {project_name}")
        self.do_send_keys(self.pvc_loc["search_pvc"], text=pvc_name)

        logger.info(f"Go to PVC {pvc_name} Page")
        self.do_click(get_element_type(pvc_name))

        logger.info("Click on Actions")
        self.do_click(self.pvc_loc["pvc_actions"])

        logger.info("Click on 'Delete PVC'")
        self.do_click(self.pvc_loc["pvc_delete"])

        logger.info("Confirm PVC Deletion")
        self.do_click(self.pvc_loc["confirm_pvc_deletion"], enable_screenshot=True)

    def pvc_clone_ui(
        self,
        project_name,
        pvc_name,
        cloned_pvc_access_mode=constants.ACCESS_MODE_RWO,
        cloned_pvc_name=None,
    ):
        """
        Clone PVC via UI

        Args:
            project_name (str): The name of project
            pvc_name (str): The name of PVC
            cloned_pvc_access_mode (str): Access mode for cloned PVC
            cloned_pvc_name (str): The name for cloned PVC

        """
        clone_name = cloned_pvc_name or f"{pvc_name}-clone"
        self.navigate_persistentvolumeclaims_page()

        logger.info(f"Search and select the project {project_name}")
        self.do_click(self.pvc_loc["pvc_project_selector"])
        self.do_send_keys(self.pvc_loc["search-project"], text=project_name)

        self.wait_for_namespace_selection(project_name=project_name)

        logger.info(f"Search for PVC {pvc_name}")
        self.do_send_keys(self.pvc_loc["search_pvc"], text=pvc_name)

        logger.info(f"Go to PVC {pvc_name} page")
        self.do_click(get_element_type(pvc_name))

        logger.info("Click on Actions")
        self.do_click(self.pvc_loc["pvc_actions"])

        logger.info("Click on Clone PVC from dropdown options")
        self.do_click(self.pvc_loc["clone_pvc"], enable_screenshot=True)

        logger.info("Clear the default name of clone PVC")
        ocs_version = version.get_semantic_ocs_version_from_config()
        if (
            self.ocp_version_full == version.VERSION_4_6
            and ocs_version == version.VERSION_4_6
        ):
            self.clear_with_ctrl_a_del(
                format_locator(self.pvc_loc["clone_name_input"], clone_name)
            )
        else:
            self.clear_with_ctrl_a_del(self.pvc_loc["clone_name_input"])

        logger.info("Enter the name of clone PVC")
        if (
            self.ocp_version_full == version.VERSION_4_6
            and ocs_version == version.VERSION_4_6
        ):
            self.do_send_keys(
                format_locator(self.pvc_loc["clone_name_input"], clone_name),
                text=clone_name,
            )
        else:
            self.do_send_keys(self.pvc_loc["clone_name_input"], text=clone_name)

        if (
            not self.ocp_version_full == version.VERSION_4_6
            and ocs_version == version.VERSION_4_6
        ):
            logger.info("Select Access Mode of clone PVC")
            self.do_click(self.pvc_loc[cloned_pvc_access_mode])

        logger.info("Click on Clone button")
        self.do_click(generic_locators["confirm_action"], enable_screenshot=True)
