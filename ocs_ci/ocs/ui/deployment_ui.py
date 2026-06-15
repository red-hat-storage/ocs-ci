import logging
import re
import time

from ocs_ci.ocs.ui.views import osd_sizes, OCS_OPERATOR, ODF_OPERATOR, LOCAL_STORAGE
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility import version
from ocs_ci.ocs.resources import csv
from ocs_ci.ocs.exceptions import TimeoutExpiredError, ConfigurationError
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.node import (
    get_worker_nodes,
    mark_masters_schedulable,
    get_all_nodes,
    get_node_objs,
    label_nodes,
)
from ocs_ci.utility.operators import LocalStorageOperator
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)


class DeploymentUI(PageNavigator):
    """
    Deployment OCS/ODF Operator via User Interface

    """

    def __init__(self):
        super().__init__()

    def verify_disks_lso_attached(self, timeout=600, sleep=20):
        """
        Verify Disks Attached

        Args:
            timeout (int): Time in seconds to wait
            sleep (int): Sampling time in seconds

        """
        osd_size = config.ENV_DATA.get("device_size", defaults.DEVICE_SIZE)
        number_worker_nodes = get_worker_nodes()
        capacity = int(osd_size) * len(number_worker_nodes)
        if capacity >= 1024:
            capacity_str = str(capacity / 1024).rstrip("0").rstrip(".") + " TiB"
        else:
            capacity_str = str(capacity) + " GiB"
        logger.info(f"Waiting for at least {capacity_str}")
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=self._check_disk_capacity,
            min_capacity_gib=capacity,
            take_screenshot=True,
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutExpiredError(f"Disks are not attached after {timeout} seconds")

    def _check_disk_capacity(self, min_capacity_gib, take_screenshot=False):
        """
        Check if the displayed disk capacity meets the minimum expected.

        Searches for capacity text (GiB or TiB) on the page and verifies
        the value is at least min_capacity_gib. This tolerates extra disks
        from previous runs.

        Args:
            min_capacity_gib (int): Minimum expected capacity in GiB.
            take_screenshot (bool): Whether to take a screenshot.

        Returns:
            bool: True if displayed capacity >= min_capacity_gib.
        """
        if take_screenshot:
            self.take_screenshot("disk_capacity_check")
        page_text = self.driver.find_elements(
            By.XPATH, "//*[contains(text(), 'GiB') or contains(text(), 'TiB')]"
        )
        for el in page_text:
            try:
                text = el.text.strip()
                parts = text.split()
                value = float(parts[0])
                unit = parts[1]
                capacity_gib = value * 1024 if unit == "TiB" else value
                if capacity_gib >= min_capacity_gib:
                    logger.info(
                        f"Found disk capacity: {text} (>= {min_capacity_gib} GiB)"
                    )
                    return True
            except (ValueError, IndexError):
                continue
        return False

    def install_ocs_operator(self):
        """
        Install OCS/ODF Opeartor
        """
        self.navigate_operatorhub_page()
        if self.ocp_version_semantic >= version.VERSION_4_20 and (
            self.driver.find_elements(*self.dep_loc["filter_operator_namespace"][::-1])
        ):
            self.do_send_keys(
                self.dep_loc["filter_operator_namespace"], text="openshift-operators"
            )
            self.do_click(
                self.dep_loc["openshift_operators_namespace"], enable_screenshot=True
            )
        self.do_send_keys(
            self.dep_loc["search_operators"], text=self.operator_name, timeout=60
        )
        logger.info(f"Choose {self.operator_name} Version")
        if self.operator_name is OCS_OPERATOR:
            self.do_click(self.dep_loc["choose_ocs_version"], enable_screenshot=True)
        elif self.operator_name is ODF_OPERATOR:
            self.do_click(self.dep_loc["click_odf_operator"], enable_screenshot=True)
        logger.info(f"Click Install {self.operator_name}")
        self.do_click(
            self.dep_loc["click_install_ocs"], enable_screenshot=True, timeout=60
        )
        self.page_has_loaded()
        if self.operator_name is ODF_OPERATOR:
            self.do_click(self.dep_loc["enable_console_plugin"], enable_screenshot=True)
        self.do_click(self.dep_loc["click_install_ocs_page"], enable_screenshot=True)
        if self.operator_name is ODF_OPERATOR:
            try:
                self.page_has_loaded()
                self.do_click(
                    locator=self.dep_loc["view_installed_operators_btn"],
                    enable_screenshot=True,
                    timeout=60,
                )
                self.do_click(locator=self.dep_loc["refresh_popup"], timeout=30)
            except Exception:
                logger.info("Post-install navigation/refresh failed, refreshing page")
            self.refresh_page()
            self.page_has_loaded()
            self._dismiss_welcome_modal()
        self.verify_operator_succeeded(operator=self.operator_name)

    def _dismiss_welcome_modal(self):
        """
        Dismiss the ODF welcome modal overlay if present.
        The modal appears after ODF console plugin loads and blocks
        sidebar navigation.
        """
        close_btn = self.get_elements(locator=self.dep_loc["dismiss_welcome_modal"])
        if close_btn:
            logger.info("Dismissing ODF welcome modal")
            close_btn[0].click()
            time.sleep(1)

    def refresh_popup(self, timeout=30):
        """
        Refresh PopUp
        """
        if self.check_element_text("Web console update is available"):
            logger.info("Web console update is available and Refresh web console")
            self.do_click(locator=self.dep_loc["refresh_popup"], timeout=timeout)

    def install_local_storage_operator(self):
        """
        Install local storage operator

        """
        if config.DEPLOYMENT.get("local_storage"):
            self.navigate_operatorhub_page()
            logger.info(f"Search {self.operator_name} Operator")
            self.do_send_keys(self.dep_loc["search_operators"], text="Local Storage")
            logger.info("Choose Local Storage Version")
            lso = LocalStorageOperator()
            self.do_click(
                locator=format_locator(
                    self.dep_loc["choose_local_storage_version"],
                    lso.catalog_name,
                ),
                enable_screenshot=True,
            )

            logger.info("Click Install LSO")
            self.do_click(self.dep_loc["click_install_lso"], enable_screenshot=True)
            self.do_click(
                self.dep_loc["click_install_lso_page"], enable_screenshot=True
            )
            self.verify_operator_succeeded(operator=LOCAL_STORAGE, timeout_install=300)

    def _is_page_crashed(self):
        """
        Check if the current page shows a crash (404 or error page).

        Returns:
            bool: True if the page displays a crash indicator.

        """
        return self.check_element_text("404") or self.check_element_text("An error")

    def install_storage_cluster(self):
        """
        Install StorageCluster/StorageSystem

        """

        ocs_version = version.get_semantic_ocs_version_from_config()
        if ocs_version >= version.VERSION_4_20:
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                logger.info("Navigate to Storage Cluster page")
                self.nav_storage_cluster_default_page()
                logger.info("Click Configure ODF")
                self.do_click(
                    locator=self.dep_loc["configure_odf"], enable_screenshot=True
                )
                self.page_has_loaded()
                self.do_click(
                    locator=self.dep_loc["setup_storage_cluster"],
                    enable_screenshot=True,
                )
                self.page_has_loaded()
                if not self._is_page_crashed():
                    break
                logger.warning(
                    f"Page crashed during Storage System creation "
                    f"(attempt {attempt}/{max_retries}). Refreshing and retrying."
                )
                self.take_screenshot()
                self.refresh_page()
                self.page_has_loaded()
            else:
                self.take_screenshot()
                raise ValueError(
                    "Page crashed at the time of Storage System creation "
                    f"after {max_retries} attempts"
                )
        elif ocs_version >= version.VERSION_4_19:
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                self.nav_storage_cluster_default_page()
                logger.info("Click on 'Storage Systems tab' under the dashboard")
                self.do_click(
                    locator=self.dep_loc["create_storage_cluster"],
                    enable_screenshot=True,
                )
                self.page_has_loaded()
                logger.info("Click on 'Create StorageSystem' button")
                self.do_click(
                    locator=self.dep_loc["storage_system_btn"],
                    enable_screenshot=True,
                )
                self.page_has_loaded()
                if not self._is_page_crashed():
                    break
                logger.warning(
                    f"Page crashed during Storage System creation "
                    f"(attempt {attempt}/{max_retries}). Refreshing and retrying."
                )
                self.take_screenshot()
                self.refresh_page()
                self.page_has_loaded()
            else:
                self.take_screenshot()
                raise ValueError(
                    "Page crashed at the time of Storage System creation "
                    f"after {max_retries} attempts"
                )
        else:
            if self.operator_name == ODF_OPERATOR:
                self.navigate_installed_operators_page()
                self.choose_expanded_mode(
                    mode=True, locator=self.dep_loc["drop_down_projects"]
                )
                self.do_click(
                    self.dep_loc["choose_all_projects"], enable_screenshot=True
                )
            else:
                self.search_operator_installed_operators_page(
                    operator=self.operator_name
                )

            logger.info(f"Click on {self.operator_name} on 'Installed Operators' page")
            if self.operator_name == ODF_OPERATOR:
                logger.info("Click on Create StorageSystem")
                self.do_click(
                    locator=self.dep_loc["odf_operator_installed"],
                    enable_screenshot=True,
                )
                time.sleep(5)
                self.do_click(
                    locator=self.dep_loc["storage_system_tab"], enable_screenshot=True
                )
            elif self.operator_name == OCS_OPERATOR:
                logger.info("Click on Create StorageCluster")
                self.do_click(
                    locator=self.dep_loc["ocs_operator_installed"],
                    enable_screenshot=True,
                )
                time.sleep(5)
                self.do_click(
                    locator=self.dep_loc["storage_cluster_tab"], enable_screenshot=True
                )
                if self.check_element_text("404"):
                    raise ValueError(
                        "Page crashed at the time of Storage System creation"
                    )
                self.do_click(
                    locator=self.dep_loc["create_storage_cluster"],
                    enable_screenshot=True,
                )
        if self._is_page_crashed():
            raise ValueError("Page crashed at the time of Storage System creation")
        if config.ENV_DATA.get("mcg_only_deployment", False):
            self.install_mcg_only_cluster()
        elif config.DEPLOYMENT.get("local_storage"):
            self.install_lso_cluster()
        else:
            self.install_internal_cluster()

    def install_mcg_only_cluster(self):
        """
        Install MCG ONLY cluster via UI

        """
        logger.info("Install MCG ONLY cluster via UI")
        if self.ocp_version == "4.9":
            self.do_click(self.dep_loc["advanced_deployment"])
        self.do_click(self.dep_loc["expand_advanced_mode"], enable_screenshot=True)
        if self.ocp_version == "4.9":
            self.do_click(self.dep_loc["mcg_only_option"], enable_screenshot=True)
        elif self.ocp_version in ("4.10", "4.11", "4.12"):
            self.do_click(self.dep_loc["mcg_only_option_4_10"], enable_screenshot=True)
        if config.DEPLOYMENT.get("local_storage"):
            self.install_lso_cluster()
        else:
            self.do_click(self.dep_loc["next"], enable_screenshot=True)
        self.do_click(self.dep_loc["next"], enable_screenshot=True)
        self.create_storage_cluster()

    def configure_in_transit_encryption(self):
        """
        Configure in_transit_encryption

        """
        if config.ENV_DATA.get("in_transit_encryption"):
            logger.info("Enable in-transit encryption")
            self.select_checkbox_status(
                status=True, locator=self.dep_loc["enable_in_transit_encryption"]
            )

    def install_lso_cluster(self):
        """
        Install LSO cluster via UI

        """
        if config.DEPLOYMENT.get("ec_default_pools"):
            if config.ENV_DATA.get("mark_masters_schedulable", True):
                mark_masters_schedulable()
                logger.info("Labeling all nodes as storage nodes")
                nodes = get_all_nodes()
                node_objs = get_node_objs(nodes)
                label_nodes(nodes=node_objs, label=constants.OPERATOR_NODE_LABEL)
                time.sleep(10)

        logger.info("Click Internal - Attached Devices")
        if self.operator_name == ODF_OPERATOR:
            self.do_click(self.dep_loc["choose_lso_deployment"], enable_screenshot=True)
        else:
            self.do_click(
                self.dep_loc["internal-attached_devices"], enable_screenshot=True
            )
            logger.info("Click on All nodes")
            self.do_click(self.dep_loc["all_nodes_lso"], enable_screenshot=True)
        self.do_click(self.dep_loc["next"], enable_screenshot=True)

        # skipping optional settings such as NFS,
        # RBD as the default StorageClass, Set default StorageClass for virtualization,
        # Use external PostgreSQL, Automatic backup
        # In future we'll need to automate them
        self.do_click(self.dep_loc["next"], enable_screenshot=True)

        logger.info(
            f"Configure Volume Set Name and Storage Class Name as {constants.LOCAL_BLOCK_RESOURCE}"
        )
        # setting a large
        self.do_send_keys(
            locator=self.dep_loc["lv_name"],
            text=constants.LOCAL_BLOCK_RESOURCE,
            timeout=600,
        )
        self.take_screenshot()
        self.do_send_keys(
            locator=self.dep_loc["sc_name"], text=constants.LOCAL_BLOCK_RESOURCE
        )
        if self.operator_name == OCS_OPERATOR:
            logger.info("Select all nodes on 'Create Storage Class' step")
            self.do_click(
                locator=self.dep_loc["all_nodes_create_sc"], enable_screenshot=True
            )
        if config.ENV_DATA.get("platform") not in [
            constants.BAREMETAL_PLATFORM,
            constants.HCI_BAREMETAL,
        ]:
            self.take_screenshot()
            self.verify_disks_lso_attached()
            timeout_next = 60
        else:
            timeout_next = 600
        self.do_click(
            self.dep_loc["next"], enable_screenshot=True, timeout=timeout_next
        )

        logger.info("Confirm new storage class")
        self.do_click(self.dep_loc["yes"], enable_screenshot=True)
        if config.ENV_DATA.get("mcg_only_deployment", False):
            return

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=self.check_element_text,
            expected_text="Memory",
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutExpiredError("Nodes not found after 600 seconds")

        self.enable_taint_nodes()

        self.configure_performance()

        sample = TimeoutSampler(
            timeout=700,
            sleep=40,
            func=self.wait_next_button_lso,
        )
        if not sample.wait_for_func_status(result=True):
            self.take_screenshot()
            raise TimeoutExpiredError(
                "Next button on LSO is not clickable after 700 seconds"
            )

        if config.DEPLOYMENT.get("odf_forceful_deployment"):
            if "enable_forceful_deployment" not in self.dep_loc:
                raise ConfigurationError(
                    "Forceful deployment UI automation is supported only on ODF/OCP 4.22+."
                )
            self.enable_forceful_deployment()

        if config.DEPLOYMENT.get("ec_default_pools"):
            if "use_erasure_coding" not in self.dep_loc:
                raise ConfigurationError(
                    "Erasure coding UI automation is supported only on ODF/OCP 4.22+."
                )
            if not self.enable_erasure_coding():
                raise ConfigurationError(
                    "Erasure coding is unavailable in the Advanced Settings step."
                )

        self.do_click(
            locator=self.dep_loc["next"], enable_screenshot=True, timeout=timeout_next
        )

        self.configure_encryption()

        self.configure_data_protection()

        self.create_storage_cluster()

    def wait_next_button_lso(self):
        try:
            self.do_click(self.dep_loc["next"], enable_screenshot=True, timeout=20)
        except Exception as e:
            logger.error(f"Next button on LSO error: {e}")
            return False
        return True

    def install_internal_cluster(self):
        """
        Install Internal Cluster

        """
        logger.info("Deployment type is 'Full Deployment' as default selection")
        if self.operator_name == ODF_OPERATOR:
            self.do_click(
                locator=self.dep_loc["internal_mode_odf"], enable_screenshot=True
            )
        else:
            self.do_click(locator=self.dep_loc["internal_mode"], enable_screenshot=True)
        ocs_version = version.get_semantic_ocs_version_from_config()
        if ocs_version >= version.VERSION_4_20:
            logger.info("Storage class is chosen automatically")
        else:
            logger.info("Configure Storage Class (thin-csi on vmware, gp2 on aws)")
            self.do_click(
                locator=self.dep_loc["storage_class_dropdown"], enable_screenshot=True
            )
            self.do_click(
                locator=self.dep_loc[self.storage_class],
                enable_screenshot=True,
                copy_dom=True,
            )

        if self.operator_name == ODF_OPERATOR:
            self.do_click(locator=self.dep_loc["next"], enable_screenshot=True)

        if ocs_version >= version.VERSION_4_21:
            self.do_click(locator=self.dep_loc["next"], enable_screenshot=True)

        self.configure_osd_size()

        self.configure_performance()

        logger.info("Select all worker nodes")
        self.select_checkbox_status(status=True, locator=self.dep_loc["all_nodes"])

        self.enable_taint_nodes()

        if self.ocp_version == "4.6" and config.ENV_DATA.get("encryption_at_rest"):
            self.do_click(
                locator=self.dep_loc["enable_encryption"], enable_screenshot=True
            )

        if self.ocp_version_semantic >= version.VERSION_4_7:
            logger.info("Next on step 'Select capacity and nodes'")
            self.do_click(locator=self.dep_loc["next"], enable_screenshot=True)
            self.configure_in_transit_encryption()
            self.configure_encryption()

        self.configure_data_protection()

        self.create_storage_cluster()

    def configure_performance(self):
        """
        Configure performance mode

        """
        mode = config.ENV_DATA.get("performance_profile")
        if self.ocs_version_semantic >= version.VERSION_4_15 and mode in (
            "lean",
            "performance",
        ):
            self.do_click(
                locator=self.dep_loc["drop_down_performance"], enable_screenshot=True
            )
            if mode == "lean":
                self.do_click(locator=self.dep_loc["lean_mode"])
            elif mode == "performance":
                self.do_click(locator=self.dep_loc["performance_mode"])

    def create_storage_cluster(self):
        """
        Review and Create StorageCluster/StorageSystem

        """
        logger.info("Create storage cluster on 'Review and create' page")
        if self.operator_name is OCS_OPERATOR:
            self.do_click(
                locator=self.dep_loc["create_on_review"], enable_screenshot=True
            )
        elif self.operator_name is ODF_OPERATOR:
            self.do_click(
                locator=self.dep_loc["create_storage_system"], enable_screenshot=True
            )
        logger.info("Sleep 10 second after click on 'create storage cluster'")
        time.sleep(10)

    def check_forceful_deployment(self):
        """
        Check if the Enable forceful deployment checkbox is currently selected.

        Returns:
            bool: True if the checkbox is checked, False otherwise.
        """
        return self.get_checkbox_status(
            locator=self.dep_loc["enable_forceful_deployment"]
        )

    def enable_forceful_deployment(self):
        """
        Enable the forceful deployment checkbox and type CONFIRM in the
        confirmation input that appears.
        """
        logger.info("Enable forceful deployment")
        self.select_checkbox_status(
            status=True, locator=self.dep_loc["enable_forceful_deployment"]
        )
        self.do_send_keys(
            locator=self.dep_loc["forceful_deployment_confirmation"],
            text="CONFIRM",
        )
        self.take_screenshot("odf_forceful_deployment_enabled")

    def is_element_greyed_out(self, locator):
        """
        Check if a UI element is disabled (greyed out).

        Args:
            locator (tuple): (selector str, By type) of the element to check.

        Returns:
            bool: True if the element has the disabled attribute, False otherwise.
        """
        return self.get_element_attribute(locator, "disabled") is not None

    def check_erasure_coding(self):
        """
        Check if the Use erasure coding checkbox is currently selected.

        Returns:
            bool: True if the checkbox is checked, False otherwise or when
                ec_default_pools is not set in config.
        """
        if not config.DEPLOYMENT.get("ec_default_pools"):
            return False
        return self.get_checkbox_status(locator=self.dep_loc["use_erasure_coding"])

    def _select_ec_scheme(self, k, m):
        """
        Click the radio button for the EC scheme matching k+m in the scheme table.

        Args:
            k (int): Number of data chunks (ec_data_chunks).
            m (int): Number of coding chunks (ec_coding_chunks).
        """
        scheme = f"{k}+{m}"
        logger.info(f"Selecting EC scheme {scheme} from the erasure coding table")
        self.do_click(
            locator=format_locator(self.dep_loc["ec_scheme_radio"], scheme),
            enable_screenshot=True,
        )

    def _parse_ec_scheme_table(self):
        """
        Parse the EC scheme table rows from the UI into structured data.

        Returns:
            list[tuple]: List of (scheme_text, k, m, capacity_value) for each
                parseable row. scheme_text is e.g. "2+2", capacity_value is
                the effective capacity in TiB as a float.

        Raises:
            ConfigurationError: If the table has no rows.
        """
        rows = self.get_elements(locator=self.dep_loc["ec_scheme_table_rows"])
        if not rows:
            raise ConfigurationError(
                "EC scheme table has no rows — cannot verify effective capacity"
            )

        scheme_cell = self.dep_loc["_ec_row_scheme_cell"]
        cap_cell = self.dep_loc["_ec_row_effective_capacity_cell"]
        parsed = []
        for row in rows:
            raw_scheme = row.find_element(scheme_cell[1], scheme_cell[0]).text
            match = re.match(r"(\d+\+\d+)", raw_scheme.strip())
            if not match:
                logger.warning(f"Could not parse EC scheme from: '{raw_scheme}'")
                continue
            scheme_text = match.group(1)
            capacity_text = row.find_element(cap_cell[1], cap_cell[0]).text
            ki, mi = (int(x) for x in scheme_text.split("+"))
            capacity_value = float(capacity_text.split()[0])
            parsed.append((scheme_text, ki, mi, capacity_value))
        return parsed

    def _verify_ec_effective_capacity(self, k, m):
        """
        Cross-validate the Effective capacity values shown in the EC scheme
        table.  Derives total raw capacity from the first row and verifies
        every other row matches ``total_raw * k_i / (k_i + m_i)`` within
        0.02 TiB tolerance.

        Args:
            k (int): Number of data chunks of the selected scheme.
            m (int): Number of coding chunks of the selected scheme.
        """
        scheme = f"{k}+{m}"
        parsed = self._parse_ec_scheme_table()

        available_schemes = [p[0] for p in parsed]
        if scheme not in available_schemes:
            raise ConfigurationError(
                f"Selected EC scheme {scheme} not found in the table. "
                f"Available: {available_schemes}"
            )

        ref_scheme, ref_k, ref_m, ref_cap = parsed[0]
        total_raw = ref_cap * (ref_k + ref_m) / ref_k
        logger.info(
            f"EC table: derived total raw capacity = {total_raw:.2f} TiB "
            f"(from reference scheme {ref_scheme})"
        )

        for row_scheme, ki, mi, displayed_cap in parsed:
            expected_cap = total_raw * ki / (ki + mi)
            if abs(displayed_cap - expected_cap) > 0.02:
                raise ConfigurationError(
                    f"EC scheme {row_scheme}: expected effective capacity "
                    f"~{expected_cap:.2f} TiB, got {displayed_cap} TiB"
                )
            logger.info(
                f"EC scheme {row_scheme}: effective capacity = {displayed_cap} TiB "
                f"(expected ~{expected_cap:.2f} TiB)"
            )

    def is_erasure_coding_disabled(self):
        """
        Check whether the Use erasure coding checkbox is greyed out (disabled).

        The checkbox is disabled when the cluster does not meet EC prerequisites,
        e.g. fewer than 4 nodes or no LSO on the cluster.

        Returns:
            bool: True if the checkbox is disabled, False if it can be clicked.
        """
        disabled = self.is_element_greyed_out(
            locator=self.dep_loc["use_erasure_coding"]
        )
        if disabled:
            logger.warning(
                "Use erasure coding checkbox is greyed out — cluster may not meet "
                "EC prerequisites (e.g. fewer than 4 nodes or no LSO)."
            )
        return disabled

    def enable_erasure_coding(self):
        """
        Enable the Use erasure coding checkbox, select the EC scheme from config,
        and verify the displayed effective capacity.

        No-op when the checkbox is greyed out (not enough nodes to support EC or other).
        EC scheme is derived from ec_data_chunks (k) and ec_coding_chunks (m).

        Returns:
            bool or None: True if erasure coding was successfully enabled,
                None if the checkbox is disabled (greyed out).
        """
        if self.is_erasure_coding_disabled():
            return None
        logger.info("Enable Use erasure coding")
        self.select_checkbox_status(
            status=True, locator=self.dep_loc["use_erasure_coding"]
        )
        k = config.DEPLOYMENT.get("ec_data_chunks", 2)
        m = config.DEPLOYMENT.get("ec_coding_chunks", 1)
        self._select_ec_scheme(k, m)
        self._verify_ec_effective_capacity(k, m)
        self.take_screenshot("erasure_coding_enabled")
        return True

    def configure_encryption(self):
        """
        Configure Encryption

        """
        if config.ENV_DATA.get("encryption_at_rest"):
            logger.info("Enable OSD Encryption")
            self.select_checkbox_status(
                status=True, locator=self.dep_loc["enable_encryption"]
            )

            logger.info("Cluster-wide encryption")
            self.select_checkbox_status(
                status=True, locator=self.dep_loc["wide_encryption"]
            )
        self.do_click(self.dep_loc["next"], enable_screenshot=True)

    def configure_data_protection(self):
        """
        Configure Data Protection

        """
        if (
            self.ocs_version_semantic >= version.VERSION_4_14
            and self.ocs_version_semantic <= version.VERSION_4_17
        ):
            self.do_click(self.dep_loc["next"], enable_screenshot=True)

    def enable_taint_nodes(self):
        """
        Enable taint Nodes

        """
        logger.info("Enable taint Nodes")
        if (
            self.ocp_version_semantic >= version.VERSION_4_10
            and config.DEPLOYMENT.get("ocs_operator_nodes_to_taint") > 0
        ):
            self.select_checkbox_status(
                status=True, locator=self.dep_loc["enable_taint_node"]
            )

    def configure_osd_size(self):
        """
        Configure OSD Size
        """

        ocs_version = version.get_semantic_ocs_version_from_config()
        device_size = str(config.ENV_DATA.get("device_size"))

        # Mapping from GiB numeric values to TiB strings for versions 4.19+
        size_mapping = {
            "512": "0.5 TiB",
            "1024": "1 TiB",
            "2048": "2 TiB",
            "4096": "4 TiB",
            "8192": "8 TiB",
        }

        # For versions > 4.18, convert numeric values to TiB format
        if ocs_version > version.VERSION_4_18 and device_size in size_mapping:
            osd_size = size_mapping[device_size]
        elif device_size in osd_sizes:
            osd_size = device_size
        else:
            # Default fallback
            osd_size = "512" if ocs_version <= version.VERSION_4_18 else "0.5 TiB"
        logger.info(f"Configure OSD Capacity {osd_size}")
        if self.ocp_version_semantic >= version.VERSION_4_11:
            self.do_click(self.dep_loc["osd_size_dropdown"], enable_screenshot=True)
            logger.info(
                "Requested capacity dropdown expanded for selecting OSD capacity "
            )
        else:
            self.choose_expanded_mode(
                mode=True, locator=self.dep_loc["osd_size_dropdown"]
            )
        self.do_click(locator=self.dep_loc[osd_size], enable_screenshot=True)

    def check_odf_operators_succeeded(self, operator):
        """
        Check that exactly 2 rows whose data-test-operator-row contains the given
        operator name show a Succeeded status-text span.

        This targets the operator rows directly via data-test-operator-row, so it is
        not affected by the page-level name filter or by other operators such as
        Package Server that also show Succeeded.

        Args:
            operator (str): operator name substring to match, e.g. "OpenShift Data Foundation"

        Returns:
            bool: True when exactly 2 matching operator rows show Succeeded
        """
        from ocs_ci.ocs.ui.helpers_ui import format_locator

        locator = format_locator(self.dep_loc["odf_operator_row_succeeded"], operator)
        elements = self.driver.find_elements(locator[1], locator[0])
        return len(elements) == 2

    def verify_operator_succeeded(
        self, operator=OCS_OPERATOR, timeout_install=600, sleep=20
    ):
        """
        Verify Operator Installation

        Args:
            operator (str): type of operator
            timeout_install (int): Time in seconds to wait
            sleep (int): Sampling time in seconds

        """
        self.search_operator_installed_operators_page(operator=operator)
        time.sleep(5)
        if operator == LOCAL_STORAGE:
            sample = TimeoutSampler(
                timeout=timeout_install,
                sleep=sleep,
                func=self.check_element_text,
                expected_text="Succeeded",
            )
        elif self.ocs_version_semantic > version.VERSION_4_15:
            sample = TimeoutSampler(
                timeout=timeout_install,
                sleep=sleep,
                func=self.check_odf_operators_succeeded,
                operator=operator,
            )
        else:
            sample = TimeoutSampler(
                timeout=timeout_install,
                sleep=sleep,
                func=self.check_element_text,
                expected_text="Succeeded",
            )
        if not sample.wait_for_func_status(result=True):
            logger.error(
                f"{operator} Installation status is not Succeeded after {timeout_install} seconds"
            )
            self.take_screenshot()
            raise TimeoutExpiredError(
                f"{operator} Installation status is not Succeeded after {timeout_install} seconds"
            )
        self.take_screenshot()
        logger.info(f"{operator} operator installed Successfully.")

    def search_operator_installed_operators_page(self, operator=OCS_OPERATOR):
        """
        Search Operator on Installed Operators Page

        Args:
            operator (str): type of operator

        """
        self.navigate_operatorhub_page()
        self.navigate_installed_operators_page()
        logger.info(f"Search {operator} operator installed")
        if self.ocp_version_semantic >= version.VERSION_4_7:
            sample = TimeoutSampler(
                timeout=100,
                sleep=10,
                func=self.check_element_text,
                expected_text=operator,
            )
            if not sample.wait_for_func_status(result=True):
                logger.error(
                    f"The {operator} installation did not start after 100 seconds"
                )
                self.take_screenshot()
            self.do_send_keys(
                locator=self.dep_loc["search_operator_installed"],
                text=operator,
            )
        # https://bugzilla.redhat.com/show_bug.cgi?id=1899200
        elif self.ocp_version == "4.6":
            self.do_click(self.dep_loc["project_dropdown"], enable_screenshot=True)
            self.do_click(self.dep_loc[operator], enable_screenshot=True)
        elif self.ocp_version == "4.9" and operator != "Local Storage":
            self.choose_expanded_mode(
                mode=True, locator=self.dep_loc["drop_down_projects"]
            )
            default_projects_is_checked = self.driver.find_element(
                By.ID, "no-label-switch-on"
            ).is_selected()
            if not default_projects_is_checked:
                logger.info("Show default projects")
                self.do_click(
                    self.dep_loc["enable_default_porjects"], enable_screenshot=True
                )
            self.do_click(
                self.dep_loc["choose_openshift-storage_project"], enable_screenshot=True
            )

    def install_ocs_ui(self):
        """
        Install OCS/ODF via UI

        """
        self.install_local_storage_operator()
        if not csv.get_csvs_start_with_prefix(
            defaults.ODF_OPERATOR_NAME, config.ENV_DATA["cluster_namespace"]
        ):
            self.install_ocs_operator()
        if not config.UPGRADE.get("ui_upgrade"):
            self.install_storage_cluster()
