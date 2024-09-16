import json
import random
import time
from abc import ABC

import pandas as pd
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.errorhandler import ErrorHandler

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import (
    ON_PREM_PLATFORMS,
    CLOUD_PLATFORMS,
    HCI_PROVIDER_CLIENT_PLATFORMS,
)
from ocs_ci.ocs.exceptions import IncorrectUiOptionRequested
from ocs_ci.ocs.node import get_node_names
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.base_ui import BaseUI, logger
from ocs_ci.ocs.ui.odf_topology import TopologyUiStr, OdfTopologyHelper
from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import (
    DataFoundationDefaultTab,
    DataFoundationTabBar,
)
from ocs_ci.ocs.ui.workload_ui import WorkloadUi
from ocs_ci.utility.retry import retry


class TopologySidebar(BaseUI):
    """
    Class a child to AbstractTopologyView (it has base_ui methods) and may be used only if Topology view is opened
    """

    def __init__(self):
        BaseUI.__init__(self)

    def is_alert_tab_present(self) -> bool:
        """
        Useful to check condition: Alert tab should not be present in External mode
        """
        return bool(self.get_elements(self.topology_loc["alerts_sidebar_tab"]))

    def open_side_bar_of_entity(self, entity_name: str = None, canvas: bool = False):
        """
        Opens the sidebar of an entity in the topology view.

        Args:
            entity_name (str, optional): Name of the entity to open the side bar for.
            canvas (bool, optional): Flag indicating whether to click on the topology graph canvas.

        Note:
            If `canvas` is True, the method clicks on the topology graph canvas to open the side bar.
            Otherwise, it searches for the entity and clicks on it. The method attempts to open the
            sidebar up to three times, zooming out the topology view if necessary.

        """
        if canvas:
            self.do_click(self.topology_loc["topology_graph"])
        else:
            from ocs_ci.ocs.ui.helpers_ui import format_locator

            loc = format_locator(self.topology_loc["select_entity"], entity_name)

            for i in range(1, 4):
                try:
                    self.do_click(loc)
                    break
                except NoSuchElementException:
                    logger.info("zooming out topology view")
                    self.do_click(self.topology_loc["zoom_out"])
                    self.page_has_loaded(module_loc=self.topology_loc["topology_graph"])
                    logger.info(f"try read topology again. attempt number {i} ")
            logger.info(f"Entity {entity_name} sidebar is opened")

    def close_sidebar(self, soft=False):
        """
         Closes the sidebar in the topology view.

        Args:
            soft (bool, optional): If True and the sidebar is visible, closes the sidebar with a transition.
                                   If False (default) or the sidebar is not visible, closes the sidebar immediately and
                                   may get exception if sidebar is not open.

        """

        if soft and self.get_elements(self.topology_loc["close_sidebar"]):
            # sidebar has a slow transition and may be visible for a moment after it was closed
            try:
                self.do_click(
                    self.topology_loc["close_sidebar"], enable_screenshot=True
                )
            except TimeoutException:
                pass
        elif not soft:
            self.do_click(self.topology_loc["close_sidebar"], enable_screenshot=True)
        else:
            return
        logger.info("Sidebar is closed")

    def is_node_down_alert_in_alerts_ui(self, entity=None, read_canvas_alerts=False):
        """
        Checks if a NodeDown alert is present in the UI alerts.

        Args:
            entity (str, optional): Entity name to filter alerts (default: None).
            read_canvas_alerts (bool, optional): Whether to read alerts from the canvas (default: False).

        Returns:
            bool: if the node down alert visible in Alerts tab of the Topology
        """
        alerts_dict = retry(TimeoutException, tries=3, delay=5)(
            self.read_alerts_procedure
        )(entity, read_canvas_alerts)
        return (
            "Critical" in alerts_dict
            and constants.ALERT_NODEDOWN in alerts_dict["Critical"]
        )

    def read_alerts_procedure(self, entity=None, read_canvas_alerts=False):
        """
        Reads alerts for a specific entity using the procedure.

        This method follows a procedure to read alerts for the specified entity. It closes the sidebar,
        opens the sidebar of the entity (optionally reading alerts in the canvas), opens the alerts tab,
        reads the alerts, and finally closes the sidebar.

        Args:
            entity (str): Optional. The entity for which alerts need to be read.
            read_canvas_alerts (bool): Optional. Indicates whether to read alerts in the canvas or not.

        Returns:
            list: A list of alerts detected in side-bar

        """
        self.close_sidebar(soft=True)
        self.open_side_bar_of_entity(entity, canvas=read_canvas_alerts)
        self.open_alerts_tab()
        alerts_detected = self.read_alerts()
        self.close_sidebar()
        return alerts_detected

    def open_alerts_tab(self):
        """
        Method opens the alerts tab in the user interface.
        """
        self.do_click(self.topology_loc["alerts_sidebar_tab"], enable_screenshot=True)
        logger.info("Alerts tab is open")

    def read_alerts(self) -> dict:
        """
        Reads alerts from the alert tab and retrieves their details.

        Returns:
            dict: Dictionary containing the alerts and their corresponding levels.
            Each alert level is associated with a list of alert titles.

        Note:
            Alerts tab should be opened.
            The alert levels are expanded to read the titles and then shrunk back.

        """
        logger.info("reading alerts from the alert tab")
        alerts_dict = dict()
        alerts_lvl_to_num = self.get_number_of_alerts()
        alert_levels_exists = [
            alert for alert, value in alerts_lvl_to_num.items() if value > 0
        ]
        from ocs_ci.ocs.ui.helpers_ui import format_locator

        for alert_lvl in alert_levels_exists:

            # expand each alert and read number of alerts
            # to work with large number of alerts need to scroll and pick all titles

            self.do_click(
                format_locator(self.topology_loc["alert_list_expand_arrow"], alert_lvl)
            )
            alerts_titles = self.get_elements(
                self.topology_loc["alerts_sidebar_alert_title"]
            )

            list_of_alert_per_alert_lvl = []
            for alert_titles in alerts_titles:
                alert_text = alert_titles.text
                # check if alert has text, it may be graphical element such as "!"-icon
                if alert_text.strip():
                    list_of_alert_per_alert_lvl.append(alert_text)

            alerts_dict[alert_lvl] = list_of_alert_per_alert_lvl
            # shrink alert lvl back
            self.do_click(
                format_locator(self.topology_loc["alert_list_expand_arrow"], alert_lvl)
            )
        logger.info(f"\n{json.dumps(alerts_dict, indent=4)}")
        return alerts_dict

    def open_details_tab(self):
        """
        Opens the details tab in the UI.
        """
        self.do_click(self.topology_loc["details_sidebar_tab"], enable_screenshot=True)
        logger.info("Details tab is open")

    def open_resources_tab(self):
        """
        Opens the resources tab in the UI.
        """
        self.do_click(
            self.topology_loc["resources_sidebar_tab"], enable_screenshot=True
        )
        logger.info("Resources tab is open")

    def open_observe_tab(self):
        """
        Opens the observe tab in the UI.
        """
        self.do_click(self.topology_loc["observe_sidebar_tab"], enable_screenshot=True)
        logger.info("Observe tab is open")

    def get_number_of_alerts(self):
        """
        Retrieves the number of alerts categorized by severity level.

        Returns:
            dict: Dictionary containing the number of alerts for each severity level:
                  {'Critical': <critical_alerts>, 'Warning': <warning_alerts>, 'Info': <info_alerts>}
        """
        alerts_dict = {"Critical": -1, "Warning": -1, "Info": -1}
        from ocs_ci.ocs.ui.helpers_ui import format_locator

        for alert_lvl, val in alerts_dict.items():

            alert_num = self.get_element_text(
                format_locator(self.topology_loc["number_of_alerts"], alert_lvl)
            )
            alerts_dict[alert_lvl] = int(alert_num)
        return alerts_dict


class AbstractTopologyView(ABC, TopologySidebar):
    """
    Abstract class for ODF Topology layers operated by Selenium webdriver

    Note:
        This class should not be instantiated directly. Instead, it serves as a base class for concrete
        implementation classes.
    """

    def __init__(self):
        TopologySidebar.__init__(self)

    @property
    def nodes_view(self):
        """
        Property to create OdfTopologyNodesView only once per cluster.

        This property ensures that an instance of OdfTopologyNodesView is created only once per cluster.
        It checks if the instance already exists as the 'cluster_topology' attribute, and if not,
        creates a new instance. The property then returns the instance.

        Returns:
            OdfTopologyNodesView: The instance of OdfTopologyNodesView.

        Note:
            The property assumes the availability of the OdfTopologyNodesView class.

        """
        if not hasattr(self, "cluster_topology"):
            self.cluster_topology = OdfTopologyNodesView()
        return self.cluster_topology

    def is_cluster_in_danger(self) -> bool:
        """
        Method checks whether the cluster is red-labeled on ODF Topology canvas or not
        """
        return bool(self.get_elements(self.topology_loc["cluster_in_danger"]))

    def read_presented_topology(self):
        """
        Reads and retrieves the presented topology from the current view.

        The method attempts to read the presented topology by zooming out up to three times if the topology
        is larger than the browser window can fit. After successfully reading the topology, it updates the
        status and logs the entity names and their corresponding statuses. Finally, it returns the topology
        DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing the presented topology with entity names and statuses.

        Example:
            topology = read_presented_topology()
            # Returns a DataFrame with entity names and statuses:
            #   entity_name   |  entity_status
            # ----------------+-----------------
            #   Node 1        |  Ready
            #   Node 2        |  Not Ready
            #   Node 3        |  Ready
            #   ...
        """
        # if topology is larger than browser window can fit we need to zoom out, similarly to user actions
        for i in range(1, 4):
            try:
                self.initiate_topology_df(reinit=False)
                break
            except NoSuchElementException:
                self.zoom_out_view()
                self.page_has_loaded(module_loc=self.topology_loc["topology_graph"])
                logger.info(
                    f"try zoom out and read topology again. attempt number {i} "
                )

        self.update_topology_status()
        logger.info(
            "\n"
            + self.topology_df[["entity_name", "entity_status"]].to_markdown(
                headers="keys", index=False, tablefmt="grid"
            )
        )
        return self.topology_df

    def get_nested_deployments_of_node_from_df_ui(self, node_name):
        """
        Retrieves the nested deployments of a node from the UI dataframe.

        Args:
            node_name (str): Name of the node.

        Returns:
            dict: Dictionary representing the nested deployments of the node.

        Example:
            get_nested_deployments_of_node_from_df_ui("node-1")
            # Returns {'entity_name': 'nested-deployment-1', 'entity_status': 'Running', ...}
        """
        # get index of the node
        index = self.topology_df[self.topology_df["entity_name"] == node_name].index[0]
        return self.topology_df.at[index, "nested_deployments"]

    def get_nested_deployment_names_of_node_from_df_ui(self, node_name):
        """
        Retrieves the names of nested deployments associated with a node from the UI dataframe.

        Args:
            node_name (str): Name of the node.

        Returns:
            list: List of deployment names associated with the specified node.

        Example:
            get_nested_deployment_names_of_node_from_df_ui("my-node")
            # Returns ['deployment-1', 'deployment-2', 'deployment-3']
        """
        return list(
            self.get_nested_deployments_of_node_from_df_ui(node_name)["entity_name"]
        )

    def update_topology_status(self):
        """
        Updates the status of entities in the topology.

        This method iterates over the rows of the topology dataframe and updates the entity status, status XPath,
        select node XPath, and navigate into XPath (if applicable) for each entity.

        Note:
            The method assumes the availability of the topology dataframe, certain locators,
            and a brief pause of 0.1 seconds between iterations.

        """

        from ocs_ci.ocs.ui.helpers_ui import format_locator

        df = self.topology_df
        for index, row in df.iterrows():
            entity_name = row["entity_name"]
            df.loc[
                df["entity_name"] == entity_name, "entity_status"
            ] = self._get_status_of_entity(entity_name)
            df.loc[df["entity_name"] == entity_name, "status_xpath"] = format_locator(
                self.topology_loc["node_status_class_axis"], entity_name
            )[0]
            df.loc[
                df["entity_name"] == entity_name, "select_node_xpath"
            ] = format_locator(self.topology_loc["select_entity"], entity_name)[0]
            # navigate_into_xpath is applicable only for node level, since we can not navigate into deployment
            if "navigate_into_xpath" in df.columns:
                df.loc[
                    df["entity_name"] == entity_name, "navigate_into_xpath"
                ] = format_locator(
                    self.topology_loc["enter_into_entity_arrow"], entity_name
                )[
                    0
                ]
            time.sleep(0.1)

    def initiate_topology_df(self, reinit: bool = True):
        """
        Initializes the topology DataFrame if not already set or if explicitly requested.

        Args:
            reinit (bool, optional): Whether to reinitialize the topology DataFrame. Defaults to True.

        Raises:
            NoSuchElementException: If the element text cannot be read.

        Note:
            The method assumes the availability of the self.topology_df DataFrame and certain constants.

        """
        if not self.topology_df["entity_name"].notna().any() or reinit:
            entities = self.get_elements(self.topology_loc["node_label"])
            entity_names = []
            for entity in entities:
                text = entity.text
                if not len(text):
                    raise NoSuchElementException("Cannot read element text")
                name = text.split("\n")[1]
                entity_names.append(name)
                time.sleep(0.1)
            self.topology_df["entity_name"] = entity_names

    def _get_status_of_entity(self, entity_name: str):
        """
        Retrieves the status of an entity internally.

        Args:
            entity_name (str): Name of the entity (e.g., node or deployment).

        Note:
            This method is primarily used internally after reading the Topology.

        Returns:
            str: status of entity if the node was found
        """
        from ocs_ci.ocs.ui.helpers_ui import format_locator

        entity_class = self.get_element_attribute(
            format_locator(self.topology_loc["node_status_class_axis"], entity_name),
            "class",
            True,
        )
        if entity_class:
            return entity_class.split("-")[-1]

    def get_entity_name_from_df(self, index: int):
        """
        Returns the name of an entity at the specified index in the dataframe, if the dataframe is filled.

        Args:
            index (int): The index of the entity in the dataframe.

        Returns:
            str: The name of the entity at the given index, or None if the index is not found.

        """
        if index in self.topology_df.index:
            return self.topology_df.loc[index, "entity_name"]
        else:
            return None

    def zoom_out_view(self):
        """
        Zooms out the topology view.

        This method performs a zoom-out action on the topology view, effectively reducing the level of zoom.

        """
        logger.info("zooming out topology view")
        self.do_click(self.topology_loc["zoom_out"])

    def zoom_in_view(self):
        """
        Zooms in the topology view.
        """
        logger.info("zooming in topology view")
        self.do_click(self.topology_loc["zoom_in"])

    def reset_view(self):
        """
        Resets the topology view.
        """
        logger.info("resetting topology view")
        self.do_click(self.topology_loc["reset_view"])

    def expand_to_full_screen(self):
        """
        Expands the topology view to full screen.
        """
        logger.info("expanding topology view to full screen")
        self.do_click(self.topology_loc["expand_to_full_screen"])

    def nav_back_main_topology_view(self, soft: bool = False):
        """
        Navigate back to the higher hierarchy level in the topology view. Can be used in any level of topology,
        but will be performed only if button back exists when param soft is True

        Args:
            soft (bool, optional): If True, performs a navigation back if the "back" button is present.
                                   If False, performs a navigation back by clicking the "back" button.
                                   Defaults to False.
        Note:
            The "back" button exists only on the deployment level.
        """
        if soft and len(self.get_elements(self.topology_loc["back_btn"])):
            logger.info("navigate topology view to higher hierarchy")
            self.do_click(self.topology_loc["back_btn"])
        elif not soft:
            self.do_click(self.topology_loc["back_btn"])

    def check_entity_selected(self, entity_name):
        """
        Checks if the specified entity is selected.

        Args:
            entity_name (str): Name of the entity.

        Returns:
            bool: True if the entity is selected, False otherwise.

        """
        from ocs_ci.ocs.ui.helpers_ui import format_locator

        # selected node should contain pf-m-selected in the class name
        attribute = self.get_element_attribute(
            format_locator(
                self.topology_loc["entity_box_select_indicator"], entity_name
            ),
            "class",
            True,
        )
        if not attribute:
            return
        else:
            return "pf-m-selected" in attribute

    def select_entity_with_search_bar(self, entity_name):
        """
        Selects the specified entity using the search bar.

        Args:
            entity_name (str): Name of the entity to select.

        """
        logger.info(f"selecting '{entity_name}' with search bar")
        self.do_send_keys(self.topology_loc["topology_search_bar"], entity_name)
        self.do_click(self.topology_loc["topology_search_bar_enter_arrow"])

    def reset_search_bar(self):
        """
        Resets the search bar.
        """
        logger.info("reset search")
        self.do_click(self.topology_loc["topology_search_bar_reset_search"])

    def is_entity_present(self, entity_name) -> bool:
        """
        Checks if the specified entity is present.

        Args:
            entity_name (str): The name of the entity to search for.

        Returns:
            bool: True if the entity is present, False otherwise.
        """

        from ocs_ci.ocs.ui.helpers_ui import format_locator

        return bool(
            self.get_elements(
                format_locator(self.topology_loc["select_entity"], entity_name)
            )
        )


class TopologyTab(DataFoundationDefaultTab, AbstractTopologyView):
    """
    Topology tab Class
    Content of Data Foundation/Topology tab (default for ODF 4.13 and above)
    """

    def __init__(self):
        DataFoundationTabBar.__init__(self)
        AbstractTopologyView.__init__(self)
        self.nodes_len = -1
        self.__topology_df: pd.DataFrame = pd.DataFrame()
        self.__topology_str: TopologyUiStr
        self.topology_helper = OdfTopologyHelper()
        self.workload_ui = WorkloadUi()

    def read_all_topology(self):
        """
        Reads and records the topology of the cluster at the nodes level.

        Returns:
            pd.DataFrame: DataFrame containing the recorded topology information.

        Note:
            The recorded topology information is stored in the '__topology_df' attribute and accessible
            via get_topology_df()
        """
        # read topology of the cluster (nodes level)
        self.nodes_len = len(self.nodes_view.read_presented_topology())

        for i in range(self.nodes_len):
            entity_name = self.nodes_view.get_entity_name_from_df(i)
            logger.info(f"reading {entity_name} deployments topology")

            self.cluster_topology.nav_back_main_topology_view(soft=True)
            deployment_view = self.cluster_topology.nav_into_node(i)

            df = deployment_view.read_presented_topology()
            self.__topology_df = self.cluster_topology.record_nested_deployments(i, df)

        self.nav_back_main_topology_view(soft=True)
        return self.__topology_df

    def get_topology_df(self):
        """
        Retrieves the recorded topology DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing the recorded topology information.
        """
        return self.__topology_df

    def get_topology_str(self):
        """
        Method to get a dataframe with Topology as a str to print in console
            # +----+--------------+---------------+----------------------------------------------------------------+
            # |    | entity_name  | entity_status | nested_deployments                                             |
            # +====+==============+===============+================================================================+
            # |  0 | compute-0    | success       | entity_name entity_status                                      |
            # |    |              |               | 0                                    rook-ceph-osd-1   success |
            # |    |              |               | 1                       csi-cephfsplugin-provisioner   success |
            # |	   | 			  | 			  | ...															   |
            # +----+--------------+---------------+----------------------------------------------------------------+
            # |  1 | compute-1    | success       | entity_name entity_status                                      |
            # |    |              |               | 0                                    rook-ceph-osd-1   success |
            # |    |              |               | 1                       csi-cephfsplugin-provisioner   success |
            # +----+--------------+---------------+----------------------------------------------------------------+

        Returns:
            str: text representation of pandas Dataframe of ODF Topology view, where cluster with node names, node
            statuses and their deployment names and statuses may be found

        """
        return str(TopologyUiStr(self.__topology_df))

    def validate_topology_configuration(self):
        """
        Validates the configuration of the topology.

        Returns:
            dict: A dictionary indicating the deviations found during validation. The keys represent specific deviations
                  and the values are booleans indicating whether the deviation was detected or not.
        """

        node_with_busybox, _ = self.workload_ui.deploy_busybox()
        sleep_time = 30
        logger.info(f"give {sleep_time}sec to render on ODF Topology view")
        time.sleep(sleep_time)

        self.read_all_topology()
        logger.info("\n" + self.get_topology_str())

        topology_cli_df = self.topology_helper.read_topology_cli_all()
        logger.debug(self.topology_helper.get_topology_cli_str())

        topology_deviation = dict()

        node_names = get_node_names()
        random_node_name = random.choice(node_names)
        navigation_bar_check = self.validate_topology_navigation_bar(random_node_name)
        if not navigation_bar_check:
            logger.error("search bar validation check failed")
            topology_deviation["topology_navigation_bar_select_fail"] = True

        logger.info("check node bar filtering functionality")
        deployment_view = self.nodes_view.nav_into_node(
            node_name_option=random_node_name
        )
        another_random_node = random.choice(
            [node_name for node_name in node_names if node_name != random_node_name]
        )

        deployment_view.filter_node_by_toggle_from_deployments_level(
            another_random_node
        )
        node_selected = (
            deployment_view.get_current_selected_node_from_deployments_level()
        )
        deployment_view.nav_back_main_topology_view()

        if node_selected != another_random_node:
            logger.error("search bar navigate to another node check failed")
            topology_deviation[
                "search_bar_navigate_to_another_node_check_failed"
            ] = True

        topology_ui_df = self.get_topology_df()

        ceph_cluster = OCP(
            kind="CephCluster", namespace=config.ENV_DATA["cluster_namespace"]
        )
        cluster_app_name_cli = (
            ceph_cluster.get().get("items")[0].get("metadata").get("labels").get("app")
        )
        cluster_name_ui = self.nodes_view.get_cluster_name()

        if cluster_app_name_cli != cluster_name_ui:
            logger.error(
                "cluster app name from UI and from CLI are not identical\n"
                f"cluster_app_name_cli = '{cluster_app_name_cli}'"
                f"cluster_name_ui = '{cluster_name_ui}'"
            )
            topology_deviation["cluster_app_name_not_equal"] = True

        # zoom out to read rack/zone label
        zoom_out_times = 1 if len(node_names) < 4 else 2

        if config.ENV_DATA["platform"] != constants.HCI_BAREMETAL:
            self.validate_node_group_names(
                cluster_app_name_cli, topology_deviation, zoom_out_times
            )

        # check node names from ODF Topology UI and CLI are identical
        if not sorted(list(topology_ui_df["entity_name"])) == sorted(
            list(topology_cli_df.columns)
        ):
            logger.error(
                f"nodes of the cluster {cluster_app_name_cli} from UI and from CLI are not identical\n"
                f"deployments_list_cli = {sorted(list(topology_ui_df['entity_name']))}\n"
                f"deployments_list_ui = {sorted(list(topology_cli_df.columns))}"
            )
            topology_deviation["nodes_not_equal"] = True

        for index, row in topology_ui_df.iterrows():

            node_name = row["entity_name"]
            # comment left here for further usage as a point where we can work with states of deployments iteratively
            # node_status = row["entity_status"]

            deployments_names_list_cli = (
                self.topology_helper.get_deployment_names_from_node_df_cli(node_name)
            )
            deployments_names_list_ui = list(row["nested_deployments"]["entity_name"])

            if not sorted(deployments_names_list_cli) == sorted(
                deployments_names_list_ui
            ):
                self.take_screenshot()
                self.copy_dom()
                logger.error(
                    f"deployments of the node '{node_name}' from UI do not match deployments from CLI\n"
                    f"deployments_list_cli = '{sorted(deployments_names_list_cli)}'\n"
                    f"deployments_list_ui = '{sorted(deployments_names_list_ui)}'"
                )
                topology_deviation[f"{node_name}__deployments_not_equal"] = True

            busybox_depl_name = self.workload_ui.get_busybox_depl_name()
            if node_name == node_with_busybox and (
                busybox_depl_name not in deployments_names_list_ui
            ):
                self.take_screenshot()
                self.copy_dom()
                logger.error(
                    f"busybox deployment '{busybox_depl_name}' deployed on the node '{node_with_busybox}' "
                    f"during the test was not found in UI"
                )
                topology_deviation["added_deployment_not_found"] = True
            elif node_name == node_with_busybox and (
                busybox_depl_name in deployments_names_list_ui
            ):
                self.workload_ui.delete_busybox(busybox_depl_name)
                sleep_time = 30
                logger.info(
                    f"delete '{busybox_depl_name}' deployment from cluster, give {sleep_time}sec to update ODF "
                    "Topology and verify deployment was removed"
                )
                time.sleep(sleep_time)

                deployment_topology = self.nodes_view.nav_into_node(
                    node_name_option=node_with_busybox
                )

                # zoom out Topology view before trying to find busybox deployment
                if len(deployments_names_list_ui) < 6:
                    zoom_out_times = 1
                elif len(deployments_names_list_ui) < 12:
                    zoom_out_times = 2
                else:
                    zoom_out_times = 3
                for i in range(1, zoom_out_times + 1):
                    self.zoom_out_view()

                # check deployed during the test deployment is present
                if not deployment_topology.is_entity_present(busybox_depl_name):
                    logger.info(
                        f"Deployment '{busybox_depl_name}' was successfully removed from ODF Topology view"
                    )
                else:
                    logger.error(
                        f"busybox deployment '{busybox_depl_name}' deployed on the node '{node_with_busybox}' "
                        f"during the test was not removed from ODF Topology"
                    )
                    self.take_screenshot()
                    self.copy_dom()
                    topology_deviation[f"{busybox_depl_name}__not_removed"] = True
                deployment_topology.nav_back_main_topology_view()
        return topology_deviation

    def validate_node_group_names(
        self, cluster_app_name_cli, topology_deviation, zoom_out_times
    ):
        """
        Validates the node group names (such as rack or zone) from the ODF Topology UI against names taken from CLI.
        :param cluster_app_name_cli: cluster name visible in Topology UI
        :param topology_deviation: dictionary to store deviations if found
        :param zoom_out_times: number of times to zoom out the Topology view to see whole cluster representation
        """
        storage_cluster = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        groups_cli = (
            storage_cluster.get()
            .get("items")[0]
            .get("status")
            .get("failureDomainValues")
        )
        for i in range(1, zoom_out_times + 1):
            self.nodes_view.zoom_out_view()
        groups_ui = self.nodes_view.get_group_names()
        # check group names such as racks or zones from ODF Topology UI and CLI are identical
        # Preprocess groups_ui to remove elements that contain 'SC\n' and the cluster name
        processed_groups_ui = [group for group in groups_ui if "SC\n" not in group]
        if not sorted(groups_cli) == sorted(processed_groups_ui):
            logger.error(
                f"group names for worker nodes (labels) of the cluster {cluster_app_name_cli} "
                "from UI and from CLI are not identical\n"
                f"groups_cli = {sorted(groups_cli)}\n"
                f"groups_ui = {sorted(groups_ui)}"
            )

            topology_deviation["worker_group_labels_not_equal"] = True

    def validate_topology_navigation_bar(self, entity_name):
        """
        Validates the navigation bar functionality in the topology view.

        This method verifies that the provided entity name can be selected using the search bar in the topology view.
        It ensures that the entity is correctly selected and then resets the search bar.

        Args:
            entity_name (str): The name of the entity to be selected in the topology view.

        Returns:
            bool: True if the entity is successfully selected, False otherwise.
        """
        self.nav_back_main_topology_view(soft=True)

        self.nodes_view.select_entity_with_search_bar(entity_name)
        entity_selected = self.check_entity_selected(entity_name)

        self.nodes_view.reset_search_bar()
        return entity_selected


class OdfTopologyNodesView(TopologyTab):
    """
    The OdfTopologyNodesView class represents a view of the ODF topology at the nodes level.
    The class initializes the topology_df DataFrame with specific column
    names and data types to store information about entity names, entity status, XPath expressions for status,
    navigation, and node selection, as well as nested deployments. The DataFrame is initially empty but can be
    populated with data iteratively.
    """

    def __init__(self):
        TopologyTab.__init__(self)
        self.topology_col = [
            "entity_name",
            "entity_status",
            "status_xpath",
            "navigate_into_xpath",
            "select_node_xpath",
            "nested_deployments",
        ]
        data_types = {
            "entity_name": str,
            "entity_status": str,
            "status_xpath": str,
            "navigate_into_xpath": str,
            "select_node_xpath": str,
            "nested_deployments": object,
        }
        self.topology_df = pd.DataFrame(columns=list(data_types.keys())).astype(
            data_types
        )

    def get_group_names(self) -> list:
        """
        Get racks/zones names from Topology canvas. Sidebar is not used

        :return: names of the groups
        """
        elements = self.get_elements(self.topology_loc["node_group_name"])
        return [el.text for el in elements if "OCS" not in el.text and el.text.strip()]

    def get_cluster_name(self) -> str:
        """
        Get cluster name from Topology canvas. Sidebar is not used

        :return: name of the cluster such as 'ocs-storagecluster'
        """
        cluster_name_el = self.get_elements(self.topology_loc["node_group_name"])[0]
        return cluster_name_el.text.split("\n")[1]

    @retry(TimeoutException)
    def nav_into_node(
        self, node_index_option: int = None, node_name_option: str = None
    ):
        """
        Navigates into a specific node in the Topology UI.

        Args:
            node_index_option (int): Index of the node by order.
            node_name_option (str): Name of the node.

        Returns:
            OdfTopologyDeploymentsView: Instance of the class representing the UI Topology.

        Raises:
            IncorrectUiOptionRequested: If incorrect arguments are provided.

        Note:
            This method should be used only after reading the presented topology with read_presented_topology().

        Example:
            nav_into_node(node_index_option=0)
            # Returns an instance of OdfTopologyDeploymentsView representing the UI Topology.

        """
        if isinstance(node_index_option, str):
            # string automatically casts into int
            raise IncorrectUiOptionRequested(
                "nav_into_node method has two args to work with; "
                "do not use nav_into_node(node_name), "
                "instead use nav_into_node(node_name_option='node_name')"
            )

        if node_index_option is not None:
            loc = (
                self.topology_df.at[node_index_option, "navigate_into_xpath"],
                By.XPATH,
            )
            logger.info(f"Open node by index {node_index_option}")
        elif node_name_option is not None:

            filtered_line = self.topology_df[
                self.topology_df["entity_name"] == node_name_option
            ]
            loc = (filtered_line["navigate_into_xpath"].iloc[0], By.XPATH)
            logger.info(f"Open node by name {node_name_option}")
        else:
            raise IncorrectUiOptionRequested(
                f"Pass one of required options to use method '{self.nav_into_node.__name__}'"
            )
        self.do_click(loc, 60, True)
        self.page_has_loaded(5, 5, self.topology_loc["topology_graph"])
        return OdfTopologyDeploymentsView()

    def record_nested_deployments(
        self, node_index: int, df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Records nested deployments for a specific node in the Topology.

        Args:
            node_index (int): Index of the node in the Topology.
            df (pd.DataFrame): DataFrame representing the nested deployments.

        Returns:
            pd.DataFrame: Updated Topology DataFrame.

        Example:
            record_nested_deployments(0, nested_deployments_df)
            # Returns the updated Topology DataFrame with nested deployments recorded.

        """
        self.topology_df.at[node_index, "nested_deployments"] = df
        return self.topology_df

    def read_details(self) -> dict:
        """
        Reads and retrieves details of a node from the UI.

        Returns:
            dict: Dictionary containing the node details.

        Raises:
            IncorrectUiOptionRequested: If the wrong level of topology is opened instead of the Node level.

        Example:
            read_details()
            # Returns {'name': 'node-1', 'status': 'Ready', 'role': 'worker', 'operating_system': 'linux',
            #          'kernel_version': '4.18.0-305.12.1.el8_4.x86_64', 'instance_type': 'm5.large',
            #          'OS_image': 'CentOS Linux 8 (Core)', 'architecture': 'amd64',
            #          'addresses': 'External IP: 203.0.113.10; Hostname: node-1; Internal IP: 192.168.0.1',
            #          'kubelet_version': 'v1.21.2', 'provider_ID': 'aws', 'annotations_number': '5 annotations',
            #          'external_id': '-', 'created': 'Jun 1, 2023, 10:00 AM'}

        """
        details_dict = dict()
        if (
            self.get_element_text(self.topology_loc["details_sidebar_entity_header"])
            == "Node details"
        ):
            filtered_dict = {
                locator_name: locator_tuple
                for locator_name, locator_tuple in self.topology_loc.items()
                if locator_name.startswith("details_sidebar_node_")
            }

            for detail_name, loc in filtered_dict.items():
                if detail_name == "details_sidebar_node_addresses":
                    node_addresses = self.get_elements(loc)
                    addresses_txt = [el.text for el in node_addresses]
                    addresses_txt = "; ".join(addresses_txt)
                    details_dict[
                        detail_name.split("details_sidebar_node_", 1)[-1].strip()
                    ] = addresses_txt
                elif (
                    detail_name == "details_sidebar_node_zone"
                    and config.ENV_DATA["platform"].lower() in ON_PREM_PLATFORMS
                ):
                    continue
                elif (
                    detail_name == "details_sidebar_node_rack"
                    and config.ENV_DATA["platform"].lower() in CLOUD_PLATFORMS
                ):
                    continue
                elif (
                    detail_name == "details_sidebar_node_zone"
                    or detail_name == "details_sidebar_node_rack"
                ) and config.ENV_DATA[
                    "platform"
                ].lower() in HCI_PROVIDER_CLIENT_PLATFORMS:
                    # based on https://bugzilla.redhat.com/show_bug.cgi?id=2263826 parsing excluded
                    continue
                else:
                    details_dict[
                        detail_name.split("details_sidebar_node_", 1)[-1].strip()
                    ] = self.get_element_text(loc)
        else:
            raise IncorrectUiOptionRequested(
                "Wrong level of topology opened instead of Node lvl",
                lambda: self.take_screenshot(),
            )

        details_df = pd.DataFrame.from_dict(details_dict, orient="index")

        logger.info(
            f"Details of the {details_df.loc['name', 0]} node\n"
            f"{details_df.to_markdown(headers='keys', index=True, tablefmt='grid')}"
        )
        return details_dict


class OdfTopologyDeploymentsView(TopologyTab):
    """
    Represents the view of deployments in the ODF topology.

    This class extends the `TopologyTab` class and provides functionality specific to deployments.
    The class is accessible mainly via OdfTopologyNodesView.nav_into_node(args)

    """

    def __init__(self):
        TopologyTab.__init__(self)
        self.topology_col = [
            "entity_name",
            "entity_status",
            "status_xpath",
            "select_node_xpath",
        ]
        data_types = {
            "entity_name": str,
            "entity_status": str,
            "status_xpath": str,
            "select_node_xpath": str,
        }
        self.topology_df = pd.DataFrame(columns=list(data_types.keys())).astype(
            data_types
        )

    def read_details(self) -> dict:
        """
        Reads and retrieves details of a deploymen from the UI. Side-bar of deployment should be open.

        Returns:
            dict: Dictionary containing the node details.

        Raises:
            IncorrectUiOptionRequested: If the wrong level of topology is opened instead of the Node level.

        """
        details_dict = dict()

        # if navigate back btn exists - the deployment topology is opened. No header for deployment - bz #2210040
        if len(self.get_elements(self.topology_loc["back_btn"])):
            details_dict["name"] = self.get_element_text(
                self.topology_loc["details_sidebar_depl_name"]
            )
            details_dict["namespace"] = self.get_element_text(
                self.topology_loc["details_sidebar_depl_namespace"]
            ).split("\n")[-1]

            label_elements = self.get_elements(
                self.topology_loc["details_sidebar_depl_labels"]
            )
            labels_list = [label_element.text for label_element in label_elements]
            # work with labels including such that does not have value, such as
            # operators.coreos.com/ocs-operator.openshift-storage
            if labels_list:
                details_dict["labels"] = {
                    label.split("=", 1)[0]: (
                        label.split("=", 1)[1] if "=" in label else ""
                    )
                    for label in labels_list
                }
            else:
                details_dict["labels"] = ""

            details_dict["annotation"] = self.get_element_text(
                self.topology_loc["details_sidebar_depl_annotations"]
            )
            details_dict["created_at"] = self.get_element_text(
                self.topology_loc["details_sidebar_depl_created_at"]
            )
            details_dict["owner"] = self.get_element_text(
                self.topology_loc["details_sidebar_depl_owner"]
            ).split("\n")[-1]

        else:
            raise IncorrectUiOptionRequested(
                "Node details opened instead of Deployment details",
                lambda: self.take_screenshot(),
            )
        logger.info(
            f"Details of '{details_dict['name']}' deployment from UI\n"
            f"{json.dumps(details_dict, indent=4)}"
        )
        return details_dict

    @retry(ErrorHandler)
    def filter_node_by_toggle_from_deployments_level(self, node_name):
        """
        Filters the node by toggle from the deployments level in the topology view.

        Args:
            node_name (str): Name of the node to filter.

        Raises:
            IncorrectUiOptionRequested: If topology node filtering exists only on the Deployment Topology level.

        """
        if len(self.get_elements(self.topology_loc["back_btn"])):
            self.do_click(
                self.topology_loc["node_filter_toggle_icon_from_node_filtering_bar"]
            )
            time.sleep(0.5)
            from ocs_ci.ocs.ui.helpers_ui import format_locator

            self.do_click(
                format_locator(
                    self.topology_loc["node_selector_from_node_filtering_bar"],
                    node_name,
                )
            )
        else:
            raise IncorrectUiOptionRequested(
                "Topology node filtering exists only on Deployment Topology level"
            )

    def get_current_selected_node_from_deployments_level(self) -> str:
        """
        Retrieves the name of the currently selected node from the deployments level in the topology view.

        Returns:
            str: Name of the currently selected node.

        Raises:
            IncorrectUiOptionRequested: If the topology node filtering is not available on the deployments level.

        Example:
            get_current_selected_node_from_deployments_level()
            # Returns 'my-node-1' if the node is currently selected.
        """
        if len(self.get_elements(self.topology_loc["back_btn"])):
            return self.get_element_text(
                self.topology_loc["current_node_from_node_filtering_bar"]
            )
        else:
            raise IncorrectUiOptionRequested(
                "Topology node filtering exists only on Deployment Topology level"
            )
