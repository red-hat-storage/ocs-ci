import logging
import random
import time
import pytest
import pandas as pd
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    polarion_id,
    tier3,
    skipif_external_mode,
    tier4,
    external_mode_required,
    bugzilla,
    skipif_ibm_cloud_managed,
    skipif_ocs_version,
    skipif_managed_service,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_nodes, get_worker_nodes, get_node_names
from ocs_ci.ocs.ui.base_ui import PageNavigator
from ocs_ci.ocs.ui.odf_topology import (
    OdfTopologyHelper,
    get_deployment_details_cli,
    get_node_details_cli,
)

logger = logging.getLogger(__name__)


@pytest.fixture()
def teardown_nodes_job(request, nodes):
    def finalizer():
        """
        Make sure all nodes are up again

        """
        nodes.restart_nodes_by_stop_and_start_teardown()

    request.addfinalizer(finalizer)


@pytest.fixture()
def teardown_depl_busybox(request):
    def finalizer():
        """
        Make sure busybox deployment removed

        """

        OdfTopologyHelper().delete_busybox()

    request.addfinalizer(finalizer)


@black_squad
@skipif_ibm_cloud_managed
@skipif_managed_service
@skipif_ocs_version("<4.13")
class TestODFTopology(object):
    @bugzilla("2214023")
    @tier3
    @pytest.mark.parametrize(
        argnames="deployment_mode",
        argvalues=[
            pytest.param(
                "internal",
                marks=[pytest.mark.polarion_id("OCS-4901"), skipif_external_mode],
            ),
            pytest.param(
                "external",
                marks=[pytest.mark.polarion_id("OCS-4902"), external_mode_required],
            ),
        ],
    )
    def test_validate_topology_configuration(
        self,
        setup_ui_class,
        teardown_depl_busybox,
        deployment_mode,
    ):
        """
        Test to validate configuration of ODF Topology for internal and external deployments,
        cloud based deployments and on-prem deployments also for post-upgrade scenarios.

        Steps:
        1. Open Topology tab
        2. Read Node level of Topology - Cluster name, Number of nodes and their names, Zone/Rack name of each node,
            node filtering bar, search bar
        3. Open Topology entering to each Node and read Deployment of each Topology
        4. Compare gathered information from UI to configuration gathered from oc commands

        Test verifies requirements:
        OCS-4888        Deploy ODF internal mode cluster and verify Topology represented `oc get CephCluster`
            correctly when clicking on Data Foundation menu, check Canvas representation
        OCS-4890        Update ODF from version 4.12 to version 4.13 and verify Topology represented correctly when
            clicking on Data Foundation menu
        OCS-4891        Clustername from Topology equals to the name from 'oc get CephCluster'
        OCS-4892        Zone of each node from Topology equals description of the node, e.g label zone: zone_1
        OCS-4893        Rack of each node from Topology equals description of the node, e.g label rack: rack_1
        OCS-4894        When Topology is opened on Deployment level the Text field filters out deployment element,
            the rest elements should disapear from Topology
        OCS-4899        When Topology is opened on Deployment level the Path represents selected node name from
            selected storage cluster name. Nodes selection from dropdown represents content of selected node.
            Right btn click on storage cluster name returns to Node/Rack Topology representation.
        OCS-4895        Node bar on the Deployment Topology filters out the node by text input
        OCS-4906        Add deployment to ODF cluster and verify that Topology represents added deployment
        OCS-4907        Delete deployment from ODF cluster and verify that Topology represents that deployment
        """

        topology_tab = PageNavigator().nav_odf_default_page().nav_topology_tab()

        topology_deviation = topology_tab.validate_topology_configuration()

        if deployment_mode == "external":
            random_node_name = random.choice(get_worker_nodes())
            topology_tab.nodes_view.nav_back_main_topology_view(soft=True)
            topology_tab.nodes_view.open_side_bar_of_entity(random_node_name)
            topology_tab.nodes_view.open_alerts_tab()
            if topology_tab.nodes_view.is_alert_tab_present():
                logger.error(
                    "alert tab should not be present on External mode deployments"
                )
                topology_tab.take_screenshot()
                topology_deviation["alert_tab_present_external_mode"] = True

        if len(topology_deviation):
            pytest.fail(
                "got deviation in topology configuration, at least one check failed\n"
                f"{topology_deviation}"
            )

    @bugzilla("2214023")
    @tier3
    @skipif_external_mode
    @polarion_id("OCS-4903")
    def test_validate_topology_node_details(self, setup_ui_class):
        """
        Test to validate ODF Topology node details

        Steps:
        1. Get node names and pick random node
        2. Get node details with CLI
        3. Open Management console, login and navigate to ODF topology tab
        4. Read presented topology from UI
        5. Select node previously picked as random and open sidebar and click on details tab
        6. Read node details from UI
        7. Concatenate details from CLI and from UI and find differences
        """
        node_names = get_node_names()
        random_node_name = random.choice(node_names)

        node_details_cli = get_node_details_cli(random_node_name)

        topology_tab = PageNavigator().nav_odf_default_page().nav_topology_tab()
        topology_tab.nodes_view.read_presented_topology()

        topology_tab.nodes_view.open_side_bar_of_entity(random_node_name)
        topology_tab.nodes_view.open_details_tab()
        node_details_ui = topology_tab.nodes_view.read_details()
        topology_tab.nodes_view.close_sidebar()

        node_details_df_cli = pd.DataFrame.from_dict(
            node_details_cli, orient="index", columns=["details_cli"]
        )
        node_details_df_ui = pd.DataFrame.from_dict(
            node_details_ui, orient="index", columns=["details_ui"]
        )

        deviations_df = pd.concat([node_details_df_cli, node_details_df_ui], axis=1)
        deviations_df["Differences"] = (
            deviations_df["details_cli"] != deviations_df["details_ui"]
        )

        pd.set_option("display.max_colwidth", 100)
        if deviations_df["Differences"].any():
            pytest.fail(
                f"details of the node {random_node_name} from UI does not match details from CLI"
                f"\n{deviations_df}"
            )

    @tier3
    @skipif_external_mode
    @polarion_id("OCS-4904")
    def test_validate_topology_deployment_details(self, setup_ui_class):
        """
        Test to validate ODF Topology deployments details

        1. Get node names and pick random node
        2. Read topology CLI of the nodes and the storage related deployments
        3. Get node names and pick random node
        4. Get random deployment name from random node
        5. Open Management console, login and navigate to ODF topology tab
        6. Navigate into node that was previously picked
        7. Select deployment previously picked as random and open sidebar and click on details tab
        8. Read deployment details from CLI
        9. Read deployment details from UI
        10. Concatenate details from CLI and from UI and find differences
        """
        topology_tab = PageNavigator().nav_odf_default_page().nav_topology_tab()

        node_names = get_node_names()
        random_node_name = random.choice(node_names)
        topology_cli = topology_tab.topology_helper.read_topology_cli_all()
        random_deployment = random.choice(
            topology_cli[random_node_name].dropna().index.to_list()
        )

        topology_tab.nodes_view.read_presented_topology()
        random_odf_topology_deployment_view = topology_tab.nodes_view.nav_into_node(
            node_name_option=random_node_name
        )
        random_odf_topology_deployment_view.read_presented_topology()
        random_odf_topology_deployment_view.open_side_bar_of_entity(random_deployment)
        random_odf_topology_deployment_view.open_details_tab()

        deployment_details_cli = get_deployment_details_cli(random_deployment)
        deployment_details_ui = random_odf_topology_deployment_view.read_details()

        deployment_details_cli_df = pd.DataFrame.from_dict(
            deployment_details_cli, orient="index", columns=["details_cli"]
        )
        deployment_details_ui_df = pd.DataFrame.from_dict(
            deployment_details_ui, orient="index", columns=["details_ui"]
        )

        deviations_df = pd.concat(
            [deployment_details_cli_df, deployment_details_ui_df], axis=1
        )
        deviations_df["Differences"] = (
            deviations_df["details_cli"] != deviations_df["details_ui"]
        )

        if deviations_df["Differences"].any():
            pytest.fail(
                f"details of the deployment '{random_deployment}' of the node '{random_node_name}' "
                f"from the UI and details from the CLI are not identical"
                f"\n{deviations_df.to_markdown(headers='keys', index=True, tablefmt='grid')}"
            )

    @tier4
    @skipif_external_mode
    @polarion_id("OCS-4905")
    def test_stop_start_node_validate_topology(
        self, nodes, setup_ui_class, teardown_nodes_job
    ):
        """
        Test to validate ODF Topology when node hard-stopped and started again

        Steps:
        1. Get random node from the worker nodes
        2. Stop selected node with forcefully
        3. Wait 1 minute explicitly to get update from Prometheus
        4. Navigate to ODF console and read cluster configuration from Node level
        5. Open side-bar of the node and read presented alerts
        6. Check ceph cluster is in degraded state, ODF topology canvas border painted red
        7. Check expected CephNodeDown alert exists on Node level
        8. If 'not single node only deployment' verify that any random idle node do not show CephNodeDown alert
        9. Return node to working state and verify alert disappear, canvas shows cluster is healthy
        """
        ocp_nodes = get_nodes(node_type=constants.WORKER_MACHINE)
        random_node_under_test = random.choice(ocp_nodes)
        random_node_idle = random.choice(
            [node for node in ocp_nodes if node != random_node_under_test]
        )
        nodes.stop_nodes(nodes=[random_node_under_test], force=True)

        min_wait_for_update = 6
        logger.info(f"wait {min_wait_for_update}min to get UI updated")
        time.sleep(min_wait_for_update * 60)

        topology_tab = PageNavigator().nav_odf_default_page().nav_topology_tab()
        topology_tab.nodes_view.read_presented_topology()

        test_checks = dict()
        test_checks["cluster_in_danger_state_check"] = (
            topology_tab.nodes_view.is_cluster_in_danger(),
            "ODF Topology canvas shows cluster is not in danger state (red canvas)",
        )

        test_checks["ceph_node_down_alert_found_check"] = (
            topology_tab.check_node_down_in_alerts_ui(read_canvas_alerts=True),
            f"'{constants.ALERT_NODEDOWN}' alert has not been found in a sidebar",
        )

        if not config.ENV_DATA["sno"] and bool(random_node_idle):
            logger.info(
                "check that any random idle node do not show CephNodeDown when conditions not met"
            )
            test_checks["ceph_node_down_alert_found_on_idle_node_check"] = (
                topology_tab.check_node_down_in_alerts_ui(entity=random_node_idle.name),
                f"'{constants.ALERT_NODEDOWN}' alert has been found on idle node",
            )

        logger.info(
            f"return node back to working state and check '{constants.ALERT_NODEDOWN}' alert removed"
        )
        nodes.start_nodes(nodes=[random_node_under_test], wait=True)

        sleep_time_to_update_ui = 6 * 60
        logger.info(
            f"sleep '{sleep_time_to_update_ui}'sec to update UI and remove {constants.ALERT_NODEDOWN} alert"
        )
        time.sleep(sleep_time_to_update_ui)

        test_checks["ceph_node_down_alert_found_after_node_turned_on_check"] = (
            not topology_tab.check_node_down_in_alerts_ui(read_canvas_alerts=True),
            f"'{constants.ALERT_NODEDOWN}' alert has been found on idle node",
        )

        if any(not test_check[0] for test_check in test_checks.values()):
            test_checks_df = pd.DataFrame.from_dict(data=test_checks, orient="index")
            test_checks_df = test_checks_df.drop(test_checks_df.columns[1], axis=1)
            pytest.fail(
                "One or multiple checks did not pass:"
                f"\n{test_checks_df.to_markdown(index=True, tablefmt='grid')}"
            )
