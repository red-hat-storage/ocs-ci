import logging
import random
import time
import pytest
import pandas as pd
from ocs_ci.framework import config
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    polarion_id,
    tier3,
    skipif_external_mode,
    skipif_ibm_cloud_managed,
    skipif_ocs_version,
    skipif_managed_service,
    tier4a,
    ignore_leftovers,
    ui,
    skipif_hci_provider_or_client,
    runs_on_provider,
    jira,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_nodes, get_node_names
from ocs_ci.ocs.ui.base_ui import take_screenshot
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.ui.odf_topology import (
    get_deployment_details_cli,
    get_node_details_cli,
    get_node_names_of_the_pods_by_pattern,
)
from ocs_ci.ocs.ui.workload_ui import WorkloadUi
from ocs_ci.ocs.ui.llm_tools.llm_helper import get_llm_client
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.utility import prometheus
from ocs_ci.helpers import helpers
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@pytest.fixture()
def teardown_nodes_job(request, nodes):
    def finalizer_restart_nodes_by_stop_and_start_teardown():
        """
        Make sure all nodes are up again

        """
        nodes.restart_nodes_by_stop_and_start_teardown()

    def finalizer_wait_cluster_healthy():
        """
        Make sure health check is OK after node returned to cluster after restart to avoid
        'Ceph cluster health is not OK. Health: HEALTH_WARN 1/3 mons down, quorum d,e' error
        """
        ceph_health_check(tries=60, delay=30)

    request.addfinalizer(finalizer_wait_cluster_healthy)
    request.addfinalizer(finalizer_restart_nodes_by_stop_and_start_teardown)


@pytest.fixture()
def teardown_workload_depl(request):
    def finalizer():
        """
        Make sure workload deployments are removed

        """

        WorkloadUi().delete_all_deployments()

    request.addfinalizer(finalizer)


@ui
@black_squad
@runs_on_provider
@skipif_ibm_cloud_managed
@skipif_managed_service
@skipif_external_mode
@skipif_ocs_version("<4.13")
class TestODFTopology(object):
    @tier3
    @polarion_id("OCS-4901")
    def test_validate_topology_configuration(
        self, setup_ui_class, teardown_workload_depl, pvc_factory, teardown_factory
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
        logger.info(
            "Add an unlabeled pod to the openshift-storage ns and "
            "Check the ODF topology UI and verify that it functions as expected."
        )
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
            project=OCP(kind="Project", namespace=config.ENV_DATA["cluster_namespace"]),
        )
        pod_obj = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc_obj.name,
            namespace=pvc_obj.namespace,
            pod_dict_path=constants.NGINX_POD_YAML,
        )
        teardown_factory(pod_obj)
        topology_tab = (
            PageNavigator().nav_storage_cluster_default_page().nav_topology_tab()
        )

        topology_deviation = topology_tab.validate_topology_configuration()

        if len(topology_deviation):
            pytest.fail(
                "got deviation in topology configuration, at least one check failed\n"
                f"{topology_deviation}"
            )

    @tier3
    @polarion_id("OCS-4903")
    def deprecated_test_validate_topology_node_details(self, setup_ui_class):
        """
        Test to validate ODF Topology node details
        BZ #2214023 fixed in 4.14.0-0.nightly-2023-09-02-132842

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

        topology_tab = (
            PageNavigator().nav_storage_cluster_default_page().nav_topology_tab()
        )
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

        # details of UI and CLI are often have mismatch, depending on different platforms and versions; see output
        cli_details_beautified = node_details_df_cli.to_markdown(
            headers="keys", index=True, tablefmt="grid"
        )
        logger.info(f"node_details_df_cli:\n{cli_details_beautified}")

        deviations_df = pd.concat([node_details_df_cli, node_details_df_ui], axis=1)
        deviations_df["Differences"] = (
            deviations_df["details_cli"] != deviations_df["details_ui"]
        )

        if config.ENV_DATA["worker_replicas"] == 0:
            # Remove the row with index "role" from the deviations DataFrame if COMPACT MODE (0 worker nodes)
            deviations_df = deviations_df.drop(index="role", errors="ignore")

        pd.set_option("display.max_colwidth", 100)
        if deviations_df["Differences"].any():
            pytest.fail(
                f"details of the node {random_node_name} from UI do not match details from CLI"
                f"\n{deviations_df}"
            )

    @tier3
    @polarion_id("OCS-4903")
    def test_validate_topology_node_details_llm(self, setup_ui_class):
        """
        Test to validate ODF Topology node details using LLM-based screen reading.

        Instead of using fragile XPath locators to read node details from the sidebar,
        this test takes a screenshot and passes it to a locally-running vision LLM
        to extract the information.

        Steps:
        1. Check ollama availability — skip if not running
        2. Get node names and pick random node
        3. Get node details with CLI
        4. Navigate to ODF topology tab
        5. Click on the random node to open sidebar and click Details tab
        6. Take screenshot of the current page
        7. Ask LLM to extract node details from the screenshot
        8. Compare LLM-extracted details with CLI details
        9. Close sidebar
        """
        llm_client = get_llm_client()
        if not llm_client.is_available():
            pytest.skip(
                "LLM backend is not available. "
                "Configure llm_model in UI_SELENIUM settings to enable this test."
            )

        log_step("Get node names and pick random node")
        node_names = get_node_names()
        random_node_name = random.choice(node_names)
        logger.info(f"Selected random node: {random_node_name}")

        log_step("Get node details with CLI")
        node_details_cli = get_node_details_cli(random_node_name)

        log_step("Navigate to ODF topology tab")
        topology_tab = (
            PageNavigator().nav_storage_cluster_default_page().nav_topology_tab()
        )
        topology_tab.nodes_view.read_presented_topology()

        log_step("Open sidebar and click Details tab")
        topology_tab.nodes_view.open_side_bar_of_entity(random_node_name)
        topology_tab.nodes_view.open_details_tab()

        log_step("Take screenshots for LLM analysis")
        screenshot_paths = topology_tab.nodes_view.take_screenshot_for_llm(
            name_suffix="node_details"
        )
        logger.info(f"Screenshots saved: {screenshot_paths}")

        log_step("Ask LLM to extract node details from screenshots")
        prompt = (
            "Read the node details panel on the right side of this screenshot. "
            "Extract the exact text character by character. "
            "Do not guess or infer characters. "
            "Return the details as a JSON object with these keys: "
            "name, status, role, instance_type, zone, addresses. "
            "For addresses return a single string with all address lines joined by '; '. "
            "Only include fields that are visible in the panel."
        )
        node_details_llm = llm_client.query_screenshot_json(screenshot_paths, prompt)

        logger.info("LLM-extracted node details (raw JSON):")
        for key, value in node_details_llm.items():
            logger.info(f"  {key}: {value}")

        topology_tab.nodes_view.close_sidebar()

        log_step("Compare LLM-extracted details with CLI details")
        fields_to_check = ["name", "status", "role"]
        if node_details_cli.get("zone"):
            fields_to_check.append("zone")
        if node_details_cli.get("instance_type"):
            fields_to_check.append("instance_type")

        def normalize(text):
            """
            Normalise text for LLM OCR comparison.
            Removes whitespace and punctuation characters that the LLM commonly
            confuses (e.g. dot vs dash in instance types like bx2-16x64 → bx2.16x64).
            """
            import re

            return re.sub(r"[\s\-_.:/]", "", str(text).strip().lower())

        mismatches = {}
        for field in fields_to_check:
            cli_val = str(node_details_cli.get(field, "")).strip().lower()
            llm_val = str(node_details_llm.get(field, "")).strip().lower()
            cli_norm = normalize(cli_val)
            llm_norm = normalize(llm_val)

            if cli_norm in llm_norm or llm_norm in cli_norm:
                logger.info(
                    f"  [PASS] {field}: CLI='{cli_val}' matches LLM='{llm_val}'"
                )
            else:
                logger.error(f"  [FAIL] {field}: CLI='{cli_val}' != LLM='{llm_val}'")
                mismatches[field] = {"cli": cli_val, "llm": llm_val}

        cli_addresses = node_details_cli.get("addresses", "").lower()
        llm_addresses = node_details_llm.get("addresses", "")
        if isinstance(llm_addresses, dict):
            llm_addr_str = "; ".join(
                f"{k}: {v}" for k, v in llm_addresses.items()
            ).lower()
        else:
            llm_addr_str = str(llm_addresses).lower()
        logger.info(f"  CLI addresses: '{cli_addresses}'")
        logger.info(f"  LLM addresses: '{llm_addr_str}'")

        cli_hostname = normalize(node_details_cli.get("name", ""))
        llm_addr_norm = normalize(llm_addr_str)
        if cli_hostname and cli_hostname in llm_addr_norm:
            logger.info(f"  [PASS] addresses contain node hostname '{cli_hostname}'")
        elif cli_hostname:
            logger.error(
                f"  [FAIL] addresses do not contain node hostname "
                f"'{cli_hostname}'. LLM addresses: '{llm_addr_str}'"
            )
            mismatches["addresses"] = {
                "cli": cli_addresses,
                "llm": llm_addr_str,
            }

        if mismatches:
            mismatch_report = "\n".join(
                f"  {field}: CLI='{vals['cli']}' vs LLM='{vals['llm']}'"
                for field, vals in mismatches.items()
            )
            pytest.fail(
                f"Node details mismatch for '{random_node_name}' "
                f"({len(mismatches)} field(s) differ):\n{mismatch_report}"
            )

    @tier3
    @polarion_id("OCS-4904")
    def test_validate_topology_deployment_details(self, setup_ui_class):
        """
        Test to validate ODF Topology deployments details

        1. Open Management console, login and navigate to ODF topology tab
        2. Get node names and pick random node
        3. Read topology CLI of the nodes and the storage related deployments
        4. Get random deployment name from random node
        5. Navigate into node that was previously picked
        6. Select deployment previously picked as random and open sidebar and click on details tab
        7. Read deployment details from CLI
        8. Read deployment details from UI
        9. Concatenate details from CLI and from UI and find differences
        """
        log_step("Open Management console, login and navigate to ODF topology tab")
        topology_tab = (
            PageNavigator().nav_storage_cluster_default_page().nav_topology_tab()
        )

        log_step("Get random node and deployment")
        node_names = get_node_names()
        random_node_name = random.choice(node_names)

        log_step("Read topology CLI of the nodes and the storage related deployments")
        topology_cli = topology_tab.topology_helper.read_topology_cli_all()

        log_step("Get random deployment name from random node")
        random_deployment = random.choice(
            topology_cli[random_node_name].dropna().index.to_list()
        )

        topology_tab.nodes_view.read_presented_topology()

        log_step("Navigate into node that was previously picked")
        random_odf_topology_deployment_view = topology_tab.nodes_view.nav_into_node(
            node_name_option=random_node_name
        )
        random_odf_topology_deployment_view.read_presented_topology()

        log_step(
            "Select deployment previously picked as random and open sidebar and click on details tab"
        )
        random_odf_topology_deployment_view.open_side_bar_of_entity(random_deployment)
        random_odf_topology_deployment_view.open_details_tab()

        log_step("Read deployment details from CLI")
        deployment_details_cli = get_deployment_details_cli(random_deployment)

        log_step("Read deployment details from UI")
        deployment_details_ui = random_odf_topology_deployment_view.read_details()

        deployment_details_cli_df = pd.DataFrame.from_dict(
            deployment_details_cli, orient="index", columns=["details_cli"]
        )
        deployment_details_ui_df = pd.DataFrame.from_dict(
            deployment_details_ui, orient="index", columns=["details_ui"]
        )

        log_step("Concatenate details from CLI and from UI and find differences")
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

    @tier4a
    @jira("DFBUGS-418")
    @ignore_leftovers
    @polarion_id("OCS-4905")
    @skipif_hci_provider_or_client
    def test_stop_start_node_validate_topology(
        self, nodes, setup_ui_class, teardown_nodes_job, threading_lock
    ):
        """
        Test to validate ODF Topology when node hard-stopped and started again

        Steps:
        1. Get random node from the worker nodes
        2. Stop selected node with forcefully
        3. Wait 1 minute explicitly to get update from Prometheus
        4. Navigate to ODF console and read cluster configuration from Node level
        5. Open sidebar of the node and read presented alerts
        6. Check ceph cluster is in degraded state, ODF topology canvas border painted red
        7. Check expected CephNodeDown alert exists on Node level
        8. If 'not single node only deployment' verify that any random idle node do not show CephNodeDown alert
        9. Return node to working state and verify alert disappear, canvas shows cluster is healthy
        """

        # exclude node(s) where the 'odf-console' is running. It may distruct work of the management-console
        node_to_pods_odf_console = get_node_names_of_the_pods_by_pattern("odf-console")
        logger.info(
            f"excluding node(s) from the test '{node_to_pods_odf_console.values()}' "
            "it should be only one 'odf-console' in default configuration"
        )

        ocp_nodes = get_nodes(node_type=constants.WORKER_MACHINE)
        random_node_under_test = random.choice(
            [
                node
                for node in ocp_nodes
                if node not in node_to_pods_odf_console.values()
            ]
        )
        logger.info(f"node to shut down will be '{random_node_under_test.name}'")

        random_node_idle = random.choice(
            [node for node in ocp_nodes if node != random_node_under_test]
        )
        nodes.stop_nodes(nodes=[random_node_under_test])

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        logger.info(f"Verifying whether {constants.ALERT_NODEDOWN} has been triggered")
        alerts = api.wait_for_alert(name=constants.ALERT_NODEDOWN, state="firing")
        test_checks = dict()
        test_checks["prometheus_CephNodeDown_alert_fired"] = len(alerts) > 0
        if not test_checks["prometheus_CephNodeDown_alert_fired"]:
            logger.error(
                f"Prometheus alert '{constants.ALERT_NODEDOWN}' is not triggered"
            )
        else:
            logger.info(f"alerts found: {str(alerts)}")

        min_wait_for_update = 3
        logger.info(f"wait {min_wait_for_update}min to get UI updated with alert")
        time.sleep(min_wait_for_update * 60)

        topology_tab = (
            PageNavigator().nav_storage_cluster_default_page().nav_topology_tab()
        )
        topology_tab.nodes_view.read_presented_topology()

        test_checks["cluster_in_danger_state_check_pass"] = (
            topology_tab.nodes_view.is_cluster_in_danger()
        )
        if not test_checks["cluster_in_danger_state_check_pass"]:
            take_screenshot("cluster_in_danger_state_check")
            logger.error("cluster is not in danger, when one Worker node is down")

        test_checks["ceph_node_down_alert_found_check_pass"] = (
            topology_tab.is_node_down_alert_in_alerts_ui(read_canvas_alerts=True)
        )
        if not test_checks["ceph_node_down_alert_found_check_pass"]:
            logger.error("CephNodeDown alert has not been found after node went down")

        if not config.ENV_DATA["sno"] and bool(random_node_idle):
            logger.info(
                f"check that any random idle node '{random_node_idle.name}' "
                "do not show CephNodeDown when conditions not met"
            )
            test_checks["ceph_node_down_alert_found_on_idle_node_check_pass"] = (
                not topology_tab.is_node_down_alert_in_alerts_ui(
                    entity=random_node_idle.name
                )
            )

            if not test_checks["ceph_node_down_alert_found_on_idle_node_check_pass"]:
                logger.error(f"'{constants.ALERT_NODEDOWN}' alert found on idle node")

        logger.info(
            f"return node back to working state and check '{constants.ALERT_NODEDOWN}' alert removed"
        )
        nodes.start_nodes(nodes=[random_node_under_test], wait=True)

        logger.info(f"Verifying whether {constants.ALERT_NODEDOWN} has been triggered")
        alerts = api.wait_for_alert(name=constants.ALERT_NODEDOWN)

        test_checks["prometheus_CephNodeDown_alert_removed"] = len(alerts) == 0
        if not test_checks["prometheus_CephNodeDown_alert_removed"]:
            logger.error(
                f"Prometheus alert '{constants.ALERT_NODEDOWN}' is not removed"
            )
            logger.error(f"alerts found: {str(alerts)}")

        logger.info(
            f"sleep {min_wait_for_update}min to update UI and remove {constants.ALERT_NODEDOWN} alert"
        )
        time.sleep(min_wait_for_update * 60)

        test_checks["ceph_node_down_alert_found_after_node_turned_on_check_pass"] = (
            not topology_tab.is_node_down_alert_in_alerts_ui(read_canvas_alerts=True)
        )
        if not test_checks[
            "ceph_node_down_alert_found_after_node_turned_on_check_pass"
        ]:
            take_screenshot("ceph_node_down_alert_found_after_node_turned_on_check")
            logger.error("CephNodeDown alert found after node returned back to work")

        test_checks_df = pd.DataFrame.from_dict(
            data=test_checks, columns=["check_state"], orient="index"
        )

        if not test_checks_df["check_state"].all():
            pytest.fail(
                "One or multiple checks did not pass:"
                f"\n{test_checks_df.to_markdown(index=True, tablefmt='grid')}"
            )
