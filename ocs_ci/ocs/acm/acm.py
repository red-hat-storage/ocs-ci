import logging
import time

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.acm.acm_constants import (
    ACM_NAMESPACE,
    ACM_MANAGED_CLUSTERS,
    ACM_PAGE_TITLE,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import MultiClusterConfig
from ocs_ci.ocs.ui.helpers_ui import format_locator
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.ui.acm_ui import AcmPageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.ui.base_ui import login_ui

log = logging.getLogger(__name__)


class AcmAddClusters(AcmPageNavigator):
    """
    ACM Page Navigator Class

    """

    def __init__(self, driver):
        super().__init__(driver)
        self.page_nav = locators[self.ocp_version]["acm_page"]

    def import_cluster_ui(self, cluster_name, kubeconfig_location):
        """

        Args:
            cluster_name (str): cluster name to import
            kubeconfig_location (str): kubeconfig file location of imported cluster

        """
        self.navigate_clusters_page()
        self.do_click(self.page_nav["Import_cluster"])
        self.do_send_keys(
            self.page_nav["Import_cluster_enter_name"], text=f"{cluster_name}"
        )
        self.do_click(self.page_nav["Import_mode"])
        self.do_click(self.page_nav["choose_kubeconfig"])
        log.info(f"Coping Kubeconfig {kubeconfig_location}")
        kubeconfig_to_import = copy_kubeconfig(kubeconfig_location)
        log.info(kubeconfig_to_import)
        self.do_click(self.page_nav["Kubeconfig_text"])
        for line in kubeconfig_to_import:
            self.do_send_keys(self.page_nav["Kubeconfig_text"], text=f"{line}")
            time.sleep(2)
        log.info(f"Submitting import of {cluster_name}")
        self.do_click(self.page_nav["Submit_import"])

    def import_cluster(self, cluster_name, kubeconfig_location):
        """
        Import cluster using UI

        Args:
            cluster_name: (str): cluster name to import
            kubeconfig_location: (str): kubeconfig location

        """

        self.import_cluster_ui(
            cluster_name=cluster_name, kubeconfig_location=kubeconfig_location
        )
        for sample in TimeoutSampler(
            timeout=450,
            sleep=60,
            func=validate_cluster_import,
            cluster_name=cluster_name,
        ):
            if sample:
                log.info(f"Cluster: {cluster_name} successfully imported")
            else:
                log.error(f"import of cluster: {cluster_name} failed")

    def install_submariner_ui(self):
        """
        Installs the Submariner on the ACM Hub cluster and expects 2 OCP clusters to be already imported
        on the Hub Cluster to create a link between them

        """

        self.navigate_clusters_page()
        self.do_click(locator=self.acm_page_nav["Clusters_page"])
        log.info("Click on Cluster sets")
        self.do_click(self.page_nav["cluster-sets"])
        self.page_has_loaded(retries=15, sleep_time=5)
        log.info("Click on Create cluster set")
        self.do_click(self.page_nav["create-cluster-set"])
        cluster_set_name = create_unique_resource_name("submariner", "clusterset")
        log.info("Send Cluster set name")
        self.do_send_keys(self.page_nav["cluster-set-name"], text=cluster_set_name)
        log.info("Click on Create")
        self.do_click(self.page_nav["click-create"], enable_screenshot=True)
        time.sleep(1)
        log.info("Click on Manage resource assignments")
        self.do_click(
            self.page_nav["click-manage-resource-assignments"], enable_screenshot=True
        )
        log.info("Select all Manage resource assignments")
        self.do_click(self.page_nav["select-all-assignments"])
        log.info("Search and deselect 'local-cluster'")
        self.do_send_keys(self.page_nav["search-cluster"], text="local-cluster")
        self.do_click(self.page_nav["select-first-checkbox"], enable_screenshot=True)
        log.info("Clear search")
        self.do_click(self.page_nav["clear-search"])
        log.info("Click on 'Review'")
        self.do_click(self.page_nav["review-btn"], enable_screenshot=True)
        log.info("Click on 'Save' to confirm the changes")
        self.do_click(self.page_nav["confirm-btn"], enable_screenshot=True)
        time.sleep(2)
        log.info("Click on 'Submariner add-ons' tab")
        self.do_click(self.page_nav["submariner-tab"])
        log.info("Click on 'Install Submariner add-ons' button")
        self.do_click(self.page_nav["install-submariner-btn"])
        log.info("Click on 'Target clusters'")
        self.do_click(self.page_nav["target-clusters"])
        log.info("Select 1st cluster")
        self.do_click(
            format_locator(
                locator=self.page_nav["cluster-name-selection"],
                string_to_insert=cluster_name_a,
            )
        )
        log.info("Select 2nd cluster")
        self.do_click(
            format_locator(
                locator=self.page_nav["cluster-name-selection"],
                string_to_insert=cluster_name_b,
            ),
            enable_screenshot=True,
        )
        log.info("Click on Next button")
        self.do_click(self.page_nav["next-btn"])
        log.info("Click on 'Enable NAT-T' to uncheck it")
        self.do_click(self.page_nav["nat-t-checkbox"])
        log.info(
            "Increase the gateway count to 3 by clicking twice on the gateway count add button"
        )
        self.do_click(self.page_nav["gateway-count-btn"])
        self.do_click(self.page_nav["gateway-count-btn"])
        log.info("Click on Next button")
        self.do_click(self.page_nav["next-btn"])
        log.info("Click on 'Enable NAT-T' to uncheck it")
        self.do_click(self.page_nav["nat-t-checkbox"])
        log.info("Increase the gateway count to 3")
        self.do_click(self.page_nav["gateway-count-btn"])
        self.do_click(self.page_nav["gateway-count-btn"])
        log.info("Click on Next button")
        self.do_click(self.page_nav["next-btn"])
        log.info("Click on 'Install'")
        self.do_click(self.page_nav["install-btn"])
        log.info("Checking connection status of both the imported clusters")
        connection_status_1 = self.wait_until_expected_text_is_found(
            locator=self.page_nav["connection-status-1"],
            expected_text="Healthy",
            timeout=600,
        )
        connection_status_2 = self.wait_until_expected_text_is_found(
            locator=self.page_nav["connection-status-2"],
            expected_text="Healthy",
            timeout=600,
        )
        assert (
            connection_status_1
        ), f"Connection status of cluster {cluster_name_a} is not Healthy"
        assert (
            connection_status_2
        ), f"Connection status of cluster {cluster_name_b} is not Healthy"

        log.info("Checking agent status of both the imported clusters")
        connection_status_1 = self.wait_until_expected_text_is_found(
            locator=self.page_nav["agent-status-1"],
            expected_text="Healthy",
            timeout=600,
        )
        connection_status_2 = self.wait_until_expected_text_is_found(
            locator=self.page_nav["agent-status-2"],
            expected_text="Healthy",
            timeout=600,
        )
        assert (
            connection_status_1
        ), f"Agent status of cluster {cluster_name_a} is not Healthy"
        assert (
            connection_status_2
        ), f"Agent status of cluster {cluster_name_b} is not Healthy"
        log.info("Checking if nodes of both the imported clusters are labeled or not")
        connection_status_1 = self.wait_until_expected_text_is_found(
            locator=self.page_nav["node-label-1"],
            expected_text="Nodes labeled",
            timeout=600,
        )
        connection_status_2 = self.wait_until_expected_text_is_found(
            locator=self.page_nav["node-label-2"],
            expected_text="Nodes labeled",
            timeout=600,
        )
        assert connection_status_1, f"Nodes of cluster {cluster_name_a} are not labeled"
        assert connection_status_2, f"Nodes of cluster {cluster_name_b} are not labeled"
        self.take_screenshot()
        log.info("Submariner add-ons creation is successful")


def copy_kubeconfig(file):
    """

    Args:
        file: (str): kubeconfig file location

    Returns:
        list: with kubeconfig lines

    """

    try:
        with open(file, "r") as f:
            txt = f.readlines()
            return txt

    except FileNotFoundError as e:
        log.error(f"file {file} not found")
        raise e


def get_acm_url():
    """
    Gets ACM console url

    Returns:
        str: url of ACM console

    """
    mch_cmd = OCP(namespace=ACM_NAMESPACE)
    url = mch_cmd.exec_oc_cmd(
        "get route -ojsonpath='{.items[].spec.host}'", out_yaml_format=False
    )
    log.info(f"ACM console URL: {url}")

    return f"https://{url}"


def validate_page_title(driver, title):
    """
    Validates Page HTML Title
    Args:
        driver: driver (Selenium WebDriver)
        title (str): required title

    """
    WebDriverWait(driver, 60).until(ec.title_is(title))
    log.info(f"page title: {title}")


def login_to_acm():
    """
    Login to ACM console and validate by its title

    Returns:
        driver (Selenium WebDriver)

    """
    url = get_acm_url()
    log.info(f"URL: {url}, {type(url)}")
    driver = login_ui(url)
    validate_page_title(driver, title=ACM_PAGE_TITLE)

    return driver


def verify_running_acm():
    """
    Detect ACM and its version on Cluster

    """
    mch_cmd = OCP(namespace=ACM_NAMESPACE)
    acm_status = mch_cmd.exec_oc_cmd(
        "get mch -o jsonpath='{.items[].status.phase}'", out_yaml_format=False
    )
    assert acm_status == "Running", f"ACM status is {acm_status}"
    acm_version = mch_cmd.exec_oc_cmd(
        "get mch -o jsonpath='{.items[].status.currentVersion}'", out_yaml_format=False
    )
    log.info(f"ACM Version Detected: {acm_version}")


def validate_cluster_import(cluster_name):
    """
    Validate ACM status of managed cluster

    Args:
        cluster_name: (str): cluster name to validate

    Assert:
        All conditions of selected managed cluster should be "True", Failed otherwise

    """
    oc_obj = OCP()
    log.debug({oc_obj.get(resource_name=ACM_MANAGED_CLUSTERS)})
    conditions = oc_obj.exec_oc_cmd(
        f"get managedclusters {cluster_name} -ojsonpath='{{.status.conditions}}'"
    )
    log.debug(conditions)

    for dict_status in conditions:
        log.info(f"Message: {dict_status.get('message')}")
        log.info(f"Status: {dict_status.get('status')}")
        assert (
            dict_status.get("status") == "True"
        ), f"Status is not True, but: {dict_status.get('status')}"


def get_clusters_env():
    """
    Stores cluster's kubeconfig location and clusters name, in case of multi-cluster setup

    Returns:
        dict: with clusters names, clusters kubeconfig locations

    """
    config = MultiClusterConfig()
    clusters_env = dict()
    config.switch_ctx(index=0)
    clusters_env["kubeconfig_hub_location"] = config.ENV_DATA.get("kubeconfig_location")
    clusters_env["hub_cluster_name"] = config.ENV_DATA.get("cluster_name")
    config.switch_ctx(index=1)
    clusters_env["kubeconfig_cl_a_location"] = config.ENV_DATA.get(
        "kubeconfig_location"
    )
    clusters_env["cl_a_cluster_name"] = config.ENV_DATA.get("cluster_name")
    config.switch_ctx(index=2)
    clusters_env["kubeconfig_cl_b_location"] = config.ENV_DATA.get(
        "kubeconfig_location"
    )
    clusters_env["cl_b_cluster_name"] = config.ENV_DATA.get("cluster_name")

    config.switch_ctx(index=0)

    return clusters_env


def import_clusters_with_acm():
    """
    Run Procedure of: detecting acm, login to ACM console, import 2 clusters

    """
    clusters_env = get_clusters_env()
    log.info(clusters_env)
    kubeconfig_a = clusters_env.get("kubeconfig_cl_a_location")
    kubeconfig_b = clusters_env.get("kubeconfig_cl_b_location")
    global cluster_name_a
    cluster_name_a = clusters_env.get("cl_a_cluster_name")
    global cluster_name_b
    cluster_name_b = clusters_env.get("cl_b_cluster_name")
    verify_running_acm()
    driver = login_to_acm()
    acm_nav = AcmAddClusters(driver)
    acm_nav.import_cluster(
        cluster_name=cluster_name_a,
        kubeconfig_location=kubeconfig_a,
    )

    acm_nav.import_cluster(
        cluster_name=cluster_name_b,
        kubeconfig_location=kubeconfig_b,
    )
