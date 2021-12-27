import logging
import time

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from ocs_ci.ocs.acm.acm_constants import (
    ACM_NAMESPACE,
    ACM_MANAGED_CLUSTERS,
    ACM_PAGE_TITLE,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import MultiClusterConfig
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
            cluster_name:
            kubeconfig_location:

        Returns:

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
        log.error("file not found")
        log.debug(f"expected file location {file}")
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
    mch_cmd = OCP(namespace="open-cluster-management")
    acm_status = mch_cmd.exec_oc_cmd(
        "get mch -o jsonpath='{.items[].status.phase}'", out_yaml_format=False
    )
    assert acm_status == "Running", f"ACM status is {acm_status}"
    acm_version = mch_cmd.exec_oc_cmd(
        "get mch -o jsonpath='{.items[].status.currentVersion}'", out_yaml_format=False
    )
    log.info(f"ACM Version Detected: {acm_version}")


def validate_cluster_import(cluster_name):
    oc_obj = OCP()
    log.debug({oc_obj.get(resource_name=ACM_MANAGED_CLUSTERS)})
    conditions = oc_obj.exec_oc_cmd(
        f"get managedclusters {cluster_name} -ojsonpath='{{.status.conditions}}'"
    )
    log.debug(conditions)

    for dict_status in conditions:
        log.info(f"Message: {dict_status.get('message')}")
        log.info(f"Status: {dict_status.get('status')}")
        assert dict_status.get(
            "status"
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
    cluster_name_a = clusters_env.get("cl_a_cluster_name")
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
