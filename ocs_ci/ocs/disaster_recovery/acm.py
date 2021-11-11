import logging
import time

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.acm_ui import AcmPageNavigator
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.ui.base_ui import login_ui


log = logging.getLogger(__name__)

# Lines below will be moved to config files
CLUSTER_NAME_1 = "Cluster-a"
CLUSTER_NAME_2 = "Cluster-b"
KUBECONFIG_A = "TBD"
KUBECONFIG_B = "TBD"
###########################################


#####################################################
# The Code below intended to run on ACM Hub cluster #
#####################################################


class AcmAddClusters(AcmPageNavigator):
    """
    ACM Page Navigator Class

    """

    def __init__(self, driver):
        super().__init__(driver)
        self.page_nav = locators[self.ocp_version]["acm_page"]

    def import_cluster(self, cluster_name, kubeconfig_location):
        """

        Args:
            cluster_name (str): cluster name to import
            kubeconfig_location (str): kubeconfig file location of imported cluster

        """
        self.navigate_clusters_page()
        time.sleep(3)
        self.do_click(self.page_nav["Import_cluster"])
        time.sleep(3)
        self.do_send_keys(
            self.page_nav["Import_cluster_enter_name"], text=f"{cluster_name}"
        )
        time.sleep(3)
        self.do_click(self.page_nav["Import_mode"])
        time.sleep(3)
        self.do_click(self.page_nav["choose_kubeconfig"])
        time.sleep(3)
        log.info(f"Coping Kubeconfig {kubeconfig_location}")
        kubeconfig_to_import = copy_kubeconfig(kubeconfig_location)
        log.info(kubeconfig_to_import)
        self.do_click(self.page_nav["Kubeconfig_text"])
        self.do_send_keys(
            self.page_nav["Kubeconfig_text"], text=f"{kubeconfig_to_import}"
        )
        time.sleep(30)
        log.info(f"Submitting import of {cluster_name}")
        time.sleep(3)
        self.do_click(self.page_nav["Submit_import"])


def copy_kubeconfig(file):

    try:
        with open(file, "r") as f:
            txt = f.readlines()
            return txt

    except FileNotFoundError:
        log.error("file not found")


def get_acm_url():
    """
    Gets ACM console url

    Returns:
        str: url of ACM console

    """
    mch_cmd = OCP(namespace="open-cluster-management")
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
    acm_title = "Red Hat Advanced Cluster Management for Kubernetes"
    validate_page_title(driver, title=acm_title)

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


def import_clusters_with_acm():
    """
    Run Procedure of: detecting acm, login to ACM console, import 2 clusters

    """
    verify_running_acm()
    driver = login_to_acm()
    acm_nav = AcmAddClusters(driver)
    acm_nav.import_cluster(
        cluster_name=CLUSTER_NAME_1,
        kubeconfig_location=KUBECONFIG_A,
    )
    time.sleep(300)
    acm_nav.import_cluster(
        cluster_name=CLUSTER_NAME_1,
        kubeconfig_location=KUBECONFIG_B,
    )
