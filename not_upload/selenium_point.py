
import logging
import os
import sys
import ocs_ci.ocs.ui.base_ui
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.framework import config



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
console_handler = logging.StreamHandler(stream=sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


config.ENV_DATA['cluster_path'] = os.environ['CLUSTER_PATH']
config.ENV_DATA["cluster_namespace"] = "openshift-storage"
config.RUN["kubeconfig"] = os.environ['KUBECONFIG']

def login_and_nav():
    from ocs_ci.ocs.ui.base_ui import login_ui
    return PageNavigator().nav_odf_default_page().nav_storage_systems_tab().nav_storagecluster_storagesystem_details()

if __name__ == '__main__':
    from ocs_ci.ocs.ui.base_ui import login_ui
    driver = login_ui()
    PageNavigator().nav_odf_default_page().nav_storage_systems_tab().nav_storagecluster_storagesystem_details()
    # (//*[@class='capacity-breakdown-card__legend-link'])[1]
    pass