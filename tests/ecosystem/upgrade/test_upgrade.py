import logging
from copy import deepcopy
from tempfile import NamedTemporaryFile
from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.testlib import upgrade
from ocs_ci.ocs.constants import OPERATOR_CATALOG_SOURCE_NAME
from ocs_ci.ocs.defaults import OCS_OPERATOR_NAME
from ocs_ci.ocs.exceptions import TimeoutException
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.ocs import ocs_install_verification
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.utility.utils import (
    get_latest_ds_olm_tag,
    get_next_version_available_for_upgrade,
)
from ocs_ci.utility.templating import dump_data_to_temp_yaml

log = logging.getLogger(__name__)


@upgrade
def test_upgrade():
    namespace = config.ENV_DATA['cluster_namespace']
    ocs_catalog = CatalogSource(
        resource_name=OPERATOR_CATALOG_SOURCE_NAME,
        namespace="openshift-marketplace",
    )
    image_url = ocs_catalog.get_image_url()
    image_tag = ocs_catalog.get_image_name()
    if config.DEPLOYMENT.get('upgrade_to_latest', True):
        new_image_tag = get_latest_ds_olm_tag()
    else:
        new_image_tag = get_next_version_available_for_upgrade(image_tag)
    cs_data = deepcopy(ocs_catalog.data)
    cs_data['spec']['image'] = ':'.join([image_url, new_image_tag])
    package_manifest = PackageManifest(resource_name=OCS_OPERATOR_NAME)
    csv_name_pre_upgrade = package_manifest.get_current_csv()
    log.info(f"CSV name before upgrade is: {csv_name_pre_upgrade}")
    with NamedTemporaryFile() as cs_yaml:
        dump_data_to_temp_yaml(cs_data, cs_yaml.name)
        ocs_catalog.apply(cs_yaml.name)
    # Wait for package manifest is ready
    package_manifest.wait_for_resource()
    attempts = 145
    for attempt in range(1, attempts):
        if attempts == attempt:
            raise TimeoutException("No new CSV found after upgrade!")
        log.info(f"Attempt {attempt}/{attempts} to check CSV upgraded.")
        package_manifest.reload_data()
        csv_name_post_upgrade = package_manifest.get_current_csv()
        if csv_name_post_upgrade == csv_name_pre_upgrade:
            log.info(f"CSV is still: {csv_name_post_upgrade}")
            sleep(5)
        else:
            log.info(f"CSV now upgraded to: {csv_name_post_upgrade}")
            break

    csv = CSV(
        resource_name=csv_name_post_upgrade,
        namespace=namespace
    )
    log.info(
        f"Waiting for CSV {csv_name_post_upgrade} to be in succeeded state"
    )
    csv.wait_for_phase("Succeeded", timeout=400)
    ocs_install_verification(timeout=600)
