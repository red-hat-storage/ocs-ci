import logging
from copy import deepcopy
from tempfile import NamedTemporaryFile
from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.testlib import upgrade
from ocs_ci.ocs import constants
from ocs_ci.ocs.defaults import OCS_OPERATOR_NAME
from ocs_ci.ocs.exceptions import TimeoutException
from ocs_ci.ocs.ocp import get_images
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.ocs import ocs_install_verification
from ocs_ci.ocs.resources.pod import verify_pods_upgraded
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.utility.utils import (
    get_latest_ds_olm_tag,
    get_next_version_available_for_upgrade,
)
from ocs_ci.utility.templating import dump_data_to_temp_yaml

log = logging.getLogger(__name__)


def get_upgrade_image_info(old_csv_images, new_csv_images):
    """
    Log the info about the images which are going to be upgraded.

    Args:
        old_csv_images (dict): dictionary with the old images from pre upgrade CSV
        new_csv_images (dict): dictionary with the post upgrade images

    Returns:
        tuple: with three sets which contain those types of images
            old_images_for_upgrade - images which should be upgraded
            new_images_to_upgrade - new images to which upgrade
            unchanged_images - unchanged images without upgrade

    """
    old_csv_images = set(old_csv_images.values())
    new_csv_images = set(new_csv_images.values())
    old_images_for_upgrade = old_csv_images - new_csv_images
    log.info(
        f"Old images which are going to be upgraded: "
        f"{sorted(old_images_for_upgrade)}"
    )
    new_images_to_upgrade = new_csv_images - old_csv_images
    log.info(
        f"New images for upgrade: "
        f"{sorted(new_images_to_upgrade)}"
    )
    unchanged_images = old_csv_images.intersection(new_csv_images)
    log.info(
        f"Unchanged images after upgrade: "
        f"{sorted(unchanged_images)}"
    )
    return (
        old_images_for_upgrade,
        new_images_to_upgrade,
        unchanged_images,
    )


def verify_image_versions(old_images):
    """
    Verify if all the images of OCS objects got upgraded

    Args:
        old_images (set): set with old images

    """
    verify_pods_upgraded(old_images, selector=constants.OCS_OPERATOR_LABEL)
    verify_pods_upgraded(old_images, selector=constants.OPERATOR_LABEL)
    verify_pods_upgraded(
        old_images, selector=constants.NOOBAA_APP_LABEL, count=2
    )
    verify_pods_upgraded(
        old_images, selector=constants.CSI_CEPHFSPLUGIN_LABEL, count=3
    )
    verify_pods_upgraded(
        old_images, selector=constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
        count=2
    )
    verify_pods_upgraded(
        old_images, selector=constants.CSI_RBDPLUGIN_LABEL, count=3
    )
    verify_pods_upgraded(
        old_images, selector=constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
        count=2
    )
    verify_pods_upgraded(old_images, selector=constants.MGR_APP_LABEL)
    verify_pods_upgraded(old_images, selector=constants.MON_APP_LABEL, count=3)
    verify_pods_upgraded(old_images, selector=constants.OSD_APP_LABEL, count=3)
    verify_pods_upgraded(old_images, selector=constants.MDS_APP_LABEL, count=2)


@upgrade
def test_upgrade():
    namespace = config.ENV_DATA['cluster_namespace']
    ocs_catalog = CatalogSource(
        resource_name=constants.OPERATOR_CATALOG_SOURCE_NAME,
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
    csv_pre_upgrade = CSV(
        resource_name=csv_name_pre_upgrade,
        namespace=namespace
    )
    pre_upgrade_images = get_images(csv_pre_upgrade.get())

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
    csv_post_upgrade = CSV(
        resource_name=csv_name_post_upgrade,
        namespace=namespace
    )
    log.info(
        f"Waiting for CSV {csv_name_post_upgrade} to be in succeeded state"
    )
    csv_post_upgrade.wait_for_phase("Succeeded", timeout=600)
    post_upgrade_images = get_images(csv_post_upgrade.get())
    old_images, _, _ = get_upgrade_image_info(
        pre_upgrade_images, post_upgrade_images
    )
    verify_image_versions(old_images)
    ocs_install_verification(timeout=600)
