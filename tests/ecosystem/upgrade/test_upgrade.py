import os
import logging
from copy import deepcopy
from pkg_resources import parse_version
from tempfile import NamedTemporaryFile
from time import sleep

from ocs_ci.deployment.deployment import create_catalog_source
from ocs_ci.framework import config
from ocs_ci.framework.testlib import ocs_upgrade
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster, CephHealthMonitor
from ocs_ci.ocs.defaults import OCS_OPERATOR_NAME
from ocs_ci.ocs.exceptions import TimeoutException
from ocs_ci.ocs.node import get_typed_nodes
from ocs_ci.ocs.ocp import get_images, OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.storage_cluster import ocs_install_verification
from ocs_ci.ocs.resources.pod import verify_pods_upgraded
from ocs_ci.ocs.resources.packagemanifest import (
    get_selector_for_ocs_operator,
    PackageManifest,
)
from ocs_ci.ocs.resources.storage_cluster import StorageCluster
from ocs_ci.ocs.utils import setup_ceph_toolbox
from ocs_ci.utility.utils import (
    get_latest_ds_olm_tag,
    get_next_version_available_for_upgrade,
    get_ocs_version_from_image,
    load_config_file,
    run_cmd,
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


def verify_image_versions(old_images, upgrade_version):
    """
    Verify if all the images of OCS objects got upgraded

    Args:
        old_images (set): set with old images
        upgrade_version (packaging.version.Version): version of OCS

    """
    namespace = config.ENV_DATA['cluster_namespace']
    number_of_worker_nodes = len(get_typed_nodes())
    storage_cluster = StorageCluster(
        resource_name=config.ENV_DATA['storage_cluster_name'],
        namespace=namespace
    )
    osd_count = (
        int(storage_cluster.data['spec']['storageDeviceSets'][0]['count'])
        * int(storage_cluster.data['spec']['storageDeviceSets'][0]['replica'])
    )
    verify_pods_upgraded(old_images, selector=constants.OCS_OPERATOR_LABEL)
    verify_pods_upgraded(old_images, selector=constants.OPERATOR_LABEL)
    # in 4.3 app selector nooba have those pods: noobaa-core-ID, noobaa-db-ID,
    # noobaa-operator-ID but in 4.2 only 2: noobaa-core-ID, noobaa-operator-ID
    nooba_pods = 2 if upgrade_version < parse_version('4.3') else 3
    verify_pods_upgraded(
        old_images, selector=constants.NOOBAA_APP_LABEL, count=nooba_pods
    )
    verify_pods_upgraded(
        old_images, selector=constants.CSI_CEPHFSPLUGIN_LABEL,
        count=number_of_worker_nodes,
    )
    verify_pods_upgraded(
        old_images, selector=constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
        count=2
    )
    verify_pods_upgraded(
        old_images, selector=constants.CSI_RBDPLUGIN_LABEL,
        count=number_of_worker_nodes,
    )
    verify_pods_upgraded(
        old_images, selector=constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
        count=2
    )
    verify_pods_upgraded(old_images, selector=constants.MGR_APP_LABEL)
    verify_pods_upgraded(old_images, selector=constants.MON_APP_LABEL, count=3)
    verify_pods_upgraded(
        old_images, selector=constants.OSD_APP_LABEL, count=osd_count
    )
    verify_pods_upgraded(old_images, selector=constants.MDS_APP_LABEL, count=2)
    if config.ENV_DATA.get('platform') == constants.VSPHERE_PLATFORM:
        verify_pods_upgraded(
            old_images, selector=constants.RGW_APP_LABEL, count=1
        )


@ocs_upgrade
def test_upgrade():
    ceph_cluster = CephCluster()
    with CephHealthMonitor(ceph_cluster):
        namespace = config.ENV_DATA['cluster_namespace']
        version_before_upgrade = config.ENV_DATA.get("ocs_version")
        upgrade_version = config.UPGRADE.get(
            "upgrade_ocs_version", version_before_upgrade
        )
        ocs_registry_image = config.UPGRADE.get('upgrade_ocs_registry_image')
        if ocs_registry_image:
            upgrade_version = get_ocs_version_from_image(
                ocs_registry_image
            )
        parsed_version_before_upgrade = parse_version(version_before_upgrade)
        parsed_upgrade_version = parse_version(upgrade_version)
        assert parsed_upgrade_version >= parsed_version_before_upgrade, (
            f"Version you would like to upgrade to: {upgrade_version} "
            f"is not higher or equal to the version you currently running: "
            f"{version_before_upgrade}"
        )
        operator_selector = get_selector_for_ocs_operator()
        package_manifest = PackageManifest(
            resource_name=OCS_OPERATOR_NAME, selector=operator_selector,
        )
        channel = config.DEPLOYMENT.get('ocs_csv_channel')
        csv_name_pre_upgrade = package_manifest.get_current_csv(channel)
        log.info(f"CSV name before upgrade is: {csv_name_pre_upgrade}")
        csv_pre_upgrade = CSV(
            resource_name=csv_name_pre_upgrade,
            namespace=namespace
        )
        pre_upgrade_images = get_images(csv_pre_upgrade.get())
        version_change = parsed_upgrade_version > parsed_version_before_upgrade
        if version_change:
            version_config_file = os.path.join(
                constants.CONF_DIR, 'ocs_version', f'ocs-{upgrade_version}.yaml'
            )
            load_config_file(version_config_file)
        ocs_catalog = CatalogSource(
            resource_name=constants.OPERATOR_CATALOG_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        upgrade_in_current_source = config.UPGRADE.get(
            'upgrade_in_current_source', False
        )
        if not ocs_catalog.is_exist() and not upgrade_in_current_source:
            log.info("OCS catalog source doesn't exist. Creating new one.")
            create_catalog_source(ocs_registry_image, ignore_upgrade=True)
        image_url = ocs_catalog.get_image_url()
        image_tag = ocs_catalog.get_image_name()
        log.info(f"Current image is: {image_url}, tag: {image_tag}")
        if ocs_registry_image:
            image_url, new_image_tag = ocs_registry_image.split(':')
        elif config.UPGRADE.get('upgrade_to_latest', True) or version_change:
            new_image_tag = get_latest_ds_olm_tag()
        else:
            new_image_tag = get_next_version_available_for_upgrade(image_tag)
        cs_data = deepcopy(ocs_catalog.data)
        image_for_upgrade = ':'.join([image_url, new_image_tag])
        log.info(f"Image: {image_for_upgrade} will be used for upgrade.")
        cs_data['spec']['image'] = image_for_upgrade

        with NamedTemporaryFile() as cs_yaml:
            dump_data_to_temp_yaml(cs_data, cs_yaml.name)
            ocs_catalog.apply(cs_yaml.name)
        # Wait for the new package manifest for upgrade.
        operator_selector = get_selector_for_ocs_operator()
        package_manifest = PackageManifest(
            resource_name=OCS_OPERATOR_NAME, selector=operator_selector,
        )
        package_manifest.wait_for_resource()
        channel = config.DEPLOYMENT.get('ocs_csv_channel')
        if not channel:
            channel = package_manifest.get_default_channel()

        # update subscription
        subscription = OCP(
            resource_name=constants.OCS_SUBSCRIPTION,
            kind='subscription',
            namespace=config.ENV_DATA['cluster_namespace'],
        )
        current_ocs_source = subscription.data['spec']['source']
        log.info(
            f"Current OCS subscription source: {current_ocs_source}"
        )
        ocs_source = current_ocs_source if upgrade_in_current_source else (
            constants.OPERATOR_CATALOG_SOURCE_NAME
        )
        patch_subscription_cmd = (
            f'oc patch subscription {constants.OCS_SUBSCRIPTION} '
            f'-n {namespace} --type merge -p \'{{"spec":{{"channel": '
            f'"{channel}", "source": "{ocs_source}"}}}}\''
        )
        run_cmd(patch_subscription_cmd)

        subscription_plan_approval = config.DEPLOYMENT.get(
            'subscription_plan_approval'
        )
        if subscription_plan_approval == 'Manual':
            wait_for_install_plan_and_approve(namespace)
        attempts = 10
        for attempt in range(1, attempts + 1):
            log.info(f"Attempt {attempt}/{attempts} to check CSV upgraded.")
            csv_name_post_upgrade = package_manifest.get_current_csv(channel)
            if csv_name_post_upgrade == csv_name_pre_upgrade:
                log.info(f"CSV is still: {csv_name_post_upgrade}")
                sleep(5)
            else:
                log.info(f"CSV now upgraded to: {csv_name_post_upgrade}")
                break
            if attempts == attempt:
                raise TimeoutException("No new CSV found after upgrade!")
        csv_post_upgrade = CSV(
            resource_name=csv_name_post_upgrade,
            namespace=namespace
        )
        log.info(
            f"Waiting for CSV {csv_name_post_upgrade} to be in succeeded state"
        )
        if version_before_upgrade == '4.2' and upgrade_version == '4.3':
            log.info("Force creating Ceph toolbox after upgrade 4.2 -> 4.3")
            setup_ceph_toolbox(force_setup=True)
        csv_post_upgrade.wait_for_phase("Succeeded", timeout=600)
        post_upgrade_images = get_images(csv_post_upgrade.get())
        old_images, _, _ = get_upgrade_image_info(
            pre_upgrade_images, post_upgrade_images
        )
        verify_image_versions(old_images, parsed_upgrade_version)
        ocs_install_verification(
            timeout=600, skip_osd_distribution_check=True,
            ocs_registry_image=ocs_registry_image,
            post_upgrade_verification=True,
        )
