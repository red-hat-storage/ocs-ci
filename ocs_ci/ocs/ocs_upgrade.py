import os
import logging
from copy import deepcopy
from pkg_resources import parse_version
from tempfile import NamedTemporaryFile
import time

from ocs_ci.framework import config
from ocs_ci.deployment.deployment import create_catalog_source
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster, CephHealthMonitor
from ocs_ci.ocs.defaults import OCS_OPERATOR_NAME
from ocs_ci.ocs.ocp import get_images, OCP
from ocs_ci.ocs.node import get_typed_nodes
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.pod import verify_pods_upgraded
from ocs_ci.ocs.resources.packagemanifest import (
    get_selector_for_ocs_operator, PackageManifest,
)
from ocs_ci.ocs.resources.storage_cluster import (
    get_osd_count, ocs_install_verification,
)
from ocs_ci.ocs.utils import setup_ceph_toolbox
from ocs_ci.utility.utils import (
    get_latest_ds_olm_tag, get_next_version_available_for_upgrade,
    get_ocs_version_from_image, load_config_file, TimeoutSampler,
)
from ocs_ci.utility.templating import dump_data_to_temp_yaml
from ocs_ci.ocs.exceptions import TimeoutException


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
    number_of_worker_nodes = len(get_typed_nodes())
    osd_count = get_osd_count()
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
    verify_pods_upgraded(old_images, selector=constants.MON_APP_LABEL, count=3)
    verify_pods_upgraded(old_images, selector=constants.MGR_APP_LABEL)
    # OSD upgrade have timeout 10mins for new attempt if cluster is not health.
    # https://bugzilla.redhat.com/show_bug.cgi?id=1840729 setting timeout for
    # 12.5 minutes per OSD
    verify_pods_upgraded(
        old_images, selector=constants.OSD_APP_LABEL, count=osd_count,
        timeout=750 * osd_count,
    )
    verify_pods_upgraded(old_images, selector=constants.MDS_APP_LABEL, count=2)
    if config.ENV_DATA.get('platform') in constants.ON_PREM_PLATFORMS or (
        config.ENV_DATA.get('platform') == constants.AZURE_PLATFORM
    ):
        # Workaround for https://bugzilla.redhat.com/show_bug.cgi?id=1857802 - RGW count is 1
        # post upgrade to OCS 4.5. Tracked with
        # https://github.com/red-hat-storage/ocs-ci/issues/2532
        # TODO: uncomment the below 1 line:
        # rgw_count = 2 if float(config.ENV_DATA['ocs_version']) >= 4.5 else 1
        # TODO: Delete the below 1 line
        rgw_count = 1
        verify_pods_upgraded(old_images, selector=constants.RGW_APP_LABEL, count=rgw_count)

    # With 4.4 OCS cluster deployed over Azure, RGW is the default backingstore


class OCSUpgrade(object):
    """
    OCS Upgrade helper class

    """
    def __init__(self, namespace, version_before_upgrade, ocs_registry_image, upgrade_in_current_source):
        self.namespace = namespace
        self._version_before_upgrade = version_before_upgrade
        self._ocs_registry_image = ocs_registry_image
        self.upgrade_in_current_source = upgrade_in_current_source

    @property
    def version_before_upgrade(self):
        return self._version_before_upgrade

    @property
    def ocs_registry_image(self):
        return self._ocs_registry_image

    def get_upgrade_version(self):
        """
        Getting the required upgrade version

        Returns:
            str: version to be upgraded

        """
        upgrade_version = config.UPGRADE.get(
            "upgrade_ocs_version", self.version_before_upgrade
        )
        if self.ocs_registry_image:
            upgrade_version = get_ocs_version_from_image(
                self.ocs_registry_image
            )

        return upgrade_version

    def get_parsed_versions(self):
        """
        Getting parsed version names, current version, and upgrade version

        Returns:
            tuple: contains 2 strings with parsed names of current version
                and upgrade version

        """
        parsed_version_before_upgrade = parse_version(self.version_before_upgrade)
        parsed_upgrade_version = parse_version(self.get_upgrade_version())

        return parsed_version_before_upgrade, parsed_upgrade_version

    def load_version_config_file(self, upgrade_version):
        """
        Loads config file to the ocs-ci config with upgrade version

        Args:
            upgrade_version: (str): version to be upgraded

        """

        version_change = self.get_parsed_versions()[1] > self.get_parsed_versions()[0]
        if version_change:
            version_config_file = os.path.join(
                constants.CONF_DIR, 'ocs_version', f'ocs-{upgrade_version}.yaml'
            )
            load_config_file(version_config_file)
        else:
            log.info(f"Upgrade version {upgrade_version} is not higher than old version:"
                     f" {self.version_before_upgrade}, config file will not be loaded")

    def get_csv_name_pre_upgrade(self):
        """
        Getting OCS operator name as displayed in CSV

        Returns:
            str: OCS operator name, as displayed in CSV

        """
        operator_selector = get_selector_for_ocs_operator()
        package_manifest = PackageManifest(
            resource_name=OCS_OPERATOR_NAME, selector=operator_selector,
        )
        channel = config.DEPLOYMENT.get('ocs_csv_channel')

        return package_manifest.get_current_csv(channel)

    def get_pre_upgrade_image(self, csv_name_pre_upgrade):
        """
        Getting all OCS cluster images, before upgrade

        Args:
            csv_name_pre_upgrade: (str): OCS operator name

        Returns:
            dict: Contains all OCS cluster images
                dict keys: Image name
                dict values: Image full path

        """
        csv_name_pre_upgrade = csv_name_pre_upgrade
        log.info(f"CSV name before upgrade is: {csv_name_pre_upgrade}")
        csv_pre_upgrade = CSV(
            resource_name=csv_name_pre_upgrade,
            namespace=self.namespace
        )
        return get_images(csv_pre_upgrade.get())

    def set_upgrade_channel(self):
        """
        Wait for the new package manifest for upgrade.

        Returns:
            str: OCS subscription channel

        """
        operator_selector = get_selector_for_ocs_operator()
        package_manifest = PackageManifest(
            resource_name=OCS_OPERATOR_NAME, selector=operator_selector,
        )
        package_manifest.wait_for_resource()
        channel = config.DEPLOYMENT.get('ocs_csv_channel')
        if not channel:
            channel = package_manifest.get_default_channel()

        return channel

    def update_subscription(self, channel):
        """
        Updating OCS operator subscription

        Args:
            channel: (str): OCS subscription channel

        """
        subscription = OCP(
            resource_name=constants.OCS_SUBSCRIPTION,
            kind='subscription',
            namespace=config.ENV_DATA['cluster_namespace'],
        )
        current_ocs_source = subscription.data['spec']['source']
        log.info(
            f"Current OCS subscription source: {current_ocs_source}"
        )
        ocs_source = current_ocs_source if self.upgrade_in_current_source else (
            constants.OPERATOR_CATALOG_SOURCE_NAME
        )
        patch_subscription_cmd = (
            f'patch subscription {constants.OCS_SUBSCRIPTION} '
            f'-n {self.namespace} --type merge -p \'{{"spec":{{"channel": '
            f'"{channel}", "source": "{ocs_source}"}}}}\''
        )
        subscription.exec_oc_cmd(patch_subscription_cmd, out_yaml_format=False)

        subscription_plan_approval = config.DEPLOYMENT.get(
            'subscription_plan_approval'
        )
        if subscription_plan_approval == 'Manual':
            wait_for_install_plan_and_approve(self.namespace)

    def check_if_upgrade_completed(self, channel, csv_name_pre_upgrade):
        """
        Checks if OCS operator finishes it's upgrade

        Args:
            channel: (str): OCS subscription channel
            csv_name_pre_upgrade: (str): OCS operator name

        Returns:
            bool: True if upgrade completed, False otherwise

        """
        operator_selector = get_selector_for_ocs_operator()
        package_manifest = PackageManifest(
            resource_name=OCS_OPERATOR_NAME, selector=operator_selector,
        )
        csv_name_post_upgrade = package_manifest.get_current_csv(channel)
        if csv_name_post_upgrade == csv_name_pre_upgrade:
            log.info(f"CSV is still: {csv_name_post_upgrade}")
            return False
        else:
            log.info(f"CSV now upgraded to: {csv_name_post_upgrade}")
            return True

    def get_images_post_upgrade(self, channel, pre_upgrade_images, upgrade_version):
        """
        Checks if all images of OCS cluster upgraded,
            and return list of all images if upgrade success

        Args:
            channel: (str): OCS subscription channel
            pre_upgrade_images: (dict): Contains all OCS cluster images
            upgrade_version: (str): version to be upgraded

        Returns:
            set: Contains full path of OCS cluster old images

        """
        operator_selector = get_selector_for_ocs_operator()
        package_manifest = PackageManifest(
            resource_name=OCS_OPERATOR_NAME, selector=operator_selector,
        )
        csv_name_post_upgrade = package_manifest.get_current_csv(channel)
        csv_post_upgrade = CSV(
            resource_name=csv_name_post_upgrade,
            namespace=self.namespace
        )
        log.info(
            f"Waiting for CSV {csv_name_post_upgrade} to be in succeeded state"
        )

        # Workaround for patching missing ceph-rook-tools pod after upgrade
        if self.version_before_upgrade == '4.2' and upgrade_version == '4.3':
            log.info("Force creating Ceph toolbox after upgrade 4.2 -> 4.3")
            setup_ceph_toolbox(force_setup=True)
        # End of workaround

        osd_count = get_osd_count()
        csv_post_upgrade.wait_for_phase("Succeeded", timeout=200 * osd_count)
        post_upgrade_images = get_images(csv_post_upgrade.get())
        old_images, _, _ = get_upgrade_image_info(
            pre_upgrade_images, post_upgrade_images
        )

        return old_images

    def set_upgrade_images(self):
        """
        Set images for upgrade

        """
        ocs_catalog = CatalogSource(
            resource_name=constants.OPERATOR_CATALOG_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )

        if not self.upgrade_in_current_source:
            if not ocs_catalog.is_exist() and not self.upgrade_in_current_source:
                log.info("OCS catalog source doesn't exist. Creating new one.")
                create_catalog_source(self.ocs_registry_image, ignore_upgrade=True)
            image_url = ocs_catalog.get_image_url()
            image_tag = ocs_catalog.get_image_name()
            log.info(f"Current image is: {image_url}, tag: {image_tag}")
            version_change = self.get_parsed_versions()[1] > self.get_parsed_versions()[0]
            if self.ocs_registry_image:
                image_url, new_image_tag = self.ocs_registry_image.split(':')
            elif (
                config.UPGRADE.get('upgrade_to_latest', True) or version_change
            ):
                new_image_tag = get_latest_ds_olm_tag()
            else:
                new_image_tag = get_next_version_available_for_upgrade(
                    image_tag
                )
            cs_data = deepcopy(ocs_catalog.data)
            image_for_upgrade = ':'.join([image_url, new_image_tag])
            log.info(f"Image: {image_for_upgrade} will be used for upgrade.")
            cs_data['spec']['image'] = image_for_upgrade

            with NamedTemporaryFile() as cs_yaml:
                dump_data_to_temp_yaml(cs_data, cs_yaml.name)
                ocs_catalog.apply(cs_yaml.name)


def run_ocs_upgrade(operation=None, *operation_args, **operation_kwargs):
    """
    Run upgrade procedure of OCS cluster

    Args:
        operation: (function): Function to run
        operation_args: (iterable): Function's arguments
        operation_kwargs: (map): Function's keyword arguments

    """

    ceph_cluster = CephCluster()
    upgrade_ocs = OCSUpgrade(
        namespace=config.ENV_DATA['cluster_namespace'],
        version_before_upgrade=config.ENV_DATA.get("ocs_version"),
        ocs_registry_image=config.UPGRADE.get('upgrade_ocs_registry_image'),
        upgrade_in_current_source=config.UPGRADE.get('upgrade_in_current_source', False)
    )
    upgrade_version = upgrade_ocs.get_upgrade_version()
    assert upgrade_ocs.get_parsed_versions()[1] >= upgrade_ocs.get_parsed_versions()[0], (
        f"Version you would like to upgrade to: {upgrade_version} "
        f"is not higher or equal to the version you currently running: "
        f"{upgrade_ocs.version_before_upgrade}"
    )
    csv_name_pre_upgrade = upgrade_ocs.get_csv_name_pre_upgrade()
    pre_upgrade_images = upgrade_ocs.get_pre_upgrade_image(csv_name_pre_upgrade)
    upgrade_ocs.load_version_config_file(upgrade_version)
    with CephHealthMonitor(ceph_cluster):
        channel = upgrade_ocs.set_upgrade_channel()
        upgrade_ocs.set_upgrade_images()
        upgrade_ocs.update_subscription(channel)
        if operation:
            log.info(f"Calling test function: {operation}")
            _ = operation(*operation_args, **operation_kwargs)
            # Workaround for issue #2531
            time.sleep(30)
            # End of workaround

        for sample in TimeoutSampler(
            timeout=725,
            sleep=5,
            func=upgrade_ocs.check_if_upgrade_completed,
            channel=channel,
            csv_name_pre_upgrade=csv_name_pre_upgrade,
        ):
            try:
                if sample:
                    log.info("Upgrade success!")
                    break
            except TimeoutException:
                raise TimeoutException("No new CSV found after upgrade!")
        old_image = upgrade_ocs.get_images_post_upgrade(
            channel, pre_upgrade_images, upgrade_version
        )
    verify_image_versions(old_image, upgrade_ocs.get_parsed_versions()[1])
    ocs_install_verification(
        timeout=600, skip_osd_distribution_check=True,
        ocs_registry_image=upgrade_ocs.ocs_registry_image,
        post_upgrade_verification=True,
    )
