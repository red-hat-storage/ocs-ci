import logging

from ocs_ci.framework.testlib import ocs_upgrade
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import CephCluster, CephHealthMonitor
from ocs_ci.ocs.resources.storage_cluster import (
    ocs_install_verification
)
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.ocs_upgrade import (
    verify_image_versions,
    OCSUpgrade,
)
from ocs_ci.ocs.exceptions import TimeoutException

log = logging.getLogger(__name__)


@ocs_upgrade
def test_upgrade():
    """
    Tests upgrade procedure of OCS cluster

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
        upgrade_ocs.update_subscription(channel)
        upgrade_ocs.set_upgrade_images()
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
