import logging

from pkg_resources import parse_version

from ocs_ci.framework.testlib import ocs_upgrade
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import CephCluster, CephHealthMonitor
from ocs_ci.ocs.ocs_upgrade import (
    run_ocs_upgrade, verify_image_versions,
)
from ocs_ci.ocs.resources.storage_cluster import ocs_install_verification

log = logging.getLogger(__name__)


@ocs_upgrade
def test_upgrade():
    # Get versions and images information
    ocs_registry_image = config.UPGRADE.get('upgrade_ocs_registry_image')
    version_before_upgrade = config.ENV_DATA.get("ocs_version")
    upgrade_version = ocs_upgrade.get_upgrade_version(
        ocs_registry_image, version_before_upgrade
    )
    parsed_version_before_upgrade = parse_version(version_before_upgrade)
    parsed_upgrade_version = parse_version(upgrade_version)

    # Run upgrade test under Ceph cluster health check
    ceph_cluster = CephCluster()
    with CephHealthMonitor(ceph_cluster):
        old_images = run_ocs_upgrade(ocs_registry_image,
                                     parsed_upgrade_version,
                                     parsed_version_before_upgrade,
                                     upgrade_version,
                                     version_before_upgrade)

        verify_image_versions(old_images, parsed_upgrade_version)
        ocs_install_verification(
            timeout=600, skip_osd_distribution_check=True,
            ocs_registry_image=ocs_registry_image,
            post_upgrade_verification=True,
        )
