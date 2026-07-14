import logging

from ocs_ci.framework import config
from ocs_ci.utility import version

log = logging.getLogger(__name__)


def get_rgw_count(ocs_version, is_upgrade, version_before_upgrade):
    """
    Get RGW Count

    RGW Count is 2 if:
       OCS 4.5 unless upgraded from a prior version
       OCS 4.6
       arbiter deployment

    Otherwise, RGW Count is 1

    Args:
        ocs_version (str, float): OCS Version
        is_upgrade (bool): If cluster was upgraded to current version
        version_before_upgrade (str, float): OCS Version prior to upgrade

    Returns:
        int: RGW Count

    """
    if config.DEPLOYMENT.get("arbiter_deployment"):
        log.debug("RGW Count: 2")
        return 2

    semantic_ocs_version = version.get_semantic_version(
        ocs_version, only_major_minor=True
    )
    # Assume upgrade from prior version if one is not provided
    if is_upgrade:
        version_before_upgrade = version.get_semantic_version(
            f"{semantic_ocs_version.major}.{semantic_ocs_version.minor - 1}",
            only_major_minor=True,
        )
        log.info(
            "version_before_upgrade not provided, assuming prior release is "
            f"{version_before_upgrade}",
        )

    if (
        semantic_ocs_version == version.VERSION_4_5
        and not (is_upgrade and version_before_upgrade < version.VERSION_4_5)
        or semantic_ocs_version == version.VERSION_4_6
    ):
        log.debug("RGW Count: 2")
        return 2
    else:
        log.debug("RGW Count: 1")
        return 1
